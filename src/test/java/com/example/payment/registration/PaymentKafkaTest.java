package com.example.payment.registration;

import com.example.payment.domain.Payment;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.database.DatabaseFailureClassifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;
import com.example.payment.registration.kafka.KafkaConfig;
import com.example.payment.registration.kafka.KafkaListenerHealthCheck;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DataJpaTest(properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "kafka.listener.database-retry-delay-ms=3000"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({KafkaConfig.class, DatabaseFailureClassifier.class, PaymentRegistrationService.class, PaymentKafkaTest.Config.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentKafkaTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.16")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentRegistrationService paymentRegistrationService;

    @Autowired
    private Validator validator;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, PaymentCommand> kafkaListenerContainerFactory;

    @Autowired
    private DefaultAfterRollbackProcessor<String, PaymentCommand> kafkaAfterRollbackProcessor;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private AdminClient admin;
    private KafkaProducer<String, byte[]> producer;
    private ConcurrentMessageListenerContainer<String, PaymentCommand> container;
    private PaymentListener paymentListener;
    private String topic;
    private String group;

    @BeforeEach
    void setUp() throws Exception {
        paymentRepository.deleteAllInBatch();
        topic = "payments-test-" + UUID.randomUUID();
        group = "payment-group-test-" + UUID.randomUUID();
        kafkaAfterRollbackProcessor.setRetryListeners();
        paymentListener = new PaymentListener(validator, paymentRegistrationService);
        admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000
        ));
        producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000
        ), new StringSerializer(), new ByteArraySerializer());
        createTopic(topic);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        try {
            if (container != null) {
                CountDownLatch stopped = new CountDownLatch(1);
                container.stop(stopped::countDown);
                assertThat(stopped.await(20, TimeUnit.SECONDS)).isTrue();
            }
        } finally {
            kafkaAfterRollbackProcessor.setRetryListeners();
            if (producer != null) {
                producer.close(Duration.ofSeconds(5));
            }
            if (admin != null) {
                admin.close(Duration.ofSeconds(5));
            }
        }
    }

    @Test
    @DisplayName("Identical messages register a payment only once")
    void registersOnce() throws Exception {
        startListener();
        PaymentCommand dto = request();

        RecordMetadata first = send(dto);
        awaitOffset(first);
        Payment before = findPayment(dto.eventId());
        RecordMetadata second = send(dto);
        awaitOffset(second);

        assertThat(second.offset()).isGreaterThan(first.offset());
        assertThat(paymentRepository.count()).isEqualTo(1);
        Payment saved = findPayment(dto.eventId());
        assertThat(saved).usingRecursiveComparison().isEqualTo(before);
        assertThat(saved.getPaymentMethodId()).isEqualTo("pm_card_visa");
        assertThat(saved.getOwnerUsername()).isEqualTo(dto.ownerUsername());
        assertThat(saved.getAmount()).isEqualTo(dto.amount());
        assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(saved.getAttempts()).isZero();
        assertThat(saved.getMaxAttempts()).isEqualTo(3);
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(readDlt()).isEmpty();
    }

    @Test
    @DisplayName("The same record is redelivered after the database commit without a Kafka commit")
    void replaysAfterCommit() throws Exception {
        createTopic(topic + ".DLT");
        CountDownLatch committed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean failOnce = new AtomicBoolean(true);
        List<ConsumerRecord<String, PaymentCommand>> deliveries = new CopyOnWriteArrayList<>();
        listen(record -> {
            paymentListener.receive(record);
            deliveries.add(record);
            if (failOnce.getAndSet(false)) {

                committed.countDown();
                awaitLatch(release);
                throw new IllegalStateException("Test failure after the database commit");
            }
        });
        PaymentCommand dto = request();
        RecordMetadata sent;
        Long id;

        try {
            sent = send(dto);
            awaitLatch(committed);
            id = findPayment(dto.eventId()).getId();
            assertThat(paymentRepository.count()).isEqualTo(1);
            assertNotCommitted(sent);
        } finally {
            release.countDown();
        }

        awaitOffset(sent);
        assertThat(deliveries).hasSize(2);
        for (ConsumerRecord<String, PaymentCommand> record : deliveries) {
            assertThat(record.topic()).isEqualTo(sent.topic());
            assertThat(record.partition()).isEqualTo(sent.partition());
            assertThat(record.offset()).isEqualTo(sent.offset());
        }
        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(findPayment(dto.eventId()).getId()).isEqualTo(id);
        assertThat(readDlt()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"paymentMethodId", "amount", "ownerUsername"})
    @DisplayName("A duplicate with different parameters is rejected and sent to DLT")
    void rejectsChangedPayment(String field) throws Exception {
        startListener();
        PaymentCommand dto = request();
        awaitOffset(send(dto));
        Payment before = findPayment(dto.eventId());
        PaymentCommand changed = switch (field) {
            case "paymentMethodId" -> new PaymentCommand(
                dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
                dto.amount(), "usd", "pm_card_mastercard"
            );
            case "ownerUsername" -> new PaymentCommand(
                dto.eventId(), "bob", dto.paymentOperationId(), dto.orderId(),
                dto.amount(), "usd", dto.paymentMethodId()
            );
            default -> new PaymentCommand(
                dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
                2000L, "usd", dto.paymentMethodId()
            );
        };

        RecordMetadata sent = send(changed);
        awaitOffset(sent);

        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(findPayment(dto.eventId())).usingRecursiveComparison().isEqualTo(before);
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(changed)));
        assertOrigin(dlt, sent, dto.eventId().toString());
        assertThat(new String(dlt.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value(),
            StandardCharsets.UTF_8)).contains("eventId=" + dto.eventId(), "already registered with different values");
    }

    @Test
    @DisplayName("A DTO with an invalid amount is sent to DLT as JSON")
    void sendsInvalidPayment() throws Exception {
        startListener();
        PaymentCommand dto = request("alice", "pm_card_visa", 0L);

        RecordMetadata sent = send(dto);
        awaitOffset(sent);

        assertThat(paymentRepository.count()).isZero();
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(dto)));
        assertOrigin(dlt, sent, dto.eventId().toString());
    }

    @Test
    @DisplayName("A message without an owner is sent to DLT without registering an unowned payment")
    void rejectsMessageWithoutOwner() throws Exception {
        startListener();
        PaymentCommand dto = request(null, "pm_card_visa", 1000L);

        RecordMetadata sent = send(dto);
        awaitOffset(sent);

        assertThat(paymentRepository.count()).isZero();
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(dto)));
        assertOrigin(dlt, sent, dto.eventId().toString());
    }

    @Test
    @ExtendWith(OutputCaptureExtension.class)
    @DisplayName("A payment method ID with NUL is logged and sent to DLT and the next payment is processed")
    void rejectsNulWithoutStoppingListener(CapturedOutput output) throws Exception {
        startListener();
        PaymentCommand invalid = request("alice", "Payment\u0000name", 1000L);

        RecordMetadata rejected = send(invalid);
        awaitOffset(rejected);
        PaymentCommand valid = request();
        awaitOffset(send(valid));

        assertThat(container.isRunning()).isTrue();
        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(findPayment(valid.eventId()).getPaymentMethodId()).isEqualTo("pm_card_visa");
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(invalid)));
        assertOrigin(dlt, rejected, invalid.eventId().toString());
        assertThat(new String(dlt.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value(),
            StandardCharsets.UTF_8)).contains("eventId=" + invalid.eventId(), "Expected a Stripe PaymentMethod ID");
        assertThat(output.getAll()).contains("Rejected payment message eventId=%s topic=%s partition=%d offset=%d"
            .formatted(invalid.eventId(), rejected.topic(), rejected.partition(), rejected.offset()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"paymentMethodId", "amount"})
    @DisplayName("PostgreSQL data and constraint errors go directly to DLT without stopping the listener")
    void recoversDatabaseDataErrors(String field) throws Exception {
        createTopic(topic + ".DLT");
        AtomicInteger deliveries = new AtomicInteger();
        listen(record -> {
            deliveries.incrementAndGet();
            paymentRegistrationService.register(record.value());
        });
        PaymentCommand invalid = field.equals("paymentMethodId")
            ? request("alice", "Payment\u0000name", 1000L)
            : request("alice", "pm_card_visa", 0L);

        RecordMetadata rejected = send(invalid);
        awaitOffset(rejected);
        PaymentCommand valid = request();
        awaitOffset(send(valid));

        assertThat(deliveries).hasValue(2);
        assertThat(container.isRunning()).isTrue();
        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(findPayment(valid.eventId()).getPaymentMethodId()).isEqualTo("pm_card_visa");
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(invalid)));
        assertOrigin(dlt, rejected, invalid.eventId().toString());
    }

    @Test
    @DisplayName("A three-partition source can publish failures to a one-partition DLT")
    void publishesToDltWithFewerPartitions() throws Exception {
        topic = "payments-partition-test-" + UUID.randomUUID();
        createTopic(topic, 3);
        createTopic(topic + ".DLT");
        listen(paymentListener::receive, 3);
        PaymentCommand invalid = request("alice", "pm_card_visa", 0L);
        String key = invalid.eventId().toString();

        RecordMetadata rejected = send(2, key, objectMapper.writeValueAsBytes(invalid));
        awaitOffset(rejected);

        assertThat(rejected.partition()).isEqualTo(2);
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertOrigin(dlt, rejected, key);
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(invalid)));
        assertThat(paymentRepository.count()).isZero();

        PaymentCommand valid = request();
        awaitOffset(send(2, valid.eventId().toString(), objectMapper.writeValueAsBytes(valid)));
        assertThat(findPayment(valid.eventId()).getPaymentMethodId()).isEqualTo("pm_card_visa");
        assertThat(container.isRunning()).isTrue();
        assertThat(readDlt()).hasSize(1);
    }

    @Test
    @DisplayName("Malformed JSON is sent to DLT with its original bytes")
    void sendsBrokenJson() throws Exception {
        createTopic(topic + ".DLT");
        AtomicInteger received = new AtomicInteger();
        listen(record -> {
            received.incrementAndGet();
            paymentListener.receive(record);
        });
        byte[] broken = "{\"eventId\":".getBytes(StandardCharsets.UTF_8);
        String key = UUID.randomUUID().toString();

        RecordMetadata sent = send(key, broken);
        awaitOffset(sent);

        assertThat(received).hasValue(0);
        assertThat(paymentRepository.count()).isZero();
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(dlt.value()).containsExactly(broken);
        assertOrigin(dlt, sent, key);
    }

    @Test
    @DisplayName("Offsets remain uncommitted while DLT is unavailable; processing resumes after it is created")
    void waitsForDlt() throws Exception {
        CompletableFuture<Exception> failed = new CompletableFuture<>();
        kafkaAfterRollbackProcessor.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int attempt) {
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
                failed.complete(failure);
            }
        });
        listen(paymentListener::receive);
        byte[] broken = "{\"eventId\":".getBytes(StandardCharsets.UTF_8);
        String key = UUID.randomUUID().toString();

        RecordMetadata sent = send(key, broken);

        assertThat(failed.get(45, TimeUnit.SECONDS)).isNotNull();
        assertNotCommitted(sent);
        assertThat(paymentRepository.count()).isZero();
        createTopic(topic + ".DLT");
        awaitOffset(sent);
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(dlt.value()).containsExactly(broken);
        assertOrigin(dlt, sent, key);
    }

    @Test
    @DisplayName("Read-only failover pauses without committing and automatically retries after recovery")
    void resumesAfterReadOnlyFailover() throws Exception {
        createTopic(topic + ".DLT");
        AtomicBoolean readOnly = new AtomicBoolean(true);
        AtomicReference<String> sqlState = new AtomicReference<>();
        List<ConsumerRecord<String, PaymentCommand>> deliveries = new CopyOnWriteArrayList<>();
        listen(record -> {
            deliveries.add(record);
            if (readOnly.get()) {
                try (Connection connection = dataSource.getConnection()) {
                    connection.setReadOnly(true);
                    connection.setAutoCommit(false);
                    try (var statement = connection.prepareStatement("""
                        INSERT INTO payments (event_id, payment_method_id, amount, owner_username, payment_operation_id, order_id, currency, idempotency_key)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """)) {
                        statement.setObject(1, record.value().eventId());
                        statement.setString(2, record.value().paymentMethodId());
                        statement.setLong(3, record.value().amount());
                        statement.setString(4, record.value().ownerUsername());
                        statement.setObject(5, record.value().paymentOperationId());
                        statement.setObject(6, record.value().orderId());
                        statement.setString(7, record.value().currency());
                        statement.setString(8, record.value().paymentOperationId().toString());
                        statement.executeUpdate();
                    } finally {
                        connection.rollback();
                    }
                } catch (SQLException failure) {
                    sqlState.set(failure.getSQLState());
                    throw new DataAccessResourceFailureException("Failover database is read-only", failure);
                }
            } else {
                paymentListener.receive(record);
            }
        });
        KafkaListenerEndpointRegistry registry = mock(KafkaListenerEndpointRegistry.class);
        when(registry.getListenerContainers()).thenReturn(List.of(container));

        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        try {
            var health = new KafkaListenerHealthCheck(registry, meters);
            PaymentCommand first = request();
            RecordMetadata sent = send(first);

            try {
                await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                    assertThat(sqlState.get()).isEqualTo("25006");
                    assertThat(container.isRunning()).isTrue();
                    assertThat(container.getContainers()).anySatisfy(child ->
                        assertThat(child.isPauseRequested()).isTrue());
                    assertThat(health.health().getStatus()).isEqualTo(Status.DOWN);
                    assertThat(meters.get("payment.kafka.listener.ready").gauge().value()).isZero();
                });
                assertNotCommitted(sent);
                assertThat(paymentRepository.count()).isZero();
            } finally {
                readOnly.set(false);
            }

            awaitOffset(sent);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(health.health().getStatus()).isEqualTo(Status.UP);
                assertThat(meters.get("payment.kafka.listener.ready").gauge().value()).isEqualTo(1);
            });
            PaymentCommand next = request();
            awaitOffset(send(next));

            assertThat(container.isRunning()).isTrue();
            assertThat(paymentRepository.count()).isEqualTo(2);
            assertThat(findPayment(first.eventId()).getPaymentMethodId()).isEqualTo("pm_card_visa");
            assertThat(findPayment(next.eventId()).getPaymentMethodId()).isEqualTo("pm_card_visa");
            List<ConsumerRecord<String, PaymentCommand>> redelivered = deliveries.stream()
                .filter(record -> first.eventId().equals(record.value().eventId()))
                .toList();
            assertThat(redelivered).hasSizeGreaterThanOrEqualTo(2);
            assertThat(redelivered).allSatisfy(record -> {
                assertThat(record.offset()).isEqualTo(sent.offset());
                assertThat(record.partition()).isEqualTo(sent.partition());
            });
            assertThat(readDlt()).isEmpty();
        } finally {
            meters.close();
        }
    }

    private PaymentCommand request() {
        return request("alice", "pm_card_visa", 1000L);
    }

    private PaymentCommand request(String ownerUsername, String paymentMethodId, Long amount) {
        return new PaymentCommand(
            UUID.randomUUID(), ownerUsername, UUID.randomUUID(), UUID.randomUUID(),
            amount, "usd", paymentMethodId
        );
    }

    private Payment findPayment(UUID eventId) {
        return paymentRepository.findByEventId(eventId).orElseThrow();
    }

    private void createTopic(String name) throws Exception {
        createTopic(name, 1);
    }

    private void createTopic(String name, int partitions) throws Exception {
        admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1)))
            .all().get(10, TimeUnit.SECONDS);
    }

    private void startListener() throws Exception {
        createTopic(topic + ".DLT");
        listen(paymentListener::receive);
    }

    private void listen(MessageListener<String, PaymentCommand> listener) {
        listen(listener, 1);
    }

    private void listen(MessageListener<String, PaymentCommand> listener, int partitions) {
        container = kafkaListenerContainerFactory.createContainer(topic);
        container.setBeanName(group);
        container.getContainerProperties().setGroupId(group);
        container.setupMessageListener(listener);
        container.start();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
            assertThat(container.getAssignedPartitions()).containsExactlyInAnyOrderElementsOf(
                java.util.stream.IntStream.range(0, partitions)
                    .mapToObj(partition -> new TopicPartition(topic, partition)).toList())
        );
    }

    private RecordMetadata send(PaymentCommand dto) throws Exception {
        return send(dto.eventId().toString(), objectMapper.writeValueAsBytes(dto));
    }

    private RecordMetadata send(String key, byte[] value) throws Exception {
        return send(0, key, value);
    }

    private RecordMetadata send(int partition, String key, byte[] value) throws Exception {
        return producer.send(new ProducerRecord<>(topic, partition, key, value))
            .get(15, TimeUnit.SECONDS);
    }

    private OffsetAndMetadata offset(RecordMetadata sent) throws Exception {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
            .get(10, TimeUnit.SECONDS)
            .get(new TopicPartition(sent.topic(), sent.partition()));
    }

    private void awaitOffset(RecordMetadata sent) {
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            OffsetAndMetadata committed = offset(sent);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(sent.offset() + 1);
        });
    }

    private void assertNotCommitted(RecordMetadata sent) throws Exception {
        OffsetAndMetadata committed = offset(sent);
        if (committed != null) {
            assertThat(committed.offset()).isLessThanOrEqualTo(sent.offset());
        }
    }

    private List<ConsumerRecord<String, byte[]>> readDlt() throws Exception {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
            ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
            ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000,
            ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000
        ), new StringDeserializer(), new ByteArrayDeserializer())) {
            TopicPartition partition = new TopicPartition(topic + ".DLT", 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = admin.listOffsets(Map.of(partition, OffsetSpec.latest()),
                    new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
                .all().get(10, TimeUnit.SECONDS).get(partition).offset();
            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            await().pollInSameThread().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
                return consumer.position(partition) >= end;
            });
            return records;
        }
    }

    private ConsumerRecord<String, byte[]> readOneDlt() throws Exception {
        List<ConsumerRecord<String, byte[]>> records = readDlt();
        assertThat(records).hasSize(1);
        return records.getFirst();
    }

    private void assertOrigin(ConsumerRecord<String, byte[]> dlt, RecordMetadata sent, String key) {
        assertThat(dlt.topic()).isEqualTo(topic + ".DLT");
        assertThat(dlt.partition()).isZero();
        assertThat(dlt.key()).isEqualTo(key);
        assertThat(new String(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_TOPIC).value(),
            StandardCharsets.UTF_8)).isEqualTo(sent.topic());
        assertThat(ByteBuffer.wrap(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_PARTITION).value())
            .getInt()).isEqualTo(sent.partition());
        assertThat(ByteBuffer.wrap(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value())
            .getLong()).isEqualTo(sent.offset());
        assertThat(new String(dlt.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP).value(),
            StandardCharsets.UTF_8)).isEqualTo(group);
        assertThat(dlt.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN)).isNotNull();
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Test wait was interrupted", e);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Config {

        @Bean
        LocalValidatorFactoryBean validator() {
            return new LocalValidatorFactoryBean();
        }

        @Bean
        ThreadPoolTaskScheduler paymentScheduler() {
            return new ThreadPoolTaskScheduler();
        }
    }
}
