package com.example.taskapplication.listener;

import com.example.taskapplication.config.KafkaConfig;
import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.service.database.DatabaseFailureClassifier;
import com.example.taskapplication.service.registration.TaskRegistrationService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({KafkaConfig.class, DatabaseFailureClassifier.class, TaskRegistrationService.class, TaskKafkaTest.Config.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskKafkaTest {

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
    private TaskRepository taskRepository;

    @Autowired
    private TaskRegistrationService taskRegistrationService;

    @Autowired
    private Validator validator;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, TaskRequestDTO> kafkaListenerContainerFactory;

    @Autowired
    private DefaultAfterRollbackProcessor<String, TaskRequestDTO> kafkaAfterRollbackProcessor;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private AdminClient admin;
    private KafkaProducer<String, byte[]> producer;
    private ConcurrentMessageListenerContainer<String, TaskRequestDTO> container;
    private TaskListener taskListener;
    private String topic;
    private String group;

    @BeforeEach
    void setUp() throws Exception {
        taskRepository.deleteAllInBatch();
        topic = "tasks-test-" + UUID.randomUUID();
        group = "task-group-test-" + UUID.randomUUID();
        kafkaAfterRollbackProcessor.setRetryListeners();
        taskListener = new TaskListener(validator, taskRegistrationService);
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
    @DisplayName("Identical messages register a task only once")
    void registersOnce() throws Exception {
        startListener();
        TaskRequestDTO dto = request();

        RecordMetadata first = send(dto);
        awaitOffset(first);
        Task before = findTask(dto.getEventId());
        RecordMetadata second = send(dto);
        awaitOffset(second);

        assertThat(second.offset()).isGreaterThan(first.offset());
        assertThat(taskRepository.count()).isEqualTo(1);
        Task saved = findTask(dto.getEventId());
        assertThat(saved).usingRecursiveComparison().isEqualTo(before);
        assertThat(saved.getName()).isEqualTo("Task");
        assertThat(saved.getDuration()).isEqualTo(dto.getDuration());
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(readDlt()).isEmpty();
    }

    @Test
    @DisplayName("The same record is redelivered after the database commit without a Kafka commit")
    void replaysAfterCommit() throws Exception {
        createTopic(topic + ".DLT");
        CountDownLatch committed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean failOnce = new AtomicBoolean(true);
        List<ConsumerRecord<String, TaskRequestDTO>> deliveries = new CopyOnWriteArrayList<>();
        listen(record -> {
            taskListener.receive(record);
            deliveries.add(record);
            if (failOnce.getAndSet(false)) {

                committed.countDown();
                awaitLatch(release);
                throw new IllegalStateException("Test failure after the database commit");
            }
        });
        TaskRequestDTO dto = request();
        RecordMetadata sent;
        Long id;

        try {
            sent = send(dto);
            awaitLatch(committed);
            id = findTask(dto.getEventId()).getId();
            assertThat(taskRepository.count()).isEqualTo(1);
            assertNotCommitted(sent);
        } finally {
            release.countDown();
        }

        awaitOffset(sent);
        assertThat(deliveries).hasSize(2);
        for (ConsumerRecord<String, TaskRequestDTO> record : deliveries) {
            assertThat(record.topic()).isEqualTo(sent.topic());
            assertThat(record.partition()).isEqualTo(sent.partition());
            assertThat(record.offset()).isEqualTo(sent.offset());
        }
        assertThat(taskRepository.count()).isEqualTo(1);
        assertThat(findTask(dto.getEventId()).getId()).isEqualTo(id);
        assertThat(readDlt()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"name", "duration"})
    @DisplayName("A duplicate with different parameters is rejected and sent to DLT")
    void rejectsChangedTask(String field) throws Exception {
        startListener();
        TaskRequestDTO dto = request();
        awaitOffset(send(dto));
        Task before = findTask(dto.getEventId());
        if (field.equals("name")) {
            dto.setName("Another task");
        } else {
            dto.setDuration(2000L);
        }

        RecordMetadata sent = send(dto);
        awaitOffset(sent);

        assertThat(taskRepository.count()).isEqualTo(1);
        assertThat(findTask(dto.getEventId())).usingRecursiveComparison().isEqualTo(before);
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(dto)));
        assertOrigin(dlt, sent, dto.getEventId().toString());
    }

    @Test
    @DisplayName("A DTO with an invalid duration is sent to DLT as JSON")
    void sendsInvalidTask() throws Exception {
        startListener();
        TaskRequestDTO dto = request();
        dto.setDuration(0L);

        RecordMetadata sent = send(dto);
        awaitOffset(sent);

        assertThat(taskRepository.count()).isZero();
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(objectMapper.readTree(dlt.value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(dto)));
        assertOrigin(dlt, sent, dto.getEventId().toString());
    }

    @Test
    @DisplayName("Malformed JSON is sent to DLT with its original bytes")
    void sendsBrokenJson() throws Exception {
        createTopic(topic + ".DLT");
        AtomicInteger received = new AtomicInteger();
        listen(record -> {
            received.incrementAndGet();
            taskListener.receive(record);
        });
        byte[] broken = "{\"eventId\":".getBytes(StandardCharsets.UTF_8);
        String key = UUID.randomUUID().toString();

        RecordMetadata sent = send(key, broken);
        awaitOffset(sent);

        assertThat(received).hasValue(0);
        assertThat(taskRepository.count()).isZero();
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
        listen(taskListener::receive);
        byte[] broken = "{\"eventId\":".getBytes(StandardCharsets.UTF_8);
        String key = UUID.randomUUID().toString();

        RecordMetadata sent = send(key, broken);

        assertThat(failed.get(45, TimeUnit.SECONDS)).isNotNull();
        assertNotCommitted(sent);
        assertThat(taskRepository.count()).isZero();
        createTopic(topic + ".DLT");
        awaitOffset(sent);
        ConsumerRecord<String, byte[]> dlt = readOneDlt();
        assertThat(dlt.value()).containsExactly(broken);
        assertOrigin(dlt, sent, key);
    }

    private TaskRequestDTO request() {
        TaskRequestDTO dto = new TaskRequestDTO();
        dto.setEventId(UUID.randomUUID());
        dto.setName("  Task  ");
        dto.setDuration(1000L);
        return dto;
    }

    private Task findTask(UUID eventId) {
        return taskRepository.findByEventId(eventId).orElseThrow();
    }

    private void createTopic(String name) throws Exception {
        admin.createTopics(List.of(new NewTopic(name, 1, (short) 1)))
            .all().get(10, TimeUnit.SECONDS);
    }

    private void startListener() throws Exception {
        createTopic(topic + ".DLT");
        listen(taskListener::receive);
    }

    private void listen(MessageListener<String, TaskRequestDTO> listener) {
        container = kafkaListenerContainerFactory.createContainer(topic);
        container.setBeanName(group);
        container.getContainerProperties().setGroupId(group);
        container.setupMessageListener(listener);
        container.start();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
            assertThat(container.getAssignedPartitions()).containsExactly(new TopicPartition(topic, 0))
        );
    }

    private RecordMetadata send(TaskRequestDTO dto) throws Exception {
        return send(dto.getEventId().toString(), objectMapper.writeValueAsBytes(dto));
    }

    private RecordMetadata send(String key, byte[] value) throws Exception {
        return producer.send(new ProducerRecord<>(topic, 0, key, value))
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
        assertThat(dlt.partition()).isEqualTo(sent.partition());
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
        ThreadPoolTaskScheduler taskScheduler() {
            return new ThreadPoolTaskScheduler();
        }
    }
}
