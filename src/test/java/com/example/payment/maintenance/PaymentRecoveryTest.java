package com.example.payment.maintenance;

import com.example.payment.worker.PaymentTestInstance;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import com.stripe.Stripe;
import com.stripe.net.Webhook;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DataJpaTest(properties = {"spring.jpa.hibernate.ddl-auto=validate"})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentRecoveryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.16")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @TempDir
    Path logs;

    private final List<Instance> instances = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS test_calls (payment_id BIGINT NOT NULL)");
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS test_effects (payment_id BIGINT PRIMARY KEY)");
        jdbcTemplate.update("DELETE FROM test_calls");
        jdbcTemplate.update("DELETE FROM test_effects");
        paymentRepository.deleteAllInBatch();
    }

    @AfterEach
    void tearDown() throws InterruptedException {

        for (Instance instance : instances) {
            instance.process().destroyForcibly();
        }
        for (Instance instance : instances) {
            assertThat(instance.process().waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest(name = "Webhook recovery after a crash {0}")
    @ValueSource(strings = {"after receipt", "before processing commit"})
    void recoversAcceptedWebhookAfterCrash(String crashPoint) throws Exception {
        Payment payment = createPayment(2500);
        String eventId = "evt_" + UUID.randomUUID();
        String intentId = "pi_" + UUID.randomUUID();
        jdbcTemplate.update("""
            UPDATE payments SET execution_status = 'COMPLETED', finished_at = CURRENT_TIMESTAMP,
                payment_status = 'AWAITING_WEBHOOK', provider_payment_id = ? WHERE id = ?
            """, intentId, payment.getId());
        String payload = """
            {"id":"%s","object":"event","type":"payment_intent.succeeded","api_version":"%s","livemode":false,
             "data":{"object":{"object":"payment_intent","id":"%s","status":"succeeded","livemode":false,
              "amount":%d,"currency":"%s","metadata":{"paymentOperationId":"%s","orderId":"%s"}}}}
            """.formatted(eventId, Stripe.API_VERSION, intentId, payment.getAmount(), payment.getCurrency(),
                payment.getPaymentOperationId(), payment.getOrderId());

        Instance receiver = start("webhookReceiver");
        int port = Integer.parseInt(awaitMessage(receiver, "WEBHOOK_PORT").split(":")[2]);
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/webhooks/stripe"))
            .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json")
            .header("Stripe-Signature", Webhook.Signature.generateSignatureHeader(payload, "whsec_recovery_fixture"))
            .POST(HttpRequest.BodyPublishers.ofString(payload)).build();
        try (var client = HttpClient.newHttpClient()) {
            assertThat(client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(200);
        }

        boolean processingStarted = crashPoint.equals("before processing commit");
        if (processingStarted) {
            proceed(receiver);
            assertThat(awaitMessage(receiver, "BEFORE_WEBHOOK_COMMIT"))
                .isEqualTo("TEST:BEFORE_WEBHOOK_COMMIT:" + eventId + ":END");
        }
        kill(receiver);

        var afterCrash = findPayment(payment.getId());
        assertThat(afterCrash.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
        assertThat(afterCrash.getVersion()).isEqualTo(payment.getVersion());
        assertThat(jdbcTemplate.queryForObject("""
            SELECT payload FROM stripe_webhook_events WHERE event_id = ? AND processed_at IS NULL
                AND review_reason IS NULL AND technical_failures = 0 AND (available_at IS NOT NULL) = ?
            """, String.class, eventId, processingStarted)).isEqualTo(payload);

        Instance recovery = start("webhookRecovery");
        assertThat(recovery.process().pid()).isNotEqualTo(receiver.process().pid());
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(jdbcTemplate.queryForObject("""
                SELECT processed_at IS NOT NULL AND payload IS NULL AND available_at IS NULL
                    AND review_reason IS NULL AND technical_failures = 0
                FROM stripe_webhook_events WHERE event_id = ?
                """, Boolean.class, eventId)).isTrue();
            var saved = findPayment(payment.getId());
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.getVersion()).isEqualTo(payment.getVersion() + 1);
        });
    }

    @Test
    @DisplayName("A crash before commit rolls back the claim and releases the payment")
    void rollsBackClaim() throws Exception {
        Payment payment = createPayment(2500);
        Instance first = start("beforeCommit");
        UUID oldExecutionId = awaitRun(first, "BEFORE_COMMIT", payment.getId());

        kill(first);

        Payment saved = findPayment(payment.getId());
        assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(saved.getAttempts()).isZero();
        assertThat(saved.getVersion()).isEqualTo(payment.getVersion());
        assertThat(saved.getExecutionId()).isNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(saved.getStartedAt()).isNull();

        Instance second = start("worker");
        assertThat(second.process().pid()).isNotEqualTo(first.process().pid());
        UUID executionId = awaitRun(second, "CLAIMED", payment.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, payment.getId(), executionId, 1);
    }

    @Test
    @DisplayName("Another instance recovers the payment after the claim commits")
    void recoversClaim() throws Exception {
        Payment payment = createPayment(2500);
        Instance first = start("afterCommit");
        UUID oldExecutionId = awaitRun(first, "AFTER_COMMIT", payment.getId());
        assertThat(findPayment(payment.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);

        Instance second = recover(first, payment.getId(), "worker");

        UUID executionId = awaitRun(second, "CLAIMED", payment.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, payment.getId(), executionId, 2);
    }

    @Test
    @DisplayName("Another instance recovers the payment after a crash during execution")
    void recoversWork() throws Exception {
        Payment payment = createPayment(2500);
        Instance first = start("duringWork");
        UUID oldExecutionId = awaitRun(first, "WORKING", payment.getId());
        Payment working = findPayment(payment.getId());
        assertThat(working.getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);

        Instance second = recover(first, payment.getId(), "worker");

        UUID executionId = awaitRun(second, "CLAIMED", payment.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, payment.getId(), executionId, 2);
    }

    @Test
    @DisplayName("A retry after an external effect uses the same operation key")
    void keepsEffect() throws Exception {
        Payment payment = createPayment(1000);
        Instance first = start("afterEffect");
        UUID oldExecutionId = awaitRun(first, "EFFECT_DONE", payment.getId());
        assertThat(findPayment(payment.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
        assertThat(findPayment(payment.getId()).getResult()).isNull();
        assertThat(countCalls(payment.getId())).isEqualTo(1);
        assertThat(countEffects(payment.getId())).isEqualTo(1);

        Instance second = recover(first, payment.getId(), "effectWorker");

        UUID executionId = awaitRun(second, "CLAIMED", payment.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, payment.getId(), executionId, 2);

        assertThat(countCalls(payment.getId())).isEqualTo(2);
        assertThat(countEffects(payment.getId())).isEqualTo(1);
    }

    @Test
    @DisplayName("The old instance does not overwrite the new attempt")
    void rejectsOldWorker() throws Exception {
        Payment payment = createPayment(2500);
        Instance first = start("oldWorker");
        UUID oldExecutionId = awaitRun(first, "PAUSED", payment.getId());
        expireLease(payment.getId());
        Instance second = start("hold");
        assertThat(second.process().pid()).isNotEqualTo(first.process().pid());
        waitForRetry(second, payment.getId(), 1);
        allowRetry(payment.getId());
        UUID executionId = awaitRun(second, "HOLD", payment.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        Payment before = findPayment(payment.getId());
        assertThat(before.getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
        assertThat(before.getExecutionId()).isEqualTo(executionId);
        assertThat(before.getAttempts()).isEqualTo(2);

        proceed(first);

        assertThat(awaitMessage(first, "OLD_RESULT"))
            .isEqualTo("TEST:OLD_RESULT:false:false:false:END");
        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(first.process().isAlive()).isTrue();
        proceed(second);
        assertCompleted(second, payment.getId(), executionId, 2);
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    @DisplayName("SIGTERM finishes a 60-second payment using the configured grace period and leaves queued work untouched")
    void shutsDown() throws Exception {
        createKafkaTopics();

        Instance instance = start("shutdown");
        Payment active = createPayment(60_000);
        UUID executionId = awaitRun(instance, "WORKING", active.getId());
        Payment queued = createPayment(500);
        assertThat(findPayment(active.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);

        long startedAt = System.nanoTime();
        assertThat(instance.process().toHandle().destroy()).isTrue();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(instance.process().isAlive()).as(readLog(instance)).isTrue();
            assertThat(readLog(instance)).contains("Waiting for workers and Kafka listeners to stop");
        });
        proceed(instance);

        long budget = TimeUnit.SECONDS.toNanos(120);
        long remaining = budget - (System.nanoTime() - startedAt);
        assertThat(remaining).isPositive();
        assertThat(instance.process().waitFor(remaining, TimeUnit.NANOSECONDS))
            .as(readLog(instance)).isTrue();
        assertThat(System.nanoTime() - startedAt).isLessThan(budget);
        assertThat(instance.process().exitValue()).isEqualTo(143);
        assertThat(message(instance, "STOPPED")).isEqualTo("TEST:STOPPED:true:true:END");
        assertThat(message(instance, "DONE"))
            .isEqualTo("TEST:DONE:" + active.getId() + ":" + executionId + ":END");

        Payment completed = findPayment(active.getId());
        assertThat(completed.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(completed.getResult()).isEqualTo("Payment execution completed");
        assertThat(completed.getExecutionId()).isEqualTo(executionId);
        assertThat(completed.getAttempts()).isEqualTo(1);
        assertThat(completed.getFinishedAt()).isNotNull();
        assertThat(completed.getLeaseUntil()).isNull();

        Payment waiting = findPayment(queued.getId());
        assertThat(waiting.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(waiting.getAttempts()).isZero();
        assertThat(waiting.getExecutionId()).isNull();
        assertThat(waiting.getStartedAt()).isNull();
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    @DisplayName("SIGTERM after a short grace period requeues an interrupted last attempt for another instance")
    void requeuesInterruptedPaymentOnShutdown() throws Exception {
        createKafkaTopics();
        Payment active = createPayment(60_000);
        jdbcTemplate.update("UPDATE payments SET attempts = max_attempts - 1 WHERE id = ?", active.getId());
        Instance first = start("shutdownInterrupted");
        UUID oldExecutionId = awaitRun(first, "WORKING", active.getId());
        assertThat(findPayment(active.getId()).getAttempts()).isEqualTo(3);

        assertThat(first.process().toHandle().destroy()).isTrue();
        assertThat(first.process().waitFor(30, TimeUnit.SECONDS)).as(readLog(first)).isTrue();
        assertThat(first.process().exitValue()).isEqualTo(143);
        assertThat(message(first, "STOPPED")).isEqualTo("TEST:STOPPED:true:true:END");

        Payment requeued = findPayment(active.getId());
        assertThat(requeued.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(requeued.getAttempts()).isEqualTo(2);
        assertThat(requeued.getResult()).isNull();
        assertThat(requeued.getExecutionId()).isNull();
        assertThat(requeued.getLeaseUntil()).isNull();
        assertThat(requeued.getStartedAt()).isNull();
        assertThat(requeued.getFinishedAt()).isNull();
        assertThat(requeued.getNextRetryAt()).isNull();

        Instance second = start("hold");
        UUID executionId = awaitRun(second, "HOLD", active.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertThat(findPayment(active.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
        assertThat(findPayment(active.getId()).getAttempts()).isEqualTo(3);
    }

    private void createKafkaTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000
        ))) {
            var existing = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            var missing = List.of("payments", "payments.DLT").stream()
                .filter(topic -> !existing.contains(topic))
                .map(topic -> new NewTopic(topic, 1, (short) 1))
                .toList();
            if (!missing.isEmpty()) {
                admin.createTopics(missing).all().get(15, TimeUnit.SECONDS);
            }
        }
    }

    private Payment createPayment(long amount) {
        Payment payment = payment(UUID.randomUUID(), "alice", "pm_test", amount);
        return paymentRepository.saveAndFlush(payment);
    }

    private Payment findPayment(Long id) {
        return paymentRepository.findById(id).orElseThrow();
    }

    private Instance start(String mode) throws IOException {
        Path log = logs.resolve(mode + "-" + instances.size() + ".log");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty(
            "surefire.test.class.path", System.getProperty("java.class.path")
        );
        ProcessBuilder builder = new ProcessBuilder(
            java, "-cp", classpath,
            PaymentTestInstance.class.getName(), mode
        );
        builder.environment().put("TEST_DB_URL", postgres.getJdbcUrl());
        builder.environment().put("TEST_DB_USERNAME", postgres.getUsername());
        builder.environment().put("TEST_DB_PASSWORD", postgres.getPassword());
        builder.environment().put("TEST_KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        builder.redirectErrorStream(true);
        builder.redirectOutput(log.toFile());
        Instance instance = new Instance(builder.start(), log);
        instances.add(instance);
        assertThat(awaitMessage(instance, "READY"))
            .isEqualTo("TEST:READY:" + instance.process().pid() + ":END");
        return instance;
    }

    private Instance recover(Instance first, Long id, String mode) throws Exception {
        int attempts = findPayment(id).getAttempts();
        kill(first);
        assertThat(findPayment(id).getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
        expireLease(id);
        Instance second = start(mode);
        assertThat(second.process().pid()).isNotEqualTo(first.process().pid());
        waitForRetry(second, id, attempts);
        allowRetry(id);
        return second;
    }

    private void waitForRetry(Instance instance, Long id, int attempts) {

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(instance.process().isAlive()).as(readLog(instance)).isTrue();
            assertThat(findPayment(id).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        });
        Payment saved = findPayment(id);
        assertThat(saved.getAttempts()).isEqualTo(attempts);
        assertThat(saved.getExecutionId()).isNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(saved.getStartedAt()).isNull();
        assertThat(saved.getFinishedAt()).isNull();
        assertThat(saved.getNextRetryAt()).isNotNull();
    }

    private void expireLease(Long id) {
        jdbcTemplate.update("""
            UPDATE payments
            SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private void allowRetry(Long id) {
        jdbcTemplate.update("""
            UPDATE payments
            SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private UUID awaitRun(Instance instance, String stage, Long id) {
        String[] parts = awaitMessage(instance, stage).split(":");
        assertThat(Long.valueOf(parts[2])).isEqualTo(id);
        return UUID.fromString(parts[3]);
    }

    private void assertCompleted(Instance instance, Long id, UUID executionId, int attempts) {
        assertThat(awaitRun(instance, "DONE", id)).isEqualTo(executionId);
        Payment saved = findPayment(id);
        assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(saved.getAttempts()).isEqualTo(attempts);
        assertThat(saved.getExecutionId()).isEqualTo(executionId);
        assertThat(saved.getResult()).isEqualTo("Payment execution completed");
        assertThat(saved.getFinishedAt()).isNotNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(saved.getNextRetryAt()).isNull();
        assertThat(paymentRepository.count()).isEqualTo(1);
        assertThat(instance.process().isAlive()).isTrue();
    }

    private String awaitMessage(Instance instance, String stage) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(instance.process().isAlive()).as(readLog(instance)).isTrue();
            assertThat(message(instance, stage)).as(readLog(instance)).isNotNull();
        });
        return message(instance, stage);
    }

    private String message(Instance instance, String stage) {
        return readLog(instance).lines()
            .filter(line -> line.startsWith("TEST:" + stage + ":") && line.endsWith(":END"))
            .findFirst()
            .orElse(null);
    }

    private String readLog(Instance instance) {
        try {
            return new String(Files.readAllBytes(instance.log()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void proceed(Instance instance) throws IOException {
        instance.process().getOutputStream().write("continue\n".getBytes(StandardCharsets.UTF_8));
        instance.process().getOutputStream().flush();
    }

    private void kill(Instance instance) throws InterruptedException {
        instance.process().destroyForcibly();
        assertThat(instance.process().waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(instance.process().isAlive()).isFalse();
    }

    private int countCalls(Long id) {
        return jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM test_calls WHERE payment_id = ?", Integer.class, id
        );
    }

    private int countEffects(Long id) {
        return jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM test_effects WHERE payment_id = ?", Integer.class, id
        );
    }

    private record Instance(Process process, Path log) {
    }
}
