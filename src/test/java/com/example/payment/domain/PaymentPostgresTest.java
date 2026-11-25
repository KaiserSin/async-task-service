package com.example.payment.domain;

import com.example.payment.worker.config.WorkerTransactionConfig;
import com.example.payment.worker.WorkerProperties;
import com.example.payment.worker.attempt.PaymentAttempt;
import com.example.payment.registration.PaymentCommand;
import com.example.payment.registration.InvalidPaymentMessageException;
import com.example.payment.observability.PaymentMetrics;
import com.example.payment.observability.PaymentMetricsRepository;
import com.example.payment.maintenance.MaintenanceScheduler;
import com.example.payment.worker.WorkerLifecycle;
import com.example.payment.worker.attempt.PaymentClaimService;
import com.example.payment.worker.attempt.PaymentRunner;
import com.example.payment.worker.attempt.PaymentUpdateService;
import com.example.payment.stripe.StripeGateway;
import com.example.payment.maintenance.PaymentRecoveryService;
import com.example.payment.domain.PaymentRepository.RecoveryCounts;
import com.example.payment.maintenance.PaymentCleanupService;
import com.example.payment.registration.PaymentRegistrationService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.ReflectionTestUtils.setField;

@DataJpaTest(properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "worker.lease-seconds=47",
    "worker.retry-backoff-seconds=7",
    "payment.registration.max-new-operations-per-minute-per-owner=2"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({WorkerTransactionConfig.class, PaymentClaimService.class, PaymentUpdateService.class, PaymentRecoveryService.class,
    PaymentRegistrationService.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentPostgresTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentMetricsRepository metricsRepository;

    @Autowired
    private PaymentRegistrationService paymentRegistrationService;

    @Autowired
    private PaymentClaimService paymentClaimService;

    @Autowired
    private PaymentUpdateService paymentUpdateService;

    @MockitoBean
    private StripeGateway stripeGateway;

    @Autowired
    private PaymentRecoveryService paymentRecoveryService;

    @Autowired
    private WorkerProperties workerProperties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private DataSource dataSource;

    @PersistenceContext
    private EntityManager entityManager;

    private TransactionTemplate transactionTemplate;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAllInBatch();
        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setTimeout(30);
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executor != null) {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @DisplayName("The claim query uses the partial NEW index when completed payments dominate the table")
    void claimsThroughPartialNewIndex() throws Exception {
        jdbcTemplate.update("""
            INSERT INTO payments (event_id, owner_username, payment_operation_id, order_id, amount, currency, payment_method_id, idempotency_key, execution_status, finished_at)
            SELECT gen_random_uuid(), 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, 'COMPLETED', CURRENT_TIMESTAMP
            FROM generate_series(1, 10000)
            """);
        Payment ready = createPayment();
        jdbcTemplate.execute("ANALYZE payments");
        String claimSql = PaymentRepository.class.getMethod("claimNextExecution", UUID.class, int.class)
            .getAnnotation(org.springframework.data.jpa.repository.Query.class).value();

        String plan = String.join("\n", new NamedParameterJdbcTemplate(jdbcTemplate).queryForList(
            "EXPLAIN " + claimSql,
            Map.of("executionId", UUID.randomUUID(), "leaseSeconds", workerProperties.leaseSeconds()),
            String.class));

        assertThat(plan).contains("Index Scan using idx_payments_new_id").doesNotContain("Sort");
        assertThat(paymentClaimService.claimNextPayment().orElseThrow().id()).isEqualTo(ready.getId());
    }

    @Test
    @DisplayName("Configured lease and retry backoff reach claim, heartbeat and lease recovery SQL")
    void usesConfiguredLeaseAndRecoveryBackoff() {
        assertThat(workerProperties.leaseSeconds()).isEqualTo(47);
        assertThat(workerProperties.retryBackoffSeconds()).isEqualTo(7);
        Payment payment = createPayment();
        Instant claimStarted = databaseNow();

        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();

        Instant claimSaved = databaseNow();
        Payment claimed = findPayment(payment.getId());
        assertThat(claimedAttempt.id()).isEqualTo(claimed.getId());
        assertThat(claimedAttempt.eventId()).isEqualTo(claimed.getEventId());
        assertThat(claimedAttempt.executionId()).isNotNull().isEqualTo(claimed.getExecutionId());
        assertThat(claimedAttempt.paymentOperationId()).isEqualTo(claimed.getPaymentOperationId());
        assertThat(claimed.getLeaseUntil())
            .isBetween(claimStarted.plusSeconds(47), claimSaved.plusSeconds(47));

        Instant heartbeatStarted = databaseNow();
        assertThat(paymentUpdateService.renewLease(claimedAttempt.id(), claimedAttempt.executionId())).isTrue();
        Instant heartbeatSaved = databaseNow();
        assertThat(findPayment(payment.getId()).getLeaseUntil())
            .isBetween(heartbeatStarted.plusSeconds(47), heartbeatSaved.plusSeconds(47));

        expireLease(payment.getId());
        Instant recoveryStarted = databaseNow();
        assertThat(paymentRecoveryService.recoverExpiredLeases()).isEqualTo(new RecoveryCounts(1, 0));
        Instant recoverySaved = databaseNow();
        Payment recovered = findPayment(payment.getId());
        assertThat(recovered.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(recovered.getNextRetryAt())
            .isBetween(recoveryStarted.plusSeconds(7), recoverySaved.plusMillis(10_500));
        assertThat(paymentClaimService.claimNextPayment()).isEmpty();
    }

    @Test
    @DisplayName("Concurrent registrations share the last owner admission slot and an exact replay is accepted at capacity")
    void serializesLastOwnerAdmissionSlotAndAcceptsReplay() throws Exception {
        Payment existing = createPayment();
        jdbcTemplate.update("UPDATE payments SET created_at = statement_timestamp() WHERE id = ?", existing.getId());
        PaymentCommand first = new PaymentCommand(
            UUID.randomUUID(), "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "eur", "pm_card_visa"
        );
        PaymentCommand second = new PaymentCommand(
            UUID.randomUUID(), "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "eur", "pm_card_visa"
        );
        assertThat(paymentRepository.countRecentByOwnerUsername("alice")).isEqualTo(1);
        CountDownLatch admitted = new CountDownLatch(1);
        CountDownLatch commit = new CountDownLatch(1);
        CompletableFuture<Integer> competingBackend = new CompletableFuture<>();
        Future<?> firstRegistration = executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
            paymentRegistrationService.register(first);
            admitted.countDown();
            awaitLatch(commit);
        }));
        Future<?> secondRegistration;

        try {
            awaitLatch(admitted);
            secondRegistration = executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
                competingBackend.complete(jdbcTemplate.queryForObject("SELECT pg_backend_pid()", Integer.class));
                paymentRegistrationService.register(second);
            }));
            int backend = competingBackend.get(2, TimeUnit.SECONDS);

            await().atMost(Duration.ofSeconds(5)).until(() -> Boolean.TRUE.equals(jdbcTemplate.queryForObject("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_stat_activity
                    WHERE pid = ? AND wait_event_type = 'Lock' AND wait_event = 'advisory'
                )
                """, Boolean.class, backend)));
            assertThat(secondRegistration.isDone()).isFalse();
            assertThat(paymentRepository.countRecentByOwnerUsername("alice")).isEqualTo(1);
        } finally {
            commit.countDown();
            firstRegistration.get(10, TimeUnit.SECONDS);
        }

        assertThatThrownBy(() -> secondRegistration.get(10, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(InvalidPaymentMessageException.class)
            .hasRootCauseMessage("New payment operation limit reached for eventId=" + second.eventId());
        assertThat(paymentRepository.count()).isEqualTo(2);
        assertThat(paymentRepository.countRecentByOwnerUsername("alice")).isEqualTo(2);
        assertThat(paymentRepository.findByEventId(second.eventId())).isEmpty();
        assertThat(paymentRepository.findByPaymentOperationId(second.paymentOperationId())).isEmpty();
        Payment accepted = paymentRepository.findByEventId(first.eventId()).orElseThrow();

        assertThatCode(() -> paymentRegistrationService.register(first)).doesNotThrowAnyException();

        assertThat(paymentRepository.count()).isEqualTo(2);
        assertThat(paymentRepository.findByEventId(first.eventId()).orElseThrow())
            .usingRecursiveComparison().isEqualTo(accepted);
    }

    @Test
    @DisplayName("Two workers claim different payments")
    void claimsTwoPayments() throws Exception {
        Payment first = createPayment();
        Payment second = createPayment();
        CyclicBarrier start = new CyclicBarrier(2);
        Callable<Optional<PaymentAttempt>> claim = () -> {

            start.await(10, TimeUnit.SECONDS);
            return paymentClaimService.claimNextPayment();
        };

        Future<Optional<PaymentAttempt>> firstClaim = executor.submit(claim);
        Future<Optional<PaymentAttempt>> secondClaim = executor.submit(claim);
        PaymentAttempt firstRun = firstClaim.get(10, TimeUnit.SECONDS).orElseThrow();
        PaymentAttempt secondRun = secondClaim.get(10, TimeUnit.SECONDS).orElseThrow();

        assertThat(List.of(firstRun.id(), secondRun.id()))
            .containsExactlyInAnyOrder(first.getId(), second.getId());
        assertThat(firstRun.executionId()).isNotNull().isNotEqualTo(secondRun.executionId());
        assertThat(secondRun.executionId()).isNotNull();
        for (Payment payment : List.of(first, second)) {
            Payment saved = findPayment(payment.getId());
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
            assertThat(saved.getAttempts()).isEqualTo(1);
            assertThat(saved.getVersion()).isEqualTo(payment.getVersion() + 1);
        }
        assertThat(findPayment(firstRun.id()).getExecutionId()).isEqualTo(firstRun.executionId());
        assertThat(findPayment(secondRun.id()).getExecutionId()).isEqualTo(secondRun.executionId());
    }

    @Test
    @DisplayName("Native SQL skips a locked payment")
    void skipsLockedPayment() throws Exception {
        Payment first = createPayment();
        Payment second = createPayment();
        Payment third = createPayment();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> holder = lockPayment(first.getId(), locked, release);

        try {
            awaitLatch(locked);
            Future<Optional<PaymentAttempt>> claim = executor.submit(paymentClaimService::claimNextPayment);

            PaymentAttempt claimedAttempt = claim.get(10, TimeUnit.SECONDS).orElseThrow();

            assertThat(claimedAttempt.id()).isEqualTo(second.getId());
        } finally {
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        }
        assertThat(findPayment(first.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(findPayment(third.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
    }

    @Test
    @DisplayName("An old entity version does not overwrite a claimed payment")
    void rejectsOldVersion() throws Exception {
        Payment payment = createPayment();
        CountDownLatch loaded = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> writer = executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
            Payment oldPayment = entityManager.find(Payment.class, payment.getId());
            loaded.countDown();
            awaitLatch(release);
            setField(oldPayment, "result", "Old change");
            entityManager.flush();
        }));

        try {
            awaitLatch(loaded);
            PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();

            assertThat(claimedAttempt.id()).isEqualTo(payment.getId());
        } finally {
            release.countDown();
            assertThatThrownBy(() -> writer.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OptimisticLockException.class);
        }

        Payment saved = findPayment(payment.getId());
        assertThat(saved.getResult()).isEqualTo(payment.getResult());
        assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
        assertThat(saved.getVersion()).isEqualTo(payment.getVersion() + 1);
    }


    @Test
    @DisplayName("An expired lease cannot be revived by a heartbeat or completed by its old worker")
    void rejectsHeartbeatAndCompletionAfterLeaseExpiry() {
        Payment payment = createPayment();
        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
        expireLease(payment.getId());
        Payment before = findPayment(payment.getId());

        assertThat(paymentUpdateService.renewLease(claimedAttempt.id(), claimedAttempt.executionId())).isFalse();
        assertThat(paymentUpdateService.complete(claimedAttempt.id(), claimedAttempt.executionId(), "Late result")).isFalse();

        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @Test
    @DisplayName("An old worker does not change a new execution attempt")
    void rejectsOldWorker() {
        Payment payment = createPayment();
        PaymentAttempt oldRun = paymentClaimService.claimNextPayment().orElseThrow();

        expireLease(payment.getId());
        assertThat(paymentRecoveryService.recoverExpiredLeases()).isEqualTo(new RecoveryCounts(1, 0));
        jdbcTemplate.update("""
            UPDATE payments
            SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, payment.getId());
        PaymentAttempt newRun = paymentClaimService.claimNextPayment().orElseThrow();
        assertThat(newRun.id()).isEqualTo(oldRun.id());
        assertThat(newRun.executionId()).isNotEqualTo(oldRun.executionId());
        assertThat(paymentUpdateService.renewLease(newRun.id(), newRun.executionId())).isTrue();
        Payment before = findPayment(payment.getId());

        boolean leaseRenewed = paymentUpdateService.renewLease(oldRun.id(), oldRun.executionId());
        boolean finished = paymentUpdateService.complete(oldRun.id(), oldRun.executionId(), "Old result");
        boolean requeued = paymentUpdateService.requeueInterruptedPayment(oldRun.id(), oldRun.executionId());
        Optional<ExecutionStatus> failureSaved = paymentUpdateService.fail(oldRun.id(), oldRun.executionId(), "Old failure");

        assertThat(leaseRenewed).isFalse();
        assertThat(finished).isFalse();
        assertThat(requeued).isFalse();
        assertThat(failureSaved).isEmpty();
        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(paymentUpdateService.complete(newRun.id(), newRun.executionId(), "New result")).isTrue();
        Payment saved = findPayment(payment.getId());
        assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(saved.getResult()).isEqualTo("New result");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    @DisplayName("An interruption resets execution state and preserves the retry budget, including the last attempt")
    void requeuesInterruptedAttempt(int previousAttempts) {
        Payment payment = createPayment();
        jdbcTemplate.update("UPDATE payments SET attempts = ? WHERE id = ?", previousAttempts, payment.getId());
        PaymentAttempt interrupted = paymentClaimService.claimNextPayment().orElseThrow();
        assertThat(paymentUpdateService.renewLease(payment.getId(), interrupted.executionId())).isTrue();
        jdbcTemplate.update("UPDATE payments SET result = 'Partial result' WHERE id = ?", payment.getId());
        Payment before = findPayment(payment.getId());

        assertThat(paymentUpdateService.requeueInterruptedPayment(payment.getId(), interrupted.executionId())).isTrue();

        Payment requeued = findPayment(payment.getId());
        assertThat(requeued.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(requeued.getAttempts()).isEqualTo(previousAttempts);
        assertThat(requeued.getResult()).isNull();
        assertThat(requeued.getExecutionId()).isNull();
        assertThat(requeued.getLeaseUntil()).isNull();
        assertThat(requeued.getNextRetryAt()).isNull();
        assertThat(requeued.getStartedAt()).isNull();
        assertThat(requeued.getFinishedAt()).isNull();
        assertThat(requeued.getCreatedAt()).isEqualTo(before.getCreatedAt());
        assertThat(requeued.getVersion()).isEqualTo(before.getVersion() + 1);
        assertThat(paymentUpdateService.requeueInterruptedPayment(payment.getId(), interrupted.executionId())).isFalse();
        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(requeued);

        PaymentAttempt resumed = paymentClaimService.claimNextPayment().orElseThrow();
        assertThat(resumed.id()).isEqualTo(payment.getId());
        assertThat(resumed.executionId()).isNotEqualTo(interrupted.executionId());
        assertThat(findPayment(payment.getId()).getAttempts()).isEqualTo(previousAttempts + 1);
        assertThat(paymentUpdateService.complete(resumed.id(), resumed.executionId(), "Payment execution completed")).isTrue();
        assertThat(findPayment(payment.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
    }

    @Test
    @DisplayName("An interrupted attempt with an expired lease cannot bypass lease recovery")
    void rejectsRequeueAfterLeaseExpiry() {
        Payment payment = createPayment();
        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
        expireLease(payment.getId());
        Payment before = findPayment(payment.getId());

        assertThat(paymentUpdateService.requeueInterruptedPayment(payment.getId(), claimedAttempt.executionId())).isFalse();

        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(paymentRecoveryService.recoverExpiredLeases()).isEqualTo(new RecoveryCounts(1, 0));
        assertThat(findPayment(payment.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(findPayment(payment.getId()).getAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("A late interruption cannot put a completed payment back into the queue")
    void rejectsRequeueOfCompletedPayment() {
        Payment payment = createPayment();
        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
        assertThat(paymentUpdateService.complete(claimedAttempt.id(), claimedAttempt.executionId(), "Payment execution completed")).isTrue();
        Payment before = findPayment(payment.getId());

        assertThat(paymentUpdateService.requeueInterruptedPayment(payment.getId(), claimedAttempt.executionId())).isFalse();

        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4})
    @DisplayName("Failed executions use the configured backoff and stop at the payment's attempt limit")
    void retriesFailuresUntilAttemptLimit(int maxAttempts) {
        Payment payment = createPayment();
        jdbcTemplate.update("UPDATE payments SET max_attempts = ? WHERE id = ?", maxAttempts, payment.getId());

        UUID previousExecutionId = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
            assertThat(claimedAttempt.id()).isEqualTo(payment.getId());
            assertThat(claimedAttempt.executionId()).isNotEqualTo(previousExecutionId);
            assertThat(paymentUpdateService.renewLease(claimedAttempt.id(), claimedAttempt.executionId())).isTrue();
            Payment before = findPayment(payment.getId());
            Instant failureStarted = databaseNow();
            String failure = "Failure on attempt " + attempt;

            assertThat(paymentUpdateService.fail(claimedAttempt.id(), claimedAttempt.executionId(), failure))
                .contains(attempt < maxAttempts ? ExecutionStatus.NEW : ExecutionStatus.FAILED);

            Instant failureSaved = databaseNow();
            Payment saved = findPayment(payment.getId());
            assertThat(saved.getAttempts()).isEqualTo(attempt);
            assertThat(saved.getMaxAttempts()).isEqualTo(maxAttempts);
            assertThat(saved.getLeaseUntil()).isNull();
            assertThat(saved.getResult()).isEqualTo(failure);
            assertThat(saved.getVersion()).isEqualTo(before.getVersion() + 1);
            assertThat(saved.getCreatedAt()).isEqualTo(before.getCreatedAt());

            if (attempt < maxAttempts) {
                assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
                long baseDelaySeconds = workerProperties.retryBackoffSeconds() * (attempt == 1 ? 1L : 2L);
                assertThat(saved.getNextRetryAt()).isBetween(failureStarted.plusSeconds(baseDelaySeconds),
                    failureSaved.plusMillis(baseDelaySeconds * 1500));
                assertThat(saved.getExecutionId()).isNull();
                assertThat(saved.getStartedAt()).isNull();
                assertThat(saved.getFinishedAt()).isNull();
            } else {
                assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
                assertThat(saved.getNextRetryAt()).isNull();
                assertThat(saved.getExecutionId()).isEqualTo(claimedAttempt.executionId());
                assertThat(saved.getStartedAt()).isEqualTo(before.getStartedAt());
                assertThat(saved.getFinishedAt()).isBetween(failureStarted, failureSaved);
            }

            assertThat(paymentClaimService.claimNextPayment()).isEmpty();
            assertThat(paymentUpdateService.fail(claimedAttempt.id(), claimedAttempt.executionId(), "Duplicate failure"))
                .isEmpty();
            assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(saved);

            if (attempt < maxAttempts) {
                allowRetry(payment.getId());
            }
            previousExecutionId = claimedAttempt.executionId();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    @DisplayName("Only a committed terminal handler failure increments the failed counter")
    void countsOnlyTerminalHandlerFailure(int maxAttempts) {
        Payment payment = createPayment();
        jdbcTemplate.update("UPDATE payments SET max_attempts = ? WHERE id = ?", maxAttempts, payment.getId());
        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            PaymentMetrics metrics = new PaymentMetrics(registry, metricsRepository);
            PaymentRunner paymentRunner = new PaymentRunner(paymentUpdateService, metrics,
                claimed -> { throw new IllegalStateException("Handler failed"); });

            paymentRunner.executePayment(claimedAttempt);

            assertThat(findPayment(payment.getId()).getExecutionStatus())
                .isEqualTo(maxAttempts == 1 ? ExecutionStatus.FAILED : ExecutionStatus.NEW);
            assertThat(registry.get("payment.execution.failed").counter().count()).isEqualTo(maxAttempts == 1 ? 1 : 0);
            assertThat(registry.get("payment.execution.completed").counter().count()).isZero();

            paymentRunner.executePayment(claimedAttempt);

            assertThat(registry.get("payment.execution.failed").counter().count()).isEqualTo(maxAttempts == 1 ? 1 : 0);
            assertThat(findPayment(payment.getId()).getAttempts()).isEqualTo(1);
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("Only a committed completion increments the completed counter; a stale final write does not")
    void countsCompletionOnceAcrossStaleFinalWrites() {
        Payment payment = createPayment();
        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            PaymentMetrics metrics = new PaymentMetrics(registry, metricsRepository);
            PaymentRunner paymentRunner = new PaymentRunner(paymentUpdateService, metrics,
                claimed -> {});

            paymentRunner.executePayment(claimedAttempt);
            paymentRunner.executePayment(claimedAttempt);

            assertThat(findPayment(payment.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(registry.get("payment.execution.completed").counter().count()).isEqualTo(1);
            assertThat(registry.get("payment.execution.failed").counter().count()).isZero();
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("Recovery counts committed failures separately from retries without counting a payment twice")
    void countsTerminalLeaseRecoveryOnce() {
        Payment exhausted = createPayment();
        jdbcTemplate.update("UPDATE payments SET attempts = 2 WHERE id = ?", exhausted.getId());
        Payment retryable = createPayment();
        paymentClaimService.claimNextPayment().orElseThrow();
        paymentClaimService.claimNextPayment().orElseThrow();
        expireLease(exhausted.getId());
        expireLease(retryable.getId());
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            PaymentMetrics metrics = new PaymentMetrics(registry, metricsRepository);
            MaintenanceScheduler scheduler = new MaintenanceScheduler(mock(WorkerLifecycle.class),
                paymentRecoveryService, mock(PaymentCleanupService.class), metrics);

            scheduler.recoverExpiredLeases();
            scheduler.recoverExpiredLeases();

            assertThat(findPayment(exhausted.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(findPayment(retryable.getId()).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
            assertThat(registry.get("payment.execution.failed").counter().count()).isEqualTo(1);
            assertThat(registry.get("payment.lease.recovered").counter().count()).isEqualTo(1);
            assertThat(registry.get("payment.execution.completed").counter().count()).isZero();
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("A payment can complete after a failed attempt without losing or refunding its consumed retry")
    void completesAfterTransientExecutionFailure() {
        Payment payment = createPayment();
        PaymentAttempt firstRun = paymentClaimService.claimNextPayment().orElseThrow();
        assertThat(paymentUpdateService.fail(firstRun.id(), firstRun.executionId(), "Transient failure"))
            .contains(ExecutionStatus.NEW);
        assertThat(paymentClaimService.claimNextPayment()).isEmpty();
        allowRetry(payment.getId());

        PaymentAttempt retriedRun = paymentClaimService.claimNextPayment().orElseThrow();

        assertThat(retriedRun.executionId()).isNotEqualTo(firstRun.executionId());
        assertThat(findPayment(payment.getId()).getAttempts()).isEqualTo(2);
        assertThat(findPayment(payment.getId()).getResult()).isNull();
        assertThat(paymentUpdateService.complete(retriedRun.id(), retriedRun.executionId(), "Payment execution completed")).isTrue();
        Payment completed = findPayment(payment.getId());
        assertThat(completed.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(completed.getAttempts()).isEqualTo(2);
        assertThat(completed.getResult()).isEqualTo("Payment execution completed");
        assertThat(completed.getNextRetryAt()).isNull();
        assertThat(paymentClaimService.claimNextPayment()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"expired", "completed"})
    @DisplayName("A late execution failure cannot change an expired or completed payment")
    void rejectsLateExecutionFailure(String state) {
        Payment payment = createPayment();
        PaymentAttempt claimedAttempt = paymentClaimService.claimNextPayment().orElseThrow();
        if (state.equals("expired")) {
            expireLease(payment.getId());
        } else {
            assertThat(paymentUpdateService.complete(claimedAttempt.id(), claimedAttempt.executionId(), "Payment execution completed")).isTrue();
        }
        Payment before = findPayment(payment.getId());

        assertThat(paymentUpdateService.fail(claimedAttempt.id(), claimedAttempt.executionId(), "Late failure")).isEmpty();

        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @Test
    @DisplayName("An expired payment becomes FAILED after the last attempt")
    void stopsAfterLastAttempt() {
        Payment payment = createPayment();
        jdbcTemplate.update("UPDATE payments SET attempts = 2 WHERE id = ?", payment.getId());

        paymentClaimService.claimNextPayment().orElseThrow();
        assertThat(findPayment(payment.getId()).getAttempts()).isEqualTo(3);
        expireLease(payment.getId());
        assertThat(paymentRecoveryService.recoverExpiredLeases()).isEqualTo(new RecoveryCounts(0, 1));
        assertThat(paymentRecoveryService.recoverExpiredLeases()).isEqualTo(new RecoveryCounts(0, 0));

        Payment saved = findPayment(payment.getId());
        assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(saved.getAttempts()).isEqualTo(3);
        assertThat(saved.getFinishedAt()).isNotNull();
        assertThat(saved.getExecutionId()).isNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(paymentClaimService.claimNextPayment()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 4})
    @DisplayName("A payment at or above the attempt limit is not claimed")
    void rejectsSpentAttempts(int attempts) {
        Payment payment = createPayment();
        jdbcTemplate.update("UPDATE payments SET attempts = ? WHERE id = ?", attempts, payment.getId());
        Payment before = findPayment(payment.getId());

        Optional<PaymentAttempt> claimedAttempt = paymentClaimService.claimNextPayment();

        assertThat(claimedAttempt).isEmpty();
        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @Test
    @DisplayName("JPA saves and reads all fields of a completed payment")
    void storesPayment() {
        Payment payment = payment(UUID.randomUUID(), "alice", "pm_test", 60000L);
        setField(payment, "executionStatus", ExecutionStatus.COMPLETED);
        setField(payment, "result", "Done");
        setField(payment, "executionId", UUID.randomUUID());
        setField(payment, "attempts", 2);
        setField(payment, "maxAttempts", 5);
        setField(payment, "createdAt", Instant.parse("2026-01-01T10:00:00Z"));
        setField(payment, "startedAt", Instant.parse("2026-01-01T10:01:00Z"));
        setField(payment, "finishedAt", Instant.parse("2026-01-01T10:02:00Z"));

        Payment saved = paymentRepository.saveAndFlush(payment);

        assertThat(saved.getId()).isPositive();
        assertThat(saved.getVersion()).isZero();
        assertThat(findPayment(saved.getId())).usingRecursiveComparison().isEqualTo(saved);
    }

    @Test
    @DisplayName("PostgreSQL rejects duplicate eventId values")
    void rejectsDuplicate() {
        UUID eventId = UUID.randomUUID();
        String sql = "INSERT INTO payments (event_id, owner_username, payment_operation_id, order_id, amount, currency, payment_method_id, idempotency_key) VALUES (?, ?, gen_random_uuid(), gen_random_uuid(), ?, 'eur', 'pm_test', gen_random_uuid()::text)";
        jdbcTemplate.update(sql, eventId, "First payment", 1000L);

        assertThatThrownBy(() -> jdbcTemplate.update(sql, eventId, "Duplicate", 1000L))
            .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(paymentRepository.count()).isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "amount = 0", "amount = 100000000", "currency = 'EU'",
        "execution_status = 'UNKNOWN'",
        "execution_status = 'IN_PROGRESS', started_at = CURRENT_TIMESTAMP",
        "execution_status = 'IN_PROGRESS', execution_id = '11111111-1111-1111-1111-111111111111'::uuid",
        "execution_status = 'COMPLETED'", "execution_status = 'FAILED'",
        "attempts = -1", "max_attempts = 0", "event_id = NULL", "payment_method_id = NULL"
    })
    @DisplayName("PostgreSQL constraints reject invalid data")
    void rejectsBadData(String change) {
        Payment payment = createPayment();
        Payment before = findPayment(payment.getId());

        assertThatThrownBy(() -> jdbcTemplate.update(
            "UPDATE payments SET " + change + " WHERE id = ?", payment.getId()
        )).isInstanceOf(DataIntegrityViolationException.class);

        assertThat(findPayment(payment.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    private Payment createPayment() {
        return paymentRepository.saveAndFlush(payment(UUID.randomUUID(), "alice", "pm_test", 2500L));
    }

    private Payment findPayment(Long id) {
        return paymentRepository.findById(id).orElseThrow();
    }

    private Future<?> lockPayment(Long id, CountDownLatch locked, CountDownLatch release) {
        return executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
            Payment payment = paymentRepository.findByIdForUpdate(id).orElseThrow();
            assertThat(payment.getId()).isEqualTo(id);

            locked.countDown();
            awaitLatch(release);
        }));
    }

    private void expireLease(Long id) {
        jdbcTemplate.update("""
            UPDATE payments
            SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private Instant databaseNow() {
        return jdbcTemplate.queryForObject("SELECT CURRENT_TIMESTAMP", Timestamp.class).toInstant();
    }

    private void allowRetry(Long id) {
        jdbcTemplate.update("""
            UPDATE payments
            SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Test wait was interrupted", e);
        }
    }
}
