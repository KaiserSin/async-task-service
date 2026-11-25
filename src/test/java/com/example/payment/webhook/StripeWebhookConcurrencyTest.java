package com.example.payment.webhook;

import com.example.payment.worker.config.WorkerTransactionConfig;
import com.example.payment.worker.WorkerProperties;
import com.example.payment.registration.PaymentCommand;
import com.example.payment.observability.PaymentMetrics;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.stripe.StripeGateway;
import com.example.payment.registration.PaymentRegistrationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.example.payment.domain.PaymentRepository;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.ReflectionTestUtils.setField;

@DataJpaTest(properties = {"spring.jpa.hibernate.ddl-auto=validate"})
@EnableConfigurationProperties(WorkerProperties.class)
@Import(PaymentRegistrationService.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class StripeWebhookConcurrencyTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private PaymentRepository payments;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private PaymentRegistrationService registration;

    @Autowired
    private WorkerProperties workerProperties;

    @Test
    @DisplayName("A stale webhook lookup cannot overwrite success committed by another instance")
    void preservesSuccessAcrossInstancesAndRetriesStaleEvent() throws Exception {
        Payment payment = payment(UUID.randomUUID(), "alice", "pm_test", 2500L);
        String intentId = "pi_concurrent_webhook";
        setField(payment, "executionStatus", ExecutionStatus.COMPLETED);
        setField(payment, "finishedAt", Instant.now());
        setField(payment, "providerPaymentId", intentId);
        setField(payment, "paymentStatus", PaymentStatus.REQUIRES_ACTION);
        payment = payments.saveAndFlush(payment);
        long initialVersion = payment.getVersion();

        Instance first = instance();
        Instance second = instance();
        var events = new StripeWebhookEventRepository(jdbc);
        events.insertIfAbsent("evt_stale_processing", "payment_intent.processing",
            payload(payment, "processing"));

        CountDownLatch lookupStarted = new CountDownLatch(1);
        CountDownLatch releaseLookup = new CountDownLatch(1);
        when(first.stripe().retrieveStatus(payment.getPaymentOperationId(), intentId)).thenAnswer(invocation -> {
            lookupStarted.countDown();
            assertThat(releaseLookup.await(10, TimeUnit.SECONDS)).isTrue();
            return PaymentStatus.PROCESSING;
        });

        try (var executor = Executors.newSingleThreadExecutor()) {
            var staleLookup = executor.submit(first.processor()::processNextEvent);
            try {
                assertThat(lookupStarted.await(10, TimeUnit.SECONDS)).isTrue();
                events.insertIfAbsent("evt_new_success", "payment_intent.succeeded",
                    payload(payment, "succeeded"));
                second.processor().processNextEvent();

                Payment succeeded = payments.findById(payment.getId()).orElseThrow();
                assertThat(succeeded.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
                assertThat(succeeded.getVersion()).isEqualTo(initialVersion + 1);
                assertThat(processed("evt_new_success")).isTrue();
                Timestamp firstTerminalAt = jdbc.queryForObject(
                    "SELECT first_terminal_at FROM payments WHERE id = ?", Timestamp.class, payment.getId());
                assertThat(firstTerminalAt).isNotNull();

                releaseLookup.countDown();
                staleLookup.get(10, TimeUnit.SECONDS);

                Payment afterStaleLookup = payments.findById(payment.getId()).orElseThrow();
                assertThat(afterStaleLookup.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
                assertThat(afterStaleLookup.getVersion()).isEqualTo(succeeded.getVersion());
                assertThat(processed("evt_stale_processing")).isFalse();
                assertThat(jdbc.queryForObject("""
                    SELECT technical_failures FROM stripe_webhook_events WHERE event_id = ?
                    """, Integer.class, "evt_stale_processing")).isZero();

                jdbc.update("""
                    UPDATE stripe_webhook_events SET available_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
                    WHERE event_id = ?
                    """, "evt_stale_processing");
                first.processor().processNextEvent();

                Payment retried = payments.findById(payment.getId()).orElseThrow();
                assertThat(retried.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
                assertThat(retried.getProviderPaymentId()).isEqualTo(intentId);
                assertThat(retried.getVersion()).isEqualTo(succeeded.getVersion() + 1);
                assertThat(jdbc.queryForObject(
                    "SELECT first_terminal_at FROM payments WHERE id = ?", Timestamp.class, payment.getId()))
                    .isEqualTo(firstTerminalAt);
                assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM stripe_webhook_events
                    WHERE event_id IN ('evt_stale_processing', 'evt_new_success') AND processed_at IS NOT NULL AND payload IS NULL
                    """, Long.class)).isEqualTo(2L);
                verify(first.stripe()).retrieveStatus(payment.getPaymentOperationId(), intentId);
                verifyNoInteractions(second.stripe());
            } finally {
                releaseLookup.countDown();
            }
        }
    }

    @Test
    @DisplayName("A stored webhook waits for registration without consuming its retry budget")
    void processesWaitingEventAfterRegistration() {
        var payment = payment(UUID.randomUUID(), "alice", "pm_test", 2500L);
        String intentId = "pi_before_registration";
        setField(payment, "providerPaymentId", intentId);
        String eventId = "evt_before_registration";
        String body = payload(payment, "succeeded");
        var events = new StripeWebhookEventRepository(jdbc);
        assertThat(events.insertIfAbsent(eventId, "payment_intent.succeeded", body)).isTrue();

        var instance = instance();
        for (int poll = 0; poll < 4; poll++) {
            instance.processor().processNextEvent();
        }
        assertThat(jdbc.queryForMap("""
            SELECT processed_at, technical_failures, available_at, review_reason, payload
            FROM stripe_webhook_events WHERE event_id = ?
            """, eventId)).containsEntry("processed_at", null).containsEntry("technical_failures", 0)
            .containsEntry("available_at", null).containsEntry("review_reason", null).containsEntry("payload", body);
        assertThat(payments.findByPaymentOperationId(payment.getPaymentOperationId())).isEmpty();

        registration.register(new PaymentCommand(payment.getEventId(), payment.getOwnerUsername(),
            payment.getPaymentOperationId(), payment.getOrderId(), payment.getAmount(), payment.getCurrency(),
            payment.getPaymentMethodId()));
        instance.processor().processNextEvent();

        var saved = payments.findByEventId(payment.getEventId()).orElseThrow();
        assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        assertThat(jdbc.queryForObject("""
            SELECT processed_at IS NOT NULL AND payload IS NULL AND technical_failures = 0
            FROM stripe_webhook_events WHERE event_id = ?
            """, Boolean.class, eventId)).isTrue();
        verifyNoInteractions(instance.stripe());
    }

    @Test
    @DisplayName("Three technical failures preserve the webhook for review and end automatic retries")
    void stopsProcessingAfterThreeTechnicalFailures() {
        var payment = payment(UUID.randomUUID(), "alice", "pm_test", 2500L);
        String intentId = "pi_technical_failure";
        setField(payment, "providerPaymentId", intentId);
        setField(payment, "paymentStatus", PaymentStatus.REQUIRES_ACTION);
        setField(payment, "executionStatus", ExecutionStatus.COMPLETED);
        setField(payment, "finishedAt", Instant.now());
        payments.saveAndFlush(payment);
        String eventId = "evt_technical_failure";
        String body = payload(payment, "processing");
        var events = new StripeWebhookEventRepository(jdbc);
        assertThat(events.insertIfAbsent(eventId, "payment_intent.processing", body)).isTrue();

        var instance = instance();
        when(instance.stripe().retrieveStatus(payment.getPaymentOperationId(), intentId))
            .thenThrow(new IllegalStateException("Stripe lookup failed"));
        for (int attempt = 1; attempt <= 3; attempt++) {
            assertThatThrownBy(instance.processor()::processNextEvent)
                .isInstanceOf(IllegalStateException.class).hasMessage("Stripe lookup failed");
            assertThat(jdbc.queryForMap("""
                SELECT technical_failures, processed_at, review_reason, payload FROM stripe_webhook_events WHERE event_id = ?
                """, eventId)).containsEntry("technical_failures", attempt).containsEntry("processed_at", null)
                .containsEntry("review_reason", attempt == 3 ? "TECHNICAL_ERROR" : null).containsEntry("payload", body);
            if (attempt < 3) {
                assertThat(jdbc.update("""
                    UPDATE stripe_webhook_events SET available_at = CURRENT_TIMESTAMP
                    WHERE event_id = ? AND available_at > CURRENT_TIMESTAMP
                    """, eventId)).isOne();
            }
        }

        instance.processor().processNextEvent();
        assertThat(jdbc.queryForObject("SELECT available_at IS NULL FROM stripe_webhook_events WHERE event_id = ?",
            Boolean.class, eventId)).isTrue();
        assertThat(payments.findById(payment.getId()).orElseThrow().getPaymentStatus()).isEqualTo(PaymentStatus.REQUIRES_ACTION);
        verify(instance.stripe(), times(3)).retrieveStatus(payment.getPaymentOperationId(), intentId);
    }

    private Instance instance() {
        var dataSource = new DriverManagerDataSource(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        var transactions = new WorkerTransactionConfig().workerTransactionTemplate(
            new DataSourceTransactionManager(dataSource), workerProperties);
        var stripe = mock(StripeGateway.class);
        var processor = new StripeWebhookProcessor(new StripeWebhookEventRepository(new JdbcTemplate(dataSource)),
            stripe, transactions, workerProperties, mock(PaymentMetrics.class));
        return new Instance(processor, stripe);
    }

    private boolean processed(String eventId) {
        return Boolean.TRUE.equals(jdbc.queryForObject(
            "SELECT processed_at IS NOT NULL FROM stripe_webhook_events WHERE event_id = ?", Boolean.class, eventId));
    }

    private String payload(Payment payment, String status) {
        return """
            {"livemode":false,"data":{"object":{"object":"payment_intent","id":"%s",
             "status":"%s","amount":%d,"currency":"%s",
             "metadata":{"paymentOperationId":"%s","orderId":"%s"}}}}
            """.formatted(payment.getProviderPaymentId(), status, payment.getAmount(), payment.getCurrency(),
                payment.getPaymentOperationId(), payment.getOrderId());
    }

    private record Instance(StripeWebhookProcessor processor, StripeGateway stripe) {
    }
}
