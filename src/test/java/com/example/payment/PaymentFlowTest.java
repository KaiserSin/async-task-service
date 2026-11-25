package com.example.payment;

import com.example.payment.registration.PaymentCommand;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.worker.attempt.PaymentClaimService;
import com.example.payment.stripe.StripeGateway;
import com.example.payment.maintenance.MaintenanceScheduler;
import com.example.payment.maintenance.PaymentCleanupService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stripe.Stripe;
import com.stripe.StripeClient;
import com.stripe.exception.ApiConnectionException;
import com.stripe.net.ApiResource.RequestMethod;
import com.stripe.net.HttpClient;
import com.stripe.net.HttpHeaders;
import com.stripe.net.StripeRequest;
import com.stripe.net.StripeResponse;
import com.stripe.net.Webhook;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = PaymentApplication.class, properties = {
    "stripe.secret-key=sk_test_payment_flow", "stripe.webhook-secret=whsec_payment_flow"
})
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentFlowTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.16");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private PaymentRepository paymentRepository;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private HikariDataSource dataSource;
    @Autowired
    private StripeGateway stripeGateway;

    @Autowired
    private PaymentClaimService paymentClaimService;

    @Autowired
    private PaymentCleanupService paymentCleanupService;
    @Autowired
    private MaintenanceScheduler maintenanceScheduler;

    @Test
    @DisplayName("A Kafka payment creates a linked PaymentIntent and the owner sees success after the webhook")
    void completesPaymentAfterWebhook() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_payment_flow";
        String eventId = "evt_payment_flow";
        var intent = paymentIntent(dto, intentId, "succeeded");
        HttpClient transport = stubStripe(200, intent);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(payment.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
            assertThat(payment.getProviderPaymentId()).isEqualTo(intentId);
        });

        var request = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport).request(request.capture());
        StripeRequest create = request.getValue();
        assertThat(create.method()).isEqualTo(RequestMethod.POST);
        assertThat(create.url().getPath()).isEqualTo("/v1/payment_intents");
        assertThat(create.headers().firstValue("Idempotency-Key")).contains(dto.paymentOperationId().toString());
        assertThat(create.params()).containsEntry("metadata", intent.get("metadata")).containsEntry("confirm", true)
            .containsEntry("currency", dto.currency()).containsEntry("payment_method", dto.paymentMethodId());
        assertThat(objectMapper.valueToTree(create.params()).path("amount").longValue()).isEqualTo(dto.amount());

        sendWebhook(eventId, "payment_intent.succeeded", intent);

        assertWebhookProcessed(eventId);
        var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(payment.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        mockMvc.perform(get("/api/payments/by-event-id/{eventId}", dto.eventId())
                .with(httpBasic("alice", "alice-test-password")))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.paymentOperationId").value(dto.paymentOperationId().toString()))
            .andExpect(jsonPath("$.orderId").value(dto.orderId().toString()))
            .andExpect(jsonPath("$.providerPaymentId").value(intentId))
            .andExpect(jsonPath("$.executionStatus").value("COMPLETED"))
            .andExpect(jsonPath("$.paymentStatus").value("SUCCEEDED"));
    }

    @ParameterizedTest(name = "{0} returns HTTP {1} and leaves the payment unchanged")
    @CsvSource({"wrongSecret, 400", "changedBody, 400", "expiredSignature, 400", "liveMode, 200"})
    void ignoresInvalidWebhookDelivery(String scenario, int expectedStatus) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "webhook-" + scenario, UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_signature_" + scenario;
        String eventId = "evt_signature_" + scenario;
        var intent = paymentIntent(dto, intentId, "succeeded");
        stubStripe(200, intent);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(payment.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
            assertThat(payment.getProviderPaymentId()).isEqualTo(intentId);
        });
        var before = paymentRepository.findByEventId(dto.eventId()).orElseThrow();

        boolean liveMode = scenario.equals("liveMode");
        intent.put("livemode", liveMode);
        long now = Instant.now().getEpochSecond();
        String payload = webhookPayload(eventId, "payment_intent.succeeded", intent, now, liveMode);
        String secret = scenario.equals("wrongSecret") ? "whsec_other_endpoint" : "whsec_payment_flow";
        long signedAt = scenario.equals("expiredSignature") ? now - Webhook.DEFAULT_TOLERANCE - 60 : now;
        String signature = Webhook.Signature.generateSignatureHeader(payload, secret, signedAt);
        String body = scenario.equals("changedBody") ? payload + "\n" : payload;
        mockMvc.perform(post("/api/webhooks/stripe").contentType(MediaType.APPLICATION_JSON)
                .header("Stripe-Signature", signature).content(body))
            .andExpect(status().is(expectedStatus));

        assertThat(jdbc.queryForObject("SELECT count(*) FROM stripe_webhook_events WHERE event_id = ?",
            Long.class, eventId)).isZero();
        assertThat(paymentRepository.findByEventId(dto.eventId()).orElseThrow()).usingRecursiveComparison().isEqualTo(before);
    }

    @ParameterizedTest(name = "A signed success event is kept for review with reason {0}")
    @ValueSource(strings = {"AMOUNT_MISMATCH", "MISSING_OPERATION_ID", "INVALID_OPERATION_ID", "OPERATION_MISMATCH",
        "INTENT_MISMATCH", "ORDER_MISMATCH", "CURRENCY_MISMATCH"})
    void preservesPaymentWhenSignedWebhookDetailsAreInvalid(String reason) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "webhook-review", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_review_" + dto.paymentOperationId();
        String eventId = "evt_review_" + dto.paymentOperationId();
        var intent = paymentIntent(dto, intentId, "succeeded");
        HttpClient transport = stubStripe(200, intent);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });
        var before = paymentRepository.findByEventId(dto.eventId()).orElseThrow();

        switch (reason) {
            case "AMOUNT_MISMATCH" -> intent.put("amount", dto.amount() + 1);
            case "CURRENCY_MISMATCH" -> intent.put("currency", "eur");
            case "INTENT_MISMATCH" -> intent.put("id", "pi_other");
            case "ORDER_MISMATCH" -> intent.put("metadata", Map.of("paymentOperationId",
                dto.paymentOperationId().toString(), "orderId", UUID.randomUUID().toString()));
            case "OPERATION_MISMATCH", "INVALID_OPERATION_ID" -> intent.put("metadata", Map.of("paymentOperationId",
                reason.equals("INVALID_OPERATION_ID") ? "invalid" : UUID.randomUUID().toString(),
                "orderId", dto.orderId().toString()));
            case "MISSING_OPERATION_ID" -> intent.put("metadata", Map.of("orderId", dto.orderId().toString()));
            default -> throw new AssertionError(reason);
        }
        String payload = webhookPayload(eventId, "payment_intent.succeeded", intent, Instant.now().getEpochSecond(), false);
        postSignedWebhook(payload).andExpect(status().isOk());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(jdbc.queryForMap("""
                SELECT review_reason, payload, processed_at, available_at, technical_failures
                FROM stripe_webhook_events WHERE event_id = ?
                """, eventId)).containsEntry("review_reason", reason).containsEntry("payload", payload)
                .containsEntry("processed_at", null).containsEntry("available_at", null)
                .containsEntry("technical_failures", 0));
        assertThat(paymentRepository.findByEventId(dto.eventId()).orElseThrow()).usingRecursiveComparison().isEqualTo(before);
        assertSingleChargeAttempt(transport);
    }

    @Test
    @DisplayName("A webhook returns 503 during a database outage and succeeds on redelivery")
    void acceptsWebhookOnlyAfterDatabaseRecovers() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "webhook-recovery", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_database_outage";
        String eventId = "evt_database_outage";
        var intent = paymentIntent(dto, intentId, "succeeded");
        stubStripe(200, intent);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
        });
        String payload = webhookPayload(eventId, "payment_intent.succeeded", intent, Instant.now().getEpochSecond(), false);

        var docker = postgres.getDockerClient();
        docker.pauseContainerCmd(postgres.getContainerId()).exec();
        try {
            dataSource.getHikariPoolMXBean().softEvictConnections();
            postSignedWebhook(payload).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("SERVICE_UNAVAILABLE"));
        } finally {
            docker.unpauseContainerCmd(postgres.getContainerId()).exec();
        }

        await().atMost(Duration.ofSeconds(30)).ignoreExceptionsInstanceOf(DataAccessException.class)
            .untilAsserted(() -> assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM stripe_webhook_events WHERE event_id = ?", Long.class, eventId)).isZero());
        assertThat(paymentRepository.findByEventId(dto.eventId()).orElseThrow().getPaymentStatus())
            .isEqualTo(PaymentStatus.AWAITING_WEBHOOK);

        postSignedWebhook(payload).andExpect(status().isOk());
        assertWebhookProcessed(eventId);
        var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
    }

    @Test
    @DisplayName("Repeated deliveries and separate success events record the same payment outcome once")
    void recordsSuccessOnceAcrossWebhookDeliveries() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_repeated_success";
        String firstEvent = "evt_first_success";
        String secondEvent = "evt_second_success";
        var intent = paymentIntent(dto, intentId, "succeeded");
        HttpClient transport = stubStripe(200, intent);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(payment.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
        });

        String terminalTime = "SELECT first_terminal_at FROM payments WHERE payment_operation_id = ?";
        String processedTime = "SELECT processed_at FROM stripe_webhook_events WHERE event_id = ?";
        assertThat(jdbc.queryForObject(terminalTime, Timestamp.class, dto.paymentOperationId())).isNull();
        sendWebhook(firstEvent, "payment_intent.succeeded", intent);
        assertWebhookProcessed(firstEvent);
        var succeeded = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(succeeded.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        var firstTerminalAt = jdbc.queryForObject(terminalTime, Timestamp.class, dto.paymentOperationId());
        assertThat(firstTerminalAt).isNotNull();
        var processedAt = jdbc.queryForObject(processedTime, Timestamp.class, firstEvent);

        sendWebhook(firstEvent, "payment_intent.succeeded", intent);
        assertThat(paymentRepository.findByEventId(dto.eventId()).orElseThrow().getVersion())
            .isEqualTo(succeeded.getVersion());
        sendWebhook(secondEvent, "payment_intent.succeeded", intent);
        assertWebhookProcessed(secondEvent);

        assertThat(jdbc.queryForObject(processedTime, Timestamp.class, firstEvent)).isEqualTo(processedAt);
        assertThat(jdbc.queryForObject("""
            SELECT count(*) FROM stripe_webhook_events
            WHERE event_id IN (?, ?) AND processed_at IS NOT NULL AND payload IS NULL
            """, Long.class, firstEvent, secondEvent)).isEqualTo(2L);
        var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(payment.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(payment.getProviderPaymentId()).isEqualTo(intentId);
        assertThat(jdbc.queryForObject(terminalTime, Timestamp.class, dto.paymentOperationId())).isEqualTo(firstTerminalAt);
        assertSingleChargeAttempt(transport);
    }

    @ParameterizedTest
    @CsvSource({"succeeded, SUCCEEDED", "canceled, CANCELED"})
    @DisplayName("An older processing webhook cannot reopen a terminal payment")
    void keepsTerminalStatusAfterOlderWebhook(String stripeStatus, PaymentStatus expected) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_out_of_order_" + stripeStatus;
        var processing = paymentIntent(dto, intentId, "processing");
        HttpClient transport = stubStripe(200, processing);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(payment.getPaymentStatus()).isEqualTo(PaymentStatus.PROCESSING);
        });

        long created = Instant.now().getEpochSecond();
        String terminalEvent = "evt_terminal_" + stripeStatus;
        sendWebhook(terminalEvent, "payment_intent." + stripeStatus, paymentIntent(dto, intentId, stripeStatus), created);
        assertWebhookProcessed(terminalEvent);
        assertThat(paymentRepository.findByEventId(dto.eventId()).orElseThrow().getPaymentStatus()).isEqualTo(expected);

        String olderEvent = "evt_older_processing_" + stripeStatus;
        sendWebhook(olderEvent, "payment_intent.processing", processing, created - 60);
        assertWebhookProcessed(olderEvent);
        var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(saved.getPaymentStatus()).isEqualTo(expected);
        assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        assertSingleChargeAttempt(transport);
    }

    @Test
    @DisplayName("A success webhook links the payment before the worker receives an older Stripe response")
    void keepsWebhookSuccessAfterLateCreateResponse() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_early_webhook";
        var processing = stripeResponse(200, paymentIntent(dto, intentId, "processing"));
        var requested = new CountDownLatch(1);
        var releaseResponse = new CountDownLatch(1);
        HttpClient transport = stripeTransport();
        doAnswer(invocation -> {
            requested.countDown();
            assertThat(releaseResponse.await(25, TimeUnit.SECONDS)).isTrue();
            return processing;
        }).when(transport).request(any());

        try {
            sendPayment(dto);
            assertThat(requested.await(20, TimeUnit.SECONDS)).isTrue();
            var before = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(before.getProviderPaymentId()).isNull();
            assertThat(before.getPaymentStatus()).isEqualTo(PaymentStatus.UNKNOWN);

            sendWebhook("evt_early_success", "payment_intent.succeeded", paymentIntent(dto, intentId, "succeeded"));
            assertWebhookProcessed("evt_early_success");
            var linked = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(linked.getExecutionStatus()).isEqualTo(ExecutionStatus.IN_PROGRESS);
            assertThat(linked.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
            assertThat(linked.getProviderPaymentId()).isEqualTo(intentId);
        } finally {
            releaseResponse.countDown();
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.getAttempts()).isEqualTo(1);
        });
        assertSingleChargeAttempt(transport);
    }

    @Test
    @DisplayName("Replaying a Kafka payment with the same or a new event ID creates one PaymentIntent")
    void chargesOnceForRepeatedPaymentCommands() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_repeated_command";
        HttpClient transport = stubStripe(200, paymentIntent(dto, intentId, "succeeded"));
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });
        var original = paymentRepository.findByEventId(dto.eventId()).orElseThrow();

        var sameEvent = sendPayment(dto);
        UUID replayEventId = UUID.randomUUID();
        var newEvent = sendPayment(new PaymentCommand(replayEventId, dto.ownerUsername(),
            dto.paymentOperationId(), dto.orderId(), dto.amount(), dto.currency(), dto.paymentMethodId()));
        awaitPaymentOffsets(sameEvent, newEvent);
        assertThat(readPaymentDlt(dto.eventId(), replayEventId)).isEmpty();

        var conflict = new PaymentCommand(replayEventId, dto.ownerUsername(), UUID.randomUUID(),
            dto.orderId(), dto.amount(), dto.currency(), dto.paymentMethodId());
        awaitPaymentOffsets(sendPayment(conflict));
        assertThat(readPaymentDlt(replayEventId)).hasSize(1);
        assertThat(paymentRepository.findByPaymentOperationId(conflict.paymentOperationId())).isEmpty();

        assertThat(jdbc.queryForObject("SELECT count(*) FROM payments WHERE payment_operation_id = ?",
            Long.class, dto.paymentOperationId())).isEqualTo(1L);
        var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(saved).usingRecursiveComparison().isEqualTo(original);
        assertSingleChargeAttempt(transport);
    }

    @ParameterizedTest(name = "Reconciliation recovers {1} from {0} before the webhook arrives")
    @CsvSource({"PROCESSING, SUCCEEDED", "REQUIRES_ACTION, CANCELED"})
    void reconcilesKnownIntentBeforeDelayedWebhook(PaymentStatus pending, PaymentStatus outcome) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "webhook-reconciliation", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_reconciliation_" + dto.paymentOperationId();
        String stripeStatus = outcome.name().toLowerCase(Locale.ROOT);
        var initial = paymentIntent(dto, intentId, pending.name().toLowerCase(Locale.ROOT));
        var terminal = paymentIntent(dto, intentId, stripeStatus);
        HttpClient transport = stripeTransport();
        doReturn(stripeResponse(200, initial), stripeResponse(200, terminal)).when(transport).request(any());

        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(payment.getPaymentStatus()).isEqualTo(pending);
            assertThat(payment.getProviderPaymentId()).isEqualTo(intentId);
        });
        var original = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(jdbc.update("""
            UPDATE payments SET next_status_check_at = CURRENT_TIMESTAMP
            WHERE id = ? AND next_status_check_at IS NOT NULL
            """, original.getId())).isOne();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
            assertThat(paymentRepository.findById(original.getId()).orElseThrow().getPaymentStatus()).isEqualTo(outcome));
        String terminalTime = "SELECT first_terminal_at FROM payments WHERE id = ?";
        var reconciledAt = jdbc.queryForObject(terminalTime, Timestamp.class, original.getId());
        assertThat(reconciledAt).isNotNull();

        String eventId = "evt_delayed_" + dto.paymentOperationId();
        sendWebhook(eventId, "payment_intent." + stripeStatus, terminal);
        assertWebhookProcessed(eventId);
        var saved = paymentRepository.findById(original.getId()).orElseThrow();
        assertThat(saved.getPaymentStatus()).isEqualTo(outcome);
        assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        assertThat(saved.getAttempts()).isEqualTo(1);
        assertThat(jdbc.queryForObject(terminalTime, Timestamp.class, saved.getId())).isEqualTo(reconciledAt);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM payments WHERE payment_operation_id = ?",
            Long.class, dto.paymentOperationId())).isEqualTo(1L);

        var request = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(2)).request(request.capture());
        var requests = request.getAllValues();
        assertThat(requests).extracting(StripeRequest::method).containsExactly(RequestMethod.POST, RequestMethod.GET);
        assertThat(requests).extracting(call -> call.url().getPath())
            .containsExactly("/v1/payment_intents", "/v1/payment_intents/" + intentId);
        assertThat(requests.getFirst().params()).containsEntry("confirm", true);
    }

    @ParameterizedTest(name = "Confirmed rate limit: {0}")
    @CsvSource({"header", "lock_timeout"})
    @DisplayName("Confirmed rate limits retry the same create with exponential backoff and the original idempotency key")
    void retriesConfirmedRateLimitWithSameCreate(String confirmation) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "rate-limit-" + confirmation,
            UUID.randomUUID(), UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_rate_limit_" + dto.paymentOperationId();
        var intent = paymentIntent(dto, intentId, "processing");
        var error = Map.of("error", Map.of("type", "invalid_request_error", "code",
            confirmation.equals("lock_timeout") ? "lock_timeout" : "rate_limit", "message", "Rate limited"));
        var headers = confirmation.equals("header")
            ? HttpHeaders.of(Map.of("Stripe-Rate-Limited-Reason", List.of("global-rate")))
            : HttpHeaders.of(Map.of());
        var limited = new StripeResponse(429, headers, objectMapper.writeValueAsString(error));
        HttpClient transport = stripeTransport();
        doReturn(limited, limited, stripeResponse(200, intent)).when(transport).request(any());

        Instant attemptStarted = jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", Timestamp.class).toInstant();
        sendPayment(dto);
        for (int attempt = 1; attempt <= 2; attempt++) {
            int expectedAttempts = attempt;
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
                assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
                assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.REQUEST_REJECTED);
                assertThat(saved.getAttempts()).isEqualTo(expectedAttempts);
                assertThat(saved.getProviderPaymentId()).isNull();
                assertThat(saved.getNextRetryAt()).isNotNull();
                assertThat(saved.getFinishedAt()).isNull();
            });
            var rejected = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            Instant observedAt = jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", Timestamp.class).toInstant();
            int minimumDelay = attempt == 1 ? 30 : 60;
            int maximumDelay = attempt == 1 ? 45 : 90;
            assertThat(rejected.getNextRetryAt()).isAfterOrEqualTo(attemptStarted.plusSeconds(minimumDelay))
                .isBeforeOrEqualTo(observedAt.plusSeconds(maximumDelay));
            await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(paymentClaimService.claimNextPayment()).isEmpty();
                verify(transport, times(expectedAttempts)).request(any());
            });
            attemptStarted = jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", Timestamp.class).toInstant();
            assertThat(jdbc.update("UPDATE payments SET next_retry_at = CURRENT_TIMESTAMP WHERE event_id = ?",
                dto.eventId())).isOne();
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.PROCESSING);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.getAttempts()).isEqualTo(3);
            assertThat(saved.getNextRetryAt()).isNull();
        });
        var captor = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(3)).request(captor.capture());
        var requests = captor.getAllValues();
        assertThat(requests).allSatisfy(request -> {
            assertThat(request.method()).isEqualTo(RequestMethod.POST);
            assertThat(request.url().getPath()).isEqualTo("/v1/payment_intents");
            assertThat(request.headers().firstValue("Idempotency-Key")).contains(dto.paymentOperationId().toString());
            assertThat(request.params()).isEqualTo(requests.getFirst().params());
            assertThat(request.params()).containsEntry("confirm", true).containsEntry("metadata", intent.get("metadata"))
                .containsEntry("currency", dto.currency()).containsEntry("payment_method", dto.paymentMethodId());
            assertThat(objectMapper.valueToTree(request.params()).path("amount").longValue()).isEqualTo(dto.amount());
        });
    }

    @ParameterizedTest(name = "Stripe HTTP {0} requires an explicit manual retry")
    @CsvSource({"401", "403"})
    @DisplayName("Stripe authentication and permission errors fail immediately until configuration is fixed and the owner retries")
    void retriesAuthenticationFailureOnlyAfterManualRequest(int httpStatus) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "access-error-" + httpStatus, UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_auth_fixed_" + dto.paymentOperationId();
        var intent = paymentIntent(dto, intentId, "processing");
        HttpClient transport = stubStripe(httpStatus, Map.of("error", Map.of(
            "type", "invalid_request_error", "message", "Invalid API key or missing permission")));
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.REQUEST_REJECTED);
            assertThat(saved.getAttempts()).isEqualTo(1);
            assertThat(saved.getProviderPaymentId()).isNull();
            assertThat(saved.getNextRetryAt()).isNull();
            assertThat(saved.getFinishedAt()).isNotNull();
        });
        var failed = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        ReflectionTestUtils.setField(stripeGateway, "stripe", StripeClient.builder()
            .setApiKey("sk_test_corrected_payment_flow").setHttpClient(transport).build());
        doReturn(stripeResponse(200, intent)).when(transport).request(any());
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(paymentClaimService.claimNextPayment()).isEmpty();
            verify(transport).request(any());
            assertThat(paymentRepository.findById(failed.getId()).orElseThrow())
                .usingRecursiveComparison().isEqualTo(failed);
        });
        mockMvc.perform(post("/api/payments/{id}/retry", failed.getId())
                .with(httpBasic("admin", "admin-test-password"))
                .with(csrf()))
            .andExpect(status().isNoContent());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findById(failed.getId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.PROCESSING);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.getAttempts()).isEqualTo(1);
            assertThat(saved.getNextRetryAt()).isNull();
        });
        var captor = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(2)).request(captor.capture());
        var requests = captor.getAllValues();
        assertThat(requests).allSatisfy(request -> {
            assertThat(request.method()).isEqualTo(RequestMethod.POST);
            assertThat(request.url().getPath()).isEqualTo("/v1/payment_intents");
            assertThat(request.headers().firstValue("Idempotency-Key")).contains(dto.paymentOperationId().toString());
            assertThat(request.params()).containsEntry("confirm", true).isEqualTo(requests.getFirst().params());
        });
        assertThat(requests.getFirst().headers().firstValue("Authorization")).contains("Bearer sk_test_payment_flow");
        assertThat(requests.getLast().headers().firstValue("Authorization"))
            .contains("Bearer sk_test_corrected_payment_flow");
    }

    @Test
    @DisplayName("A timeout after Stripe accepts the charge recovers the same PaymentIntent without another charge")
    void recoversAcceptedPaymentAfterTimeout() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_accepted_before_timeout";
        var accepted = paymentIntent(dto, intentId, "succeeded");
        var search = Map.of("object", "search_result", "data", List.of(accepted), "has_more", false);
        HttpClient transport = stripeTransport();
        doThrow(new ApiConnectionException("Timed out reading the response", new SocketTimeoutException()))
            .doReturn(stripeResponse(200, search), stripeResponse(200, accepted), stripeResponse(200, accepted))
            .when(transport).request(any());

        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.UNKNOWN);
            assertThat(saved.getProviderPaymentId()).isNull();
            assertThat(saved.getNextRetryAt()).isNotNull();
        });

        jdbc.update("UPDATE payments SET next_retry_at = CURRENT_TIMESTAMP WHERE event_id = ?", dto.eventId());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });

        jdbc.update("UPDATE payments SET next_status_check_at = CURRENT_TIMESTAMP WHERE event_id = ?", dto.eventId());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });

        var captor = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(4)).request(captor.capture());
        var requests = captor.getAllValues();
        assertThat(requests).extracting(StripeRequest::method)
            .containsExactly(RequestMethod.POST, RequestMethod.GET, RequestMethod.GET, RequestMethod.GET);
        assertThat(requests).extracting(request -> request.url().getPath()).containsExactly(
            "/v1/payment_intents", "/v1/payment_intents/search", "/v1/payment_intents/" + intentId,
            "/v1/payment_intents/" + intentId);
        var create = requests.getFirst();
        assertThat(create.params()).containsEntry("metadata", accepted.get("metadata")).containsEntry("confirm", true);
        assertThat(create.headers().firstValue("Idempotency-Key")).contains(dto.paymentOperationId().toString());
        assertThat(requests.get(1).params()).containsEntry("query",
            "metadata['paymentOperationId']:'" + dto.paymentOperationId() + "'");
    }

    @ParameterizedTest(name = "Unknown payment with {0} search matches and a later {1} mismatch")
    @CsvSource({"0, amount", "2, order"})
    void recoversUnknownOnlyAfterVerifiedMatch(int initialMatches, String mismatch) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "unknown-recovery", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_verified_" + dto.paymentOperationId();
        String wrongId = "pi_mismatched_" + dto.paymentOperationId();
        var verified = paymentIntent(dto, intentId, "succeeded");
        var wrong = paymentIntent(dto, wrongId, "succeeded");
        if (mismatch.equals("amount")) {
            wrong.put("amount", dto.amount() + 1);
        } else {
            wrong.put("metadata", Map.of("paymentOperationId", dto.paymentOperationId().toString(),
                "orderId", UUID.randomUUID().toString()));
        }
        var candidates = initialMatches == 0 ? List.of() : List.of(verified, wrong);
        HttpClient transport = stripeTransport();
        doThrow(new ApiConnectionException("Read timed out", new SocketTimeoutException()))
            .doReturn(searchResponse(candidates), searchResponse(List.of(wrong)), stripeResponse(200, wrong),
                searchResponse(List.of(verified)), stripeResponse(200, verified))
            .when(transport).request(any());

        sendPayment(dto);
        for (int attempt = 1; attempt <= 2; attempt++) {
            int expectedAttempts = attempt;
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
                assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
                assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.UNKNOWN);
                assertThat(saved.getProviderPaymentId()).isNull();
                assertThat(saved.getAttempts()).isEqualTo(expectedAttempts);
                assertThat(saved.getNextRetryAt()).isNotNull();
            });
            jdbc.update("UPDATE payments SET next_retry_at = CURRENT_TIMESTAMP WHERE event_id = ?", dto.eventId());
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var failed = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(failed.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(failed.getAttempts()).isEqualTo(failed.getMaxAttempts());
            assertThat(failed.getPaymentStatus()).isEqualTo(PaymentStatus.UNKNOWN);
            assertThat(failed.getProviderPaymentId()).isNull();
        });

        jdbc.update("UPDATE payments SET finished_at = CURRENT_TIMESTAMP - INTERVAL '61 minutes' WHERE event_id = ?",
            dto.eventId());
        maintenanceScheduler.reconcileFailedUnknownPayment();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });

        var captor = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(6)).request(captor.capture());
        var calls = captor.getAllValues();
        assertThat(calls.getFirst().method()).isEqualTo(RequestMethod.POST);
        assertThat(calls.getFirst().params()).containsEntry("confirm", true);
        assertThat(calls.subList(1, calls.size())).extracting(StripeRequest::method).containsOnly(RequestMethod.GET);
        String intents = "/v1/payment_intents";
        assertThat(calls).extracting(call -> call.url().getPath()).containsExactly(intents, intents + "/search",
            intents + "/search", intents + "/" + wrongId, intents + "/search", intents + "/" + intentId);
    }

    @Test
    @DisplayName("A declined card waits for another payment method without retrying the charge")
    void declinedCardWaitsForAnotherPaymentMethod() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa_chargeDeclined");
        String intentId = "pi_declined";
        var intent = paymentIntent(dto, intentId, "requires_payment_method");
        var error = Map.of("type", "card_error", "code", "card_declined", "decline_code", "generic_decline",
            "message", "Your card was declined.", "payment_intent", intent);
        HttpClient transport = stubStripe(402, Map.of("error", error));

        sendPayment(dto);
        assertWaitingForCustomer(dto, intentId, PaymentStatus.REQUIRES_PAYMENT_METHOD);
        sendWebhook("evt_declined", "payment_intent.payment_failed", intent);
        assertWebhookProcessed("evt_declined");
        assertWaitingForCustomer(dto, intentId, PaymentStatus.REQUIRES_PAYMENT_METHOD);
        assertSingleChargeAttempt(transport);
    }

    @Test
    @DisplayName("A payment that needs authentication waits for the customer without retrying the charge")
    void requiresActionWaitsForAuthentication() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "alice", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_authenticationRequired");
        String intentId = "pi_requires_action";
        var intent = paymentIntent(dto, intentId, "requires_action");
        intent.put("next_action", Map.of("type", "redirect_to_url",
            "redirect_to_url", Map.of("url", "https://example.test/authenticate")));
        HttpClient transport = stubStripe(200, intent);

        sendPayment(dto);
        assertWaitingForCustomer(dto, intentId, PaymentStatus.REQUIRES_ACTION);
        sendWebhook("evt_requires_action", "payment_intent.requires_action", intent);
        assertWebhookProcessed("evt_requires_action");
        assertWaitingForCustomer(dto, intentId, PaymentStatus.REQUIRES_ACTION);
        assertSingleChargeAttempt(transport);
    }

    @Test
    @DisplayName("The owner replaces a declined card and confirms the existing PaymentIntent exactly once")
    void confirmsExistingIntentAfterOwnerChangesDeclinedCard() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "bob", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa_chargeDeclined");
        String intentId = "pi_changed_card_" + dto.paymentOperationId();
        var declined = paymentIntent(dto, intentId, "requires_payment_method");
        var succeeded = paymentIntent(dto, intentId, "succeeded");
        var error = Map.of("type", "card_error", "code", "card_declined", "decline_code", "generic_decline",
            "message", "Your card was declined.", "payment_intent", declined);
        HttpClient transport = stripeTransport();
        doReturn(stripeResponse(402, Map.of("error", error)), stripeResponse(200, succeeded))
            .when(transport).request(any());

        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.REQUIRES_PAYMENT_METHOD);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });
        var declinedPayment = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        var response = mockMvc.perform(get("/api/payments/{id}", declinedPayment.getId())
                .with(httpBasic("bob", "bob-test-password")))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.paymentStatus").value("REQUIRES_PAYMENT_METHOD"))
            .andExpect(jsonPath("$.providerPaymentId").value(intentId))
            .andReturn().getResponse();
        long version = objectMapper.readTree(response.getContentAsByteArray()).path("version").longValue();
        assertThat(version).isEqualTo(declinedPayment.getVersion());
        var csrfResult = mockMvc.perform(get("/api/csrf").with(httpBasic("bob", "bob-test-password")))
            .andExpect(status().isOk()).andReturn();
        var token = objectMapper.readTree(csrfResult.getResponse().getContentAsByteArray());
        var session = (MockHttpSession) csrfResult.getRequest().getSession(false);
        assertThat(session).isNotNull();
        assertThat(token.path("token").asText()).isNotBlank();
        String stale = objectMapper.writeValueAsString(Map.of("paymentMethodId", "pm_card_visa", "version", version - 1));
        mockMvc.perform(post("/api/payments/{id}/payment-method", declinedPayment.getId())
                .with(httpBasic("bob", "bob-test-password")).session(session)
                .header(token.path("headerName").asText(), token.path("token").asText())
                .contentType(MediaType.APPLICATION_JSON).content(stale))
            .andExpect(status().isConflict());
        String change = objectMapper.writeValueAsString(Map.of("paymentMethodId", "pm_card_visa", "version", version));

        mockMvc.perform(post("/api/payments/{id}/payment-method", declinedPayment.getId())
                .with(httpBasic("bob", "bob-test-password")).session(session)
                .header(token.path("headerName").asText(), token.path("token").asText())
                .contentType(MediaType.APPLICATION_JSON).content(change))
            .andExpect(status().isAccepted());
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.currentPaymentMethodId()).isEqualTo("pm_card_visa");
            assertThat(saved.getAttempts()).isEqualTo(1);
            assertThat(saved.getNextRetryAt()).isNull();
        });
        var confirmed = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        mockMvc.perform(post("/api/payments/{id}/payment-method", declinedPayment.getId())
                .with(httpBasic("bob", "bob-test-password")).session(session)
                .header(token.path("headerName").asText(), token.path("token").asText())
                .contentType(MediaType.APPLICATION_JSON).content(change))
            .andExpect(status().isConflict());
        assertThat(paymentRepository.findByEventId(dto.eventId()).orElseThrow())
            .usingRecursiveComparison().isEqualTo(confirmed);

        String eventId = "evt_changed_card_" + dto.paymentOperationId();
        sendWebhook(eventId, "payment_intent.succeeded", succeeded);
        assertWebhookProcessed(eventId);
        mockMvc.perform(get("/api/payments/{id}", confirmed.getId()).with(httpBasic("bob", "bob-test-password")))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.executionStatus").value("COMPLETED"))
            .andExpect(jsonPath("$.paymentStatus").value("SUCCEEDED"))
            .andExpect(jsonPath("$.providerPaymentId").value(intentId));
        for (int poll = 0; poll < 3; poll++) {
            assertThat(paymentClaimService.claimNextPayment()).isEmpty();
        }
        var captor = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(2)).request(captor.capture());
        var calls = captor.getAllValues();
        assertThat(calls).extracting(StripeRequest::method).containsExactly(RequestMethod.POST, RequestMethod.POST);
        assertThat(calls).extracting(call -> call.url().getPath()).containsExactly(
            "/v1/payment_intents", "/v1/payment_intents/" + intentId + "/confirm");
        assertThat(calls.getFirst().params()).containsEntry("payment_method", dto.paymentMethodId())
            .containsEntry("confirm", true);
        assertThat(calls.get(1).params()).containsEntry("payment_method", "pm_card_visa");
        assertThat(calls.get(1).headers().firstValue("Idempotency-Key"))
            .contains("confirm-" + dto.paymentOperationId() + "-" + version);
    }

    @ParameterizedTest(name = "Expired authentication finishes as {1} after cancellation race={0}")
    @CsvSource({"false, CANCELED", "true, SUCCEEDED"})
    @DisplayName("Expired 3DS authentication cancels the same intent or accepts success that wins the race")
    void reconcilesExpiredAuthentication(boolean cancellationRace, PaymentStatus expected) throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "expired-auth-" + cancellationRace,
            UUID.randomUUID(), UUID.randomUUID(), 1000L, "usd", "pm_card_authenticationRequired");
        String intentId = "pi_expired_auth_" + dto.paymentOperationId();
        var requiresAction = paymentIntent(dto, intentId, "requires_action");
        requiresAction.put("next_action", Map.of("type", "redirect_to_url",
            "redirect_to_url", Map.of("url", "https://example.test/authenticate")));
        var terminal = paymentIntent(dto, intentId, expected.name().toLowerCase(Locale.ROOT));
        HttpClient transport = stripeTransport();
        var waitingResponse = stripeResponse(200, requiresAction);
        if (cancellationRace) {
            var error = Map.of("type", "invalid_request_error", "code", "payment_intent_unexpected_state",
                "message", "The PaymentIntent has already succeeded.");
            doReturn(waitingResponse, waitingResponse, stripeResponse(400, Map.of("error", error)),
                stripeResponse(200, terminal)).when(transport).request(any());
        } else {
            doReturn(waitingResponse, waitingResponse, stripeResponse(200, terminal)).when(transport).request(any());
        }

        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.REQUIRES_ACTION);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.getActionExpiresAt()).isAfter(Instant.now());
        });
        assertThat(jdbc.update("""
            UPDATE payments SET action_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute',
                next_status_check_at = CURRENT_TIMESTAMP - INTERVAL '1 minute' WHERE event_id = ?
            """, dto.eventId())).isOne();
        maintenanceScheduler.reconcileOrCancelPendingPayment();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(expected);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(saved.getAttempts()).isEqualTo(1);
            assertThat(saved.getNextRetryAt()).isNull();
        });
        var captor = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport, times(cancellationRace ? 4 : 3)).request(captor.capture());
        var calls = captor.getAllValues();
        var expectedPaths = new ArrayList<>(List.of("/v1/payment_intents", "/v1/payment_intents/" + intentId,
            "/v1/payment_intents/" + intentId + "/cancel"));
        var expectedMethods = new ArrayList<>(List.of(RequestMethod.POST, RequestMethod.GET, RequestMethod.POST));
        if (cancellationRace) {
            expectedPaths.add("/v1/payment_intents/" + intentId);
            expectedMethods.add(RequestMethod.GET);
        }
        assertThat(calls).extracting(call -> call.url().getPath()).containsExactlyElementsOf(expectedPaths);
        assertThat(calls).extracting(StripeRequest::method).containsExactlyElementsOf(expectedMethods);
        assertThat(calls.get(2).params()).containsEntry("cancellation_reason", "abandoned");
        assertThat(calls.get(2).headers().firstValue("Idempotency-Key")).contains("abandoned-" + intentId);
    }

    @Test
    @DisplayName("An archived successful payment rejects changed Kafka replays and consumes delayed webhooks without another charge")
    void preservesArchivedPaymentAcrossReplayAndDelayedWebhook() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "archived-replay", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa");
        String intentId = "pi_archived_" + dto.paymentOperationId();
        var succeeded = paymentIntent(dto, intentId, "succeeded");
        HttpClient transport = stubStripe(200, succeeded);
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.AWAITING_WEBHOOK);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });
        String successEvent = "evt_archive_success_" + dto.paymentOperationId();
        String successPayload = webhookPayload(successEvent, "payment_intent.succeeded", succeeded,
            Instant.now().minus(Duration.ofDays(36)).getEpochSecond(), false);
        postSignedWebhook(successPayload).andExpect(status().isOk());
        assertWebhookProcessed(successEvent);
        var original = paymentRepository.findByEventId(dto.eventId()).orElseThrow();
        assertThat(original.getPaymentStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
        assertThat(original.getAttempts()).isEqualTo(1);

        assertThat(jdbc.update("""
            UPDATE payments SET finished_at = CURRENT_TIMESTAMP - INTERVAL '31 days',
                first_terminal_at = CURRENT_TIMESTAMP - INTERVAL '31 days'
            WHERE id = ?
            """, original.getId())).isOne();
        paymentCleanupService.archiveExpiredPayments();
        assertThat(paymentRepository.findById(original.getId())).isEmpty();
        String archiveSql = "SELECT * FROM archived_payments WHERE payment_operation_id = ?";
        var archived = jdbc.queryForMap(archiveSql, dto.paymentOperationId());
        assertThat(archived).containsEntry("payment_id", original.getId())
            .containsEntry("event_id", dto.eventId())
            .containsEntry("amount", dto.amount())
            .containsEntry("provider_payment_id", intentId);

        assertThat(jdbc.update("""
            UPDATE stripe_webhook_events SET received_at = CURRENT_TIMESTAMP - INTERVAL '36 days',
                processed_at = CURRENT_TIMESTAMP - INTERVAL '36 days' WHERE event_id = ?
            """, successEvent)).isOne();
        String recentEvent = "evt_archive_recent_" + dto.paymentOperationId();
        String pendingEvent = "evt_archive_pending_" + dto.paymentOperationId();
        String reviewEvent = "evt_archive_review_" + dto.paymentOperationId();
        String pendingPayload = webhookPayload(pendingEvent, "payment_intent.succeeded", succeeded,
            Instant.now().minus(Duration.ofDays(36)).getEpochSecond(), false);
        var mismatched = paymentIntent(dto, intentId, "succeeded");
        mismatched.put("amount", dto.amount() + 1);
        String reviewPayload = webhookPayload(reviewEvent, "payment_intent.succeeded", mismatched,
            Instant.now().minus(Duration.ofDays(36)).getEpochSecond(), false);
        jdbc.update("""
            INSERT INTO stripe_webhook_events
                (event_id, event_type, payload, received_at, processed_at, available_at, review_reason)
            VALUES (?, 'payment_intent.succeeded', NULL, CURRENT_TIMESTAMP - INTERVAL '36 days',
                    CURRENT_TIMESTAMP - INTERVAL '34 days', NULL, NULL),
                   (?, 'payment_intent.succeeded', ?, CURRENT_TIMESTAMP - INTERVAL '36 days',
                    NULL, CURRENT_TIMESTAMP + INTERVAL '1 day', NULL),
                   (?, 'payment_intent.succeeded', ?, CURRENT_TIMESTAMP - INTERVAL '36 days',
                    NULL, NULL, 'AMOUNT_MISMATCH')
            """, recentEvent, pendingEvent, pendingPayload, reviewEvent, reviewPayload);
        String retainedSql = "SELECT * FROM stripe_webhook_events WHERE event_id IN (?, ?, ?) ORDER BY event_id";
        var retainedEvents = jdbc.queryForList(retainedSql, recentEvent, pendingEvent, reviewEvent);

        paymentCleanupService.deleteExpiredWebhookEvents();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM stripe_webhook_events WHERE event_id = ?",
            Long.class, successEvent)).isZero();
        assertThat(jdbc.queryForList(retainedSql, recentEvent, pendingEvent, reviewEvent)).isEqualTo(retainedEvents);

        postSignedWebhook(successPayload).andExpect(status().isOk());
        assertWebhookProcessed(successEvent);
        assertThat(jdbc.queryForMap("""
            SELECT review_reason, payload, available_at, processed_at > CURRENT_TIMESTAMP - INTERVAL '1 day' AS recent
            FROM stripe_webhook_events WHERE event_id = ?
            """, successEvent)).containsEntry("review_reason", null).containsEntry("payload", null)
            .containsEntry("available_at", null).containsEntry("recent", true);
        assertThat(paymentRepository.findByPaymentOperationId(dto.paymentOperationId())).isEmpty();
        assertThat(jdbc.queryForMap(archiveSql, dto.paymentOperationId())).isEqualTo(archived);
        assertSingleChargeAttempt(transport);

        var replay = new PaymentCommand(UUID.randomUUID(), dto.ownerUsername(), dto.paymentOperationId(),
            dto.orderId(), dto.amount(), dto.currency(), dto.paymentMethodId());
        var replayRecord = sendPayment(replay);
        awaitPaymentOffsets(replayRecord);
        assertThat(paymentRepository.findByPaymentOperationId(dto.paymentOperationId())).isEmpty();
        assertThat(jdbc.queryForMap(archiveSql, dto.paymentOperationId())).isEqualTo(archived);
        assertSingleChargeAttempt(transport);

        var changed = new PaymentCommand(UUID.randomUUID(), dto.ownerUsername(), dto.paymentOperationId(),
            dto.orderId(), dto.amount() + 1, dto.currency(), dto.paymentMethodId());
        var rejected = sendPayment(changed);
        awaitPaymentOffsets(rejected);
        var dlt = readPaymentDlt(replay.eventId(), changed.eventId());
        assertThat(dlt).hasSize(1);
        assertThat(dlt.getFirst().key()).isEqualTo(changed.eventId().toString());
        assertThat(objectMapper.readTree(dlt.getFirst().value()))
            .isEqualTo(objectMapper.readTree(objectMapper.writeValueAsBytes(changed)));
        assertThat(new String(dlt.getFirst().headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE).value(),
            StandardCharsets.UTF_8)).contains("eventId=" + changed.eventId(), "already registered with different values");
        assertThat(paymentRepository.findByPaymentOperationId(dto.paymentOperationId())).isEmpty();
        assertThat(jdbc.queryForMap(archiveSql, dto.paymentOperationId())).isEqualTo(archived);

        String delayedEvent = "evt_archive_delayed_" + dto.paymentOperationId();
        sendWebhook(delayedEvent, "payment_intent.processing", paymentIntent(dto, intentId, "processing"),
            Instant.now().minusSeconds(60).getEpochSecond());
        assertWebhookProcessed(delayedEvent);
        assertThat(jdbc.queryForMap("""
            SELECT review_reason, payload, available_at FROM stripe_webhook_events WHERE event_id = ?
            """, delayedEvent)).containsEntry("review_reason", null).containsEntry("payload", null)
            .containsEntry("available_at", null);
        assertThat(paymentRepository.findByPaymentOperationId(dto.paymentOperationId())).isEmpty();
        assertThat(jdbc.queryForMap(archiveSql, dto.paymentOperationId())).isEqualTo(archived);
        assertSingleChargeAttempt(transport);
    }

    @Test
    @DisplayName("A declined payment removed by cleanup ignores Kafka replays and keeps an unexpected success for review")
    void keepsDeclinedPaymentRegisteredAfterCleanup() throws Exception {
        PaymentCommand dto = new PaymentCommand(UUID.randomUUID(), "declined-replay", UUID.randomUUID(),
            UUID.randomUUID(), 1000L, "usd", "pm_card_visa_chargeDeclined");
        String intentId = "pi_archived_declined_" + dto.paymentOperationId();
        var declined = paymentIntent(dto, intentId, "requires_payment_method");
        var error = Map.of("type", "card_error", "code", "card_declined", "decline_code", "generic_decline",
            "message", "Your card was declined.", "payment_intent", declined);
        HttpClient transport = stubStripe(402, Map.of("error", error));
        sendPayment(dto);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var saved = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(saved.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(saved.getPaymentStatus()).isEqualTo(PaymentStatus.REQUIRES_PAYMENT_METHOD);
            assertThat(saved.getProviderPaymentId()).isEqualTo(intentId);
        });
        var original = paymentRepository.findByEventId(dto.eventId()).orElseThrow();

        assertThat(jdbc.update("UPDATE payments SET finished_at = CURRENT_TIMESTAMP - INTERVAL '31 days' WHERE id = ?",
            original.getId())).isOne();
        paymentCleanupService.archiveExpiredPayments();
        assertThat(paymentRepository.findById(original.getId())).isEmpty();

        var replay = new PaymentCommand(UUID.randomUUID(), dto.ownerUsername(), dto.paymentOperationId(),
            dto.orderId(), dto.amount(), dto.currency(), dto.paymentMethodId());
        awaitPaymentOffsets(sendPayment(dto), sendPayment(replay));
        assertThat(paymentRepository.findByPaymentOperationId(dto.paymentOperationId())).isEmpty();
        assertSingleChargeAttempt(transport);

        String archiveSql = "SELECT * FROM archived_payments WHERE payment_operation_id = ?";
        var archived = jdbc.queryForMap(archiveSql, dto.paymentOperationId());
        assertThat(archived).containsEntry("payment_id", original.getId())
            .containsEntry("event_id", dto.eventId())
            .containsEntry("provider_payment_id", intentId)
            .containsEntry("payment_status", "REQUIRES_PAYMENT_METHOD")
            .containsEntry("paid_at", null);

        var changed = new PaymentCommand(UUID.randomUUID(), dto.ownerUsername(), dto.paymentOperationId(),
            dto.orderId(), dto.amount() + 1, dto.currency(), dto.paymentMethodId());
        awaitPaymentOffsets(sendPayment(changed));
        assertThat(readPaymentDlt(changed.eventId())).hasSize(1);
        assertThat(paymentRepository.findByPaymentOperationId(dto.paymentOperationId())).isEmpty();

        String unexpectedSuccess = "evt_archived_declined_1_" + dto.paymentOperationId();
        String delayedFailure = "evt_archived_declined_2_" + dto.paymentOperationId();
        sendWebhook(unexpectedSuccess, "payment_intent.succeeded", paymentIntent(dto, intentId, "succeeded"));
        sendWebhook(delayedFailure, "payment_intent.payment_failed", declined);
        assertWebhookProcessed(delayedFailure);
        assertThat(jdbc.queryForMap("""
            SELECT processed_at, review_reason, payload IS NOT NULL AS payload_kept
            FROM stripe_webhook_events WHERE event_id = ?
            """, unexpectedSuccess)).containsEntry("processed_at", null).containsEntry("review_reason", null)
            .containsEntry("payload_kept", true);
        assertThat(jdbc.queryForMap(archiveSql, dto.paymentOperationId())).isEqualTo(archived);
        assertSingleChargeAttempt(transport);
    }

    private void awaitPaymentOffsets(RecordMetadata... records) throws Exception {
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var offsets = admin.listConsumerGroupOffsets("payment-group")
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                for (var record : records) {
                    var committed = offsets.get(new TopicPartition(record.topic(), record.partition()));
                    assertThat(committed).isNotNull();
                    assertThat(committed.offset()).isGreaterThan(record.offset());
                }
            });
        }
    }

    private List<ConsumerRecord<String, byte[]>> readPaymentDlt(UUID... eventIds) throws Exception {
        try (var admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            if (!admin.listTopics().names().get(5, TimeUnit.SECONDS).contains("payments.DLT")) {
                return List.of();
            }
            var partitions = admin.describeTopics(List.of("payments.DLT")).allTopicNames()
                .get(5, TimeUnit.SECONDS).get("payments.DLT").partitions().stream()
                .map(partition -> new TopicPartition("payments.DLT", partition.partition())).toList();
            var keys = List.of(eventIds).stream().map(UUID::toString).toList();
            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            try (var consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000,
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000
            ), new StringDeserializer(), new ByteArrayDeserializer())) {
                consumer.assign(partitions);
                var end = consumer.endOffsets(partitions);
                consumer.seekToBeginning(partitions);
                await().pollInSameThread().atMost(Duration.ofSeconds(30)).until(() -> {
                    consumer.poll(Duration.ofMillis(200)).forEach(record -> {
                        if (keys.contains(record.key())) {
                            records.add(record);
                        }
                    });
                    return partitions.stream().allMatch(partition -> consumer.position(partition) >= end.get(partition));
                });
            }
            return records;
        }
    }

    private void assertWaitingForCustomer(PaymentCommand dto, String intentId, PaymentStatus expected) throws Exception {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var payment = paymentRepository.findByEventId(dto.eventId()).orElseThrow(AssertionError::new);
            assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.COMPLETED);
            assertThat(payment.getPaymentStatus()).isEqualTo(expected);
            assertThat(payment.getProviderPaymentId()).isEqualTo(intentId);
            assertThat(payment.getAttempts()).isEqualTo(1);
            assertThat(payment.getNextRetryAt()).isNull();
        });
        mockMvc.perform(get("/api/payments/by-event-id/{eventId}", dto.eventId())
                .with(httpBasic("alice", "alice-test-password")))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.paymentStatus").value(expected.name()));
    }

    private void assertSingleChargeAttempt(HttpClient transport) throws Exception {
        for (int poll = 0; poll < 3; poll++) {
            assertThat(paymentClaimService.claimNextPayment()).isEmpty();
        }
        var request = ArgumentCaptor.forClass(StripeRequest.class);
        verify(transport).request(request.capture());
        assertThat(request.getValue().method()).isEqualTo(RequestMethod.POST);
        assertThat(request.getValue().url().getPath()).isEqualTo("/v1/payment_intents");
        assertThat(request.getValue().params()).containsEntry("confirm", true);
    }

    private Map<String, Object> paymentIntent(PaymentCommand dto, String id, String status) {
        return new HashMap<>(Map.of("object", "payment_intent", "id", id, "status", status,
            "amount", dto.amount(), "currency", dto.currency(), "livemode", false, "metadata",
            Map.of("paymentOperationId", dto.paymentOperationId().toString(), "orderId", dto.orderId().toString())));
    }

    private HttpClient stubStripe(int code, Object body) throws Exception {
        HttpClient transport = stripeTransport();
        doReturn(stripeResponse(code, body)).when(transport).request(any());
        return transport;
    }

    private StripeResponse searchResponse(List<?> matches) throws Exception {
        return stripeResponse(200, Map.of("object", "search_result", "data", matches, "has_more", false));
    }

    private StripeResponse stripeResponse(int code, Object body) throws Exception {
        return new StripeResponse(code, HttpHeaders.of(Map.of()), objectMapper.writeValueAsString(body));
    }

    private HttpClient stripeTransport() {
        HttpClient transport = mock(HttpClient.class, CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(stripeGateway, "stripe",
            StripeClient.builder().setApiKey("sk_test_payment_flow").setHttpClient(transport).build());
        return transport;
    }

    private RecordMetadata sendPayment(PaymentCommand dto) throws Exception {
        try (var producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
            new StringSerializer(), new ByteArraySerializer())) {
            return producer.send(new ProducerRecord<>("payments", dto.eventId().toString(), objectMapper.writeValueAsBytes(dto)))
                .get(20, TimeUnit.SECONDS);
        }
    }

    private void sendWebhook(String eventId, String eventType, Map<String, ?> intent) throws Exception {
        sendWebhook(eventId, eventType, intent, Instant.now().getEpochSecond());
    }

    private void sendWebhook(String eventId, String eventType, Map<String, ?> intent, long created) throws Exception {
        String payload = webhookPayload(eventId, eventType, intent, created, false);
        postSignedWebhook(payload).andExpect(status().isOk());
    }

    private ResultActions postSignedWebhook(String payload) throws Exception {
        return mockMvc.perform(post("/api/webhooks/stripe").contentType(MediaType.APPLICATION_JSON)
            .header("Stripe-Signature", Webhook.Signature.generateSignatureHeader(payload, "whsec_payment_flow"))
            .content(payload));
    }

    private String webhookPayload(String eventId, String eventType, Map<String, ?> intent, long created,
                                  boolean liveMode) throws Exception {
        return objectMapper.writeValueAsString(Map.of("id", eventId, "object", "event", "created", created,
            "type", eventType, "api_version", Stripe.API_VERSION, "livemode", liveMode, "data", Map.of("object", intent)));
    }

    private void assertWebhookProcessed(String eventId) {
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(jdbc.queryForObject("SELECT processed_at IS NOT NULL FROM stripe_webhook_events WHERE event_id = ?",
                Boolean.class, eventId)).isTrue());
    }
}
