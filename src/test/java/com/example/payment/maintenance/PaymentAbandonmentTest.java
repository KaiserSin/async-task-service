package com.example.payment.maintenance;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.api.dto.ChangePaymentMethodRequest;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.api.PaymentActionService;
import com.example.payment.stripe.StripeGateway;
import com.example.payment.api.security.PaymentAccessService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DataJpaTest(properties = {"spring.jpa.hibernate.ddl-auto=validate"})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({PaymentRecoveryService.class, PaymentActionService.class, PaymentAccessService.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentAbandonmentTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired private PaymentRepository payments;
    @Autowired private PaymentRecoveryService recovery;
    @Autowired private PaymentActionService retries;
    @Autowired private JdbcTemplate jdbc;
    @Autowired private PlatformTransactionManager transactionManager;
    @MockitoBean private StripeGateway stripe;
    @MockitoBean private WorkerProperties workerProperties;

    @BeforeEach
    void setUp() {
        payments.deleteAllInBatch();
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("alice", "unused", List.of()));
    }

    @AfterEach
    void clearAuthentication() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void staleExpirySnapshotCannotCancelAReplacementConfirmation() {
        Payment payment = expiredAction();
        when(stripe.retrieveStatus(payment.getPaymentOperationId(), "pi_expiry")).thenAnswer(call -> {
            webhookStatus(payment, "REQUIRES_PAYMENT_METHOD");
            Payment current = find(payment);
            assertThat(retries.changePaymentMethod(payment.getId(), "alice",
                new ChangePaymentMethodRequest("pm_replacement", current.getVersion()))).isTrue();
            var tx = new TransactionTemplate(transactionManager);
            UUID executionId = UUID.randomUUID();
            var claimed = tx.execute(status -> payments.claimNextExecution(executionId, 30).orElseThrow());
            assertThat(tx.execute(status -> payments.markStripeSubmission(payment.getId(), executionId,
                false, claimed.getConfirmationKey())).intValue()).isOne();
            webhookStatus(payment, "REQUIRES_ACTION");
            return PaymentStatus.REQUIRES_ACTION;
        });

        recovery.reconcileOrCancelPendingPayment();

        verify(stripe, never()).cancelAbandoned(any(), any());
        Payment current = find(payment);
        assertThat(current.isAbandonmentStarted()).isFalse();
        assertThat(current.getPaymentStatus()).isEqualTo(PaymentStatus.REQUIRES_ACTION);
        assertThat(current.getActionExpiresAt()).isAfter(Instant.now());
        assertThat(current.getExecutionStatus().name()).isEqualTo("IN_PROGRESS");
    }

    @Test
    void durableFenceBlocksReplacementAndManualRetryAfterCrashBeforeCancel() {
        Payment payment = expiredAction();
        var reserved = payments.reservePendingIntent(15).orElseThrow();
        assertThat(payments.beginAbandonment(reserved.intentId(), reserved.version())).isOne();
        webhookStatus(payment, "REQUIRES_PAYMENT_METHOD");
        jdbc.update("UPDATE payments SET execution_status = 'FAILED' WHERE id = ?", payment.getId());
        Payment current = find(payment);
        assertThat(retries.changePaymentMethod(payment.getId(), "alice",
            new ChangePaymentMethodRequest("pm_replacement", current.getVersion()))).isFalse();
        assertThat(retries.retryFailedPayment(payment.getId())).isFalse();
        assertThat(current.isAbandonmentStarted()).isTrue();
        assertThat(current.getExecutionStatus().name()).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT next_status_check_at IS NOT NULL FROM payments WHERE id = ?",
            Boolean.class, payment.getId())).isTrue();

        makeDue(payment);
        when(stripe.retrieveStatus(payment.getPaymentOperationId(), "pi_expiry"))
            .thenReturn(PaymentStatus.REQUIRES_PAYMENT_METHOD);
        when(stripe.cancelAbandoned(payment.getPaymentOperationId(), "pi_expiry"))
            .thenReturn(PaymentStatus.CANCELED);
        new PaymentRecoveryService(payments, workerProperties, stripe).reconcileOrCancelPendingPayment();

        assertThat(find(payment).getPaymentStatus()).isEqualTo(PaymentStatus.CANCELED);
        assertThat(payments.reservePendingIntent(15)).isEmpty();
    }

    @Test
    void ambiguousCancellationRemainsFencedAndResumesOnTheSameIntent() {
        Payment payment = expiredAction();
        when(stripe.retrieveStatus(payment.getPaymentOperationId(), "pi_expiry"))
            .thenReturn(PaymentStatus.REQUIRES_ACTION, PaymentStatus.REQUIRES_PAYMENT_METHOD);
        when(stripe.cancelAbandoned(payment.getPaymentOperationId(), "pi_expiry"))
            .thenThrow(new IllegalStateException("Cancellation response lost"))
            .thenReturn(PaymentStatus.CANCELED);

        assertThatThrownBy(recovery::reconcileOrCancelPendingPayment).hasMessage("Cancellation response lost");
        assertThat(find(payment).isAbandonmentStarted()).isTrue();
        webhookStatus(payment, "REQUIRES_PAYMENT_METHOD");
        makeDue(payment);
        recovery.reconcileOrCancelPendingPayment();

        assertThat(find(payment).getPaymentStatus()).isEqualTo(PaymentStatus.CANCELED);
    }

    @Test
    void staleReservationCannotFenceANewActionDeadline() {
        Payment payment = expiredAction();
        var reserved = payments.reservePendingIntent(15).orElseThrow();
        jdbc.update("UPDATE payments SET action_expires_at = CURRENT_TIMESTAMP + INTERVAL '15 minutes' WHERE id = ?",
            payment.getId());

        assertThat(payments.beginAbandonment(reserved.intentId(), reserved.version())).isZero();
        assertThat(find(payment).isAbandonmentStarted()).isFalse();
    }

    private Payment expiredAction() {
        Payment payment = payments.saveAndFlush(payment(UUID.randomUUID(), "alice", "pm_original", 2500L));
        jdbc.update("""
            UPDATE payments SET execution_status = 'COMPLETED', finished_at = CURRENT_TIMESTAMP,
                provider_payment_id = 'pi_expiry', payment_status = 'REQUIRES_ACTION',
                action_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute' WHERE id = ?
            """, payment.getId());
        return payment;
    }

    private Payment find(Payment payment) {
        return payments.findById(payment.getId()).orElseThrow();
    }

    private void webhookStatus(Payment payment, String status) {
        jdbc.update("UPDATE payments SET payment_status = ?, version = version + 1 WHERE id = ?",
            status, payment.getId());
    }

    private void makeDue(Payment payment) {
        jdbc.update("UPDATE payments SET next_status_check_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?",
            payment.getId());
    }
}
