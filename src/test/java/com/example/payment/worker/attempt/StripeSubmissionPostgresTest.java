package com.example.payment.worker.attempt;

import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;
import com.example.payment.worker.config.WorkerTransactionConfig;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest(properties = {"spring.jpa.hibernate.ddl-auto=validate"})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({WorkerTransactionConfig.class, PaymentClaimService.class, PaymentUpdateService.class})
@Testcontainers
class StripeSubmissionPostgresTest {
    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired PaymentRepository repository;
    @Autowired PaymentClaimService claims;
    @Autowired PaymentUpdateService updates;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    @BeforeEach
    void setUp() {
        repository.deleteAllInBatch();
        repository.saveAndFlush(payment(UUID.randomUUID(), "alice", "pm_test", 1000));
    }

    @Test
    void claimCrashBeforeSubmissionStillAllowsCreatingIntent() {
        PaymentAttempt crashed = claim();
        assertThat(crashed.createIntent()).isTrue();
        assertThat(repository.findById(crashed.id()).orElseThrow().getPaymentStatus()).isEqualTo(PaymentStatus.NOT_SUBMITTED);
        recover(crashed);
        PaymentAttempt replacement = claim();
        assertThat(replacement.createIntent()).isTrue();
        assertThatThrownBy(() -> updates.markStripeSubmission(crashed)).isInstanceOf(IllegalStateException.class);
        updates.markStripeSubmission(replacement);
        assertThat(repository.findById(crashed.id()).orElseThrow().getPaymentStatus()).isEqualTo(PaymentStatus.UNKNOWN);
    }

    @Test
    void expiredLeaseCannotCrossSubmissionBoundary() {
        PaymentAttempt attempt = claim();
        expire(attempt);
        assertThatThrownBy(() -> updates.markStripeSubmission(attempt)).isInstanceOf(IllegalStateException.class);
        assertThat(repository.findById(attempt.id()).orElseThrow().getPaymentStatus()).isEqualTo(PaymentStatus.NOT_SUBMITTED);
    }

    @Test
    void uncertainSubmissionIsReconciledWithoutCreatingAgain() {
        PaymentAttempt attempt = claim();
        updates.markStripeSubmission(attempt);
        assertThatThrownBy(() -> updates.markStripeSubmission(attempt)).isInstanceOf(IllegalStateException.class);
        recover(attempt);
        assertThat(claim().createIntent()).isFalse();
    }

    @Test
    void confirmationCrashBeforeSubmissionRetainsPendingConfirmation() {
        jdbc.update("""
            UPDATE payments SET provider_payment_id = 'pi_test', payment_status = 'REQUIRES_PAYMENT_METHOD',
                                pending_confirmation_key = 'confirm-key'
            """);
        PaymentAttempt attempt = claim();
        assertThat(attempt.confirmationKey()).isEqualTo("confirm-key");
        recover(attempt);
        PaymentAttempt replacement = claim();
        assertThat(replacement.confirmationKey()).isEqualTo("confirm-key");
        updates.markStripeSubmission(replacement);
        assertThat(jdbc.queryForObject("SELECT pending_confirmation_key FROM payments WHERE id = ?", String.class, attempt.id())).isNull();
        recover(replacement);
        PaymentAttempt uncertain = claim();
        assertThat(uncertain.confirmationKey()).isNull();
        assertThat(uncertain.createIntent()).isFalse();
    }

    @Test
    void confirmedValidationFailureIsTerminalAndNotEligibleForUnknownSearch() {
        PaymentAttempt attempt = claim();
        updates.markStripeSubmission(attempt);
        assertThat(updates.failRejectedRequest(attempt, new StripeRequestRejectedException(400, "req_test")))
            .contains(ExecutionStatus.FAILED);
        var failed = repository.findById(attempt.id()).orElseThrow();
        assertThat(failed.getPaymentStatus()).isEqualTo(PaymentStatus.REQUEST_REJECTED);
        jdbc.update("UPDATE payments SET finished_at = CURRENT_TIMESTAMP - INTERVAL '2 hours'");
        assertThat(repository.reserveFailedUnknownPayment()).isEmpty();
        assertThat(claims.claimNextPayment()).isEmpty();
    }

    private PaymentAttempt claim() {
        return claims.claimNextPayment().orElseThrow();
    }

    private void expire(PaymentAttempt attempt) {
        jdbc.update("UPDATE payments SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", attempt.id());
    }

    private void recover(PaymentAttempt attempt) {
        expire(attempt);
        var recovered = new TransactionTemplate(transactionManager).execute(ignored -> repository.recoverExpiredLeases(1));
        assertThat(recovered.requeued()).isEqualTo(1);
        jdbc.update("UPDATE payments SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?", attempt.id());
    }
}
