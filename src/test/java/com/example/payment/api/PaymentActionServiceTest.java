package com.example.payment.api;

import com.example.payment.domain.Payment;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import com.example.payment.api.error.PaymentNotFoundException;
import com.example.payment.api.security.PaymentAccessService;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.ReflectionTestUtils.setField;

@ExtendWith(MockitoExtension.class)
class PaymentActionServiceTest {

    private static final Long PAYMENT_ID = 7L;
    private static final UUID EVENT_ID = UUID.fromString("ac06c557-1707-421b-acfe-28a7ab1d74f6");
    private static final UUID EXECUTION_ID = UUID.fromString("cc258051-0eba-4ede-9311-eb59eb55609a");
    private static final Instant CREATED_AT = Instant.parse("2026-10-24T10:00:00Z");
    private static final Instant STARTED_AT = Instant.parse("2026-10-24T10:01:00Z");
    private static final Instant FINISHED_AT = Instant.parse("2026-10-24T10:02:00Z");

    @Mock
    private PaymentRepository paymentRepository;

    private PaymentActionService paymentActionService;

    @BeforeEach
    void setUp() {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("user1", "unused",
                AuthorityUtils.createAuthorityList("ROLE_USER"))
        );
        paymentActionService = new PaymentActionService(paymentRepository, new PaymentAccessService());
    }

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    @DisplayName("A manual retry changes FAILED to NEW and resets the attempt budget")
    void retryFailedPaymentResetsExecutionState() {
        Payment payment = createPayment(ExecutionStatus.FAILED);
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        boolean retried = paymentActionService.retryFailedPayment(PAYMENT_ID);

        assertThat(retried).isTrue();
        assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(payment.getExecutionId()).isNull();
        assertThat(payment.getLeaseUntil()).isNull();
        assertThat(payment.getNextRetryAt()).isNull();
        assertThat(payment.getAttempts()).isZero();
        assertThat(payment.getStartedAt()).isNull();
        assertThat(payment.getFinishedAt()).isNull();
        assertThat(payment.getResult()).isNull();

        assertThat(payment.getId()).isEqualTo(PAYMENT_ID);
        assertThat(payment.getEventId()).isEqualTo(EVENT_ID);
        assertThat(payment.getPaymentMethodId()).isEqualTo("pm_test");
        assertThat(payment.getAmount()).isEqualTo(2500L);
        assertThat(payment.getCreatedAt()).isEqualTo(CREATED_AT);
        assertThat(payment.getMaxAttempts()).isEqualTo(3);
        assertThat(payment.getVersion()).isEqualTo(5L);
        assertThat(payment.getOwnerUsername()).isEqualTo("user1");
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"NEW", "IN_PROGRESS", "COMPLETED"})
    @DisplayName("A manual retry does not change a payment that is not FAILED")
    void retryFailedPaymentLeavesOtherStatesUnchanged(ExecutionStatus status) {
        Payment payment = createPayment(status);
        Payment original = createPayment(status);
        setField(original, "paymentOperationId", payment.getPaymentOperationId());
        setField(original, "orderId", payment.getOrderId());
        setField(original, "idempotencyKey", payment.getIdempotencyKey());
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        boolean retried = paymentActionService.retryFailedPayment(PAYMENT_ID);

        assertThat(retried).isFalse();
        assertThat(payment).usingRecursiveComparison().isEqualTo(original);
    }

    @Test
    @DisplayName("A manual retry reports a lookup error when the payment is missing")
    void retryFailedPaymentRejectsMissingPayment() {
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> paymentActionService.retryFailedPayment(PAYMENT_ID))
            .isInstanceOf(PaymentNotFoundException.class)
            .hasMessage("Payment with ID 7 was not found");
    }

    @Test
    @DisplayName("A foreign payment is hidden and its execution state is not reset")
    void rejectsForeignPaymentWithoutChangingIt() {
        Payment payment = createPayment(ExecutionStatus.FAILED, "another-user");
        Payment original = createPayment(ExecutionStatus.FAILED, "another-user");
        setField(original, "paymentOperationId", payment.getPaymentOperationId());
        setField(original, "orderId", payment.getOrderId());
        setField(original, "idempotencyKey", payment.getIdempotencyKey());
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        assertThatThrownBy(() -> paymentActionService.retryFailedPayment(PAYMENT_ID))
            .isInstanceOf(PaymentNotFoundException.class)
            .hasMessage("Payment with ID 7 was not found");

        assertThat(payment).usingRecursiveComparison().isEqualTo(original);
    }

    @Test
    @DisplayName("A foreign non-failed payment is hidden before its status is checked")
    void hidesForeignPaymentBeforeCheckingStatus() {
        Payment payment = createPayment(ExecutionStatus.NEW, "another-user");
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        assertThatThrownBy(() -> paymentActionService.retryFailedPayment(PAYMENT_ID))
            .isInstanceOf(PaymentNotFoundException.class);
    }

    @Test
    @DisplayName("A user cannot retry a legacy payment without an owner")
    void rejectsLegacyPaymentForUser() {
        Payment payment = createPayment(ExecutionStatus.FAILED, null);
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        assertThatThrownBy(() -> paymentActionService.retryFailedPayment(PAYMENT_ID))
            .isInstanceOf(PaymentNotFoundException.class);

        assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(payment.getAttempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("An administrator can retry a legacy failed payment without taking ownership")
    void adminRetriesLegacyPayment() {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("admin", "unused",
                AuthorityUtils.createAuthorityList("ROLE_ADMIN"))
        );
        Payment payment = createPayment(ExecutionStatus.FAILED, null);
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        assertThat(paymentActionService.retryFailedPayment(PAYMENT_ID)).isTrue();

        assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(payment.getAttempts()).isZero();
        assertThat(payment.getOwnerUsername()).isNull();
    }

    @Test
    @DisplayName("Missing authentication cannot reset a failed payment")
    void rejectsUnauthenticatedRetry() {
        SecurityContextHolder.clearContext();
        Payment payment = createPayment(ExecutionStatus.FAILED);
        when(paymentRepository.findByIdForUpdate(PAYMENT_ID)).thenReturn(Optional.of(payment));

        assertThatThrownBy(() -> paymentActionService.retryFailedPayment(PAYMENT_ID))
            .isInstanceOf(PaymentNotFoundException.class);

        assertThat(payment.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(payment.getAttempts()).isEqualTo(3);
    }

    private Payment createPayment(ExecutionStatus status) {
        return createPayment(status, "user1");
    }

    private Payment createPayment(ExecutionStatus status, String ownerUsername) {
        Payment payment = payment(EVENT_ID, ownerUsername, "pm_test", 2500L);
        setField(payment, "id", PAYMENT_ID);
        setField(payment, "executionStatus", status);
        setField(payment, "result", "Previous result");
        setField(payment, "executionId", EXECUTION_ID);
        setField(payment, "leaseUntil", FINISHED_AT.plusSeconds(30));
        setField(payment, "nextRetryAt", FINISHED_AT.plusSeconds(60));
        setField(payment, "attempts", 3);
        setField(payment, "createdAt", CREATED_AT);
        setField(payment, "startedAt", STARTED_AT);
        setField(payment, "finishedAt", FINISHED_AT);
        setField(payment, "version", 5L);
        return payment;
    }
}
