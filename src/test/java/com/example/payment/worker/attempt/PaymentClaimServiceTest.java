package com.example.payment.worker.attempt;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentClaimServiceTest {

    private static final WorkerProperties TIMING = new WorkerProperties(47, 7, 5,
        new WorkerProperties.Pool(4), new WorkerProperties.Shutdown(75, 10),
        new WorkerProperties.Cleanup(30, 90, 500));

    private static final long PAYMENT_ID = 1L;
    private static final UUID EVENT_ID = UUID.fromString("12345678-1234-4321-8765-123456789abc");
    private static final long DURATION_MS = 2500L;

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentRepository.ClaimedPayment claimedPayment;

    @Mock
    private PlatformTransactionManager transactionManager;

    @Mock
    private TransactionStatus transactionStatus;

    private PaymentClaimService paymentClaimService;
    private Runnable beforeTransaction = () -> {};

    @BeforeEach
    void setUp() {
        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
            .thenAnswer(invocation -> {
                beforeTransaction.run();
                return transactionStatus;
            });
        paymentClaimService = new PaymentClaimService(
            paymentRepository, new TransactionTemplate(transactionManager), TIMING
        );
    }

    @Test
    @DisplayName("Does not create an execution attempt when no payments are ready")
    void returnsEmptyWhenNoPaymentIsAvailable() {
        when(paymentRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()))).thenReturn(Optional.empty());

        Optional<PaymentAttempt> result = paymentClaimService.claimNextPayment();

        assertThat(result).isEmpty();
        verify(paymentRepository).claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()));
    }

    @Test
    @DisplayName("Claim returns a snapshot with the saved execution ID")
    void returnsPaymentRunWithClaimedExecutionId() {
        prepareClaimedPayment();
        when(paymentRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds())))
            .thenReturn(Optional.of(claimedPayment));

        PaymentAttempt result = paymentClaimService.claimNextPayment().orElseThrow();

        ArgumentCaptor<UUID> executionId = ArgumentCaptor.forClass(UUID.class);
        verify(paymentRepository).claimNextExecution(executionId.capture(), eq(TIMING.leaseSeconds()));
        assertThat(result.id()).isEqualTo(PAYMENT_ID);
        assertThat(result.eventId()).isEqualTo(EVENT_ID);
        assertThat(result.amount()).isEqualTo(DURATION_MS);
        assertThat(result.executionId()).isNotNull().isEqualTo(executionId.getValue());
    }

    @Test
    @DisplayName("Retries retain the operation event ID while each attempt gets a different execution ID")
    void preservesEventIdAcrossExecutionAttempts() {
        prepareClaimedPayment();
        when(paymentRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds())))
            .thenReturn(Optional.of(claimedPayment));

        PaymentAttempt first = paymentClaimService.claimNextPayment().orElseThrow();
        PaymentAttempt retry = paymentClaimService.claimNextPayment().orElseThrow();

        assertThat(first.eventId()).isEqualTo(EVENT_ID);
        assertThat(retry.eventId()).isEqualTo(first.eventId());
        assertThat(retry.executionId()).isNotEqualTo(first.executionId());
    }

    @Test
    @DisplayName("The confirmed lease budget includes transaction startup, claim and commit time")
    void leaseDeadlineStartsBeforeClaimTransaction() {
        prepareClaimedPayment();
        long beforeClaim = TimeUnit.SECONDS.toNanos(100);
        AtomicLong clock = new AtomicLong(beforeClaim);
        paymentClaimService = spy(paymentClaimService);
        doAnswer(invocation -> clock.get()).when(paymentClaimService).nanoTime();
        beforeTransaction = () -> clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
        when(paymentRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()))).thenAnswer(invocation -> {
            clock.addAndGet(TimeUnit.SECONDS.toNanos(3));
            return Optional.of(claimedPayment);
        });
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.SECONDS.toNanos(4));
            return null;
        }).when(transactionManager).commit(transactionStatus);

        PaymentAttempt result = paymentClaimService.claimNextPayment().orElseThrow();

        assertThat(result.leaseDeadlineNanos()).isEqualTo(beforeClaim + TIMING.leaseNanos());
        assertThat(result.leaseDeadlineNanos() - clock.get())
            .isEqualTo(TIMING.leaseNanos() - TimeUnit.SECONDS.toNanos(9));
    }

    @Test
    @DisplayName("Propagates database errors without automatically retrying the claim")
    void propagatesDatabaseFailureWithoutRetry() {
        var failure = new DataAccessResourceFailureException("Database is not available");
        when(paymentRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()))).thenThrow(failure);

        assertThatThrownBy(() -> paymentClaimService.claimNextPayment()).isSameAs(failure);

        verify(paymentRepository).claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()));
    }

    @Test
    @DisplayName("Does not return a payment snapshot to the worker when commit fails")
    void doesNotReturnPaymentRunWhenCommitFails() {
        prepareClaimedPayment();
        var failure = new TransactionSystemException("Failed to commit the transaction");
        when(paymentRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds())))
            .thenReturn(Optional.of(claimedPayment));
        doThrow(failure).when(transactionManager).commit(transactionStatus);

        assertThatThrownBy(() -> paymentClaimService.claimNextPayment()).isSameAs(failure);
    }

    private void prepareClaimedPayment() {
        when(claimedPayment.getId()).thenReturn(PAYMENT_ID);
        when(claimedPayment.getEventId()).thenReturn(EVENT_ID);
        when(claimedPayment.getAmount()).thenReturn(DURATION_MS);
    }
}
