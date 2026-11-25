package com.example.payment.worker.attempt;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentUpdateServiceTest {

    private static final WorkerProperties TIMING = new WorkerProperties(47, 7, 5,
        new WorkerProperties.Pool(4), new WorkerProperties.Shutdown(75, 10),
        new WorkerProperties.Cleanup(30, 90, 500));

    private static final Long PAYMENT_ID = 7L;
    private static final UUID EXECUTION_ID = UUID.fromString("cc258051-0eba-4ede-9311-eb59eb55609a");

    @Mock
    private PaymentRepository paymentRepository;

    private PaymentUpdateService paymentUpdateService;

    @BeforeEach
    void setUp() {
        paymentUpdateService = new PaymentUpdateService(paymentRepository, TIMING);
    }

    @Test
    @DisplayName("Renews the lease for the execution owner")
    void renewLeasePreservesOwner() {
        when(paymentRepository.renewLease(
            PAYMENT_ID, EXECUTION_ID, TIMING.leaseSeconds()
        )).thenReturn(1);

        boolean updated = paymentUpdateService.renewLease(PAYMENT_ID, EXECUTION_ID);

        assertThat(updated).isTrue();
        verify(paymentRepository).renewLease(
            PAYMENT_ID, EXECUTION_ID, TIMING.leaseSeconds()
        );
    }

    @Test
    @DisplayName("Returns false when the lease renewal affects no rows")
    void updateProgressReturnsFalseWhenNothingWasUpdated() {
        when(paymentRepository.renewLease(
            PAYMENT_ID, EXECUTION_ID, TIMING.leaseSeconds()
        )).thenReturn(0);

        assertThat(paymentUpdateService.renewLease(PAYMENT_ID, EXECUTION_ID)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"0, false", "1, true"})
    @DisplayName("Requeues only the interrupted execution owner and reports whether it was released")
    void requeuesInterruptedPayment(int updatedRows, boolean expected) {
        when(paymentRepository.requeueInterruptedExecution(PAYMENT_ID, EXECUTION_ID)).thenReturn(updatedRows);

        assertThat(paymentUpdateService.requeueInterruptedPayment(PAYMENT_ID, EXECUTION_ID)).isEqualTo(expected);

        verify(paymentRepository).requeueInterruptedExecution(PAYMENT_ID, EXECUTION_ID);
    }


    @ParameterizedTest
    @CsvSource(value = {
        "NEW",
        "FAILED",
        "NULL"
    }, nullValues = "NULL")
    @DisplayName("Returns the saved retry or terminal status, or no result for a stale attempt")
    void failPreservesOwner(
        ExecutionStatus expectedStatus
    ) {
        when(paymentRepository.failExecution(PAYMENT_ID, EXECUTION_ID, "Payment failed",
            TIMING.retryBackoffSeconds()))
            .thenReturn(Optional.ofNullable(expectedStatus).map(ExecutionStatus::name));

        assertThat(paymentUpdateService.fail(PAYMENT_ID, EXECUTION_ID, "Payment failed"))
            .isEqualTo(Optional.ofNullable(expectedStatus));

        verify(paymentRepository).failExecution(PAYMENT_ID, EXECUTION_ID, "Payment failed",
            TIMING.retryBackoffSeconds());
    }


    @Test
    @DisplayName("Completes the execution owner with its result and finish time")
    void completePreservesOwnerResultAndFinishTime() {
        String result = "Execution result";
        when(paymentRepository.completeExecution(
            eq(PAYMENT_ID), eq(EXECUTION_ID), eq(result), any(Instant.class)
        )).thenReturn(1);

        boolean updated = paymentUpdateService.complete(PAYMENT_ID, EXECUTION_ID, result);

        ArgumentCaptor<Instant> finishedAt = ArgumentCaptor.forClass(Instant.class);
        assertThat(updated).isTrue();
        verify(paymentRepository).completeExecution(
            eq(PAYMENT_ID), eq(EXECUTION_ID), eq(result), finishedAt.capture()
        );
        assertThat(finishedAt.getValue()).isNotNull();
    }

    @ParameterizedTest
    @CsvSource({"0, false", "1, true"})
    @DisplayName("Returns the completion outcome based on the number of updated rows")
    void completeReturnsRepositoryOutcome(int updatedRows, boolean expected) {
        when(paymentRepository.completeExecution(
            eq(PAYMENT_ID), eq(EXECUTION_ID), eq("Done"), any(Instant.class)
        )).thenReturn(updatedRows);

        assertThat(paymentUpdateService.complete(PAYMENT_ID, EXECUTION_ID, "Done")).isEqualTo(expected);
    }
}
