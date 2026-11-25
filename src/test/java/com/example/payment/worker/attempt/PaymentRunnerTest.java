package com.example.payment.worker.attempt;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.domain.ExecutionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentRunnerTest {
    @Mock PaymentUpdateService updates;
    @Mock PaymentMetrics metrics;
    @Mock PaymentHandler handler;
    PaymentRunner runner;
    PaymentAttempt attempt;

    @BeforeEach
    void setUp() {
        runner = spy(new PaymentRunner(updates, metrics, handler));
        attempt = new PaymentAttempt(1L, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            UUID.randomUUID(), 2500L, "EUR", "pm_test", "stable-key",
            System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
    }

    @Test
    void executesHandlerAndSavesCompletion() throws Exception {
        when(updates.complete(attempt.id(), attempt.executionId(), "Payment execution completed")).thenReturn(true);
        runner.executePayment(attempt);
        var order = inOrder(handler, updates, metrics);
        order.verify(handler).handle(attempt);
        order.verify(updates).complete(attempt.id(), attempt.executionId(), "Payment execution completed");
        order.verify(metrics).recordExecutionCompleted();
        verify(metrics).recordExecution(anyLong());
    }

    @Test
    void ignoresStaleCompletion() {
        runner.executePayment(attempt);
        verify(metrics, never()).recordExecutionCompleted();
    }

    @Test
    void expiredLeaseDoesNotExecuteHandler() {
        doReturn(attempt.leaseDeadlineNanos()).when(runner).nanoTime();
        runner.executePayment(attempt);
        verifyNoInteractions(handler, updates);
    }

    @Test
    void expiredLeaseDoesNotSaveCompletion() throws Exception {
        doAnswer(invocation -> {
            doReturn(attempt.leaseDeadlineNanos()).when(runner).nanoTime();
            return null;
        }).when(handler).handle(attempt);
        runner.executePayment(attempt);
        verifyNoInteractions(updates);
    }

    @Test
    void interruptionRequeuesAndRestoresInterruptFlag() throws Exception {
        doThrow(new InterruptedException()).when(handler).handle(attempt);
        try {
            runner.executePayment(attempt);
            org.assertj.core.api.Assertions.assertThat(Thread.currentThread().isInterrupted()).isTrue();
            verify(updates).requeueInterruptedPayment(attempt.id(), attempt.executionId());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void failedAttemptRecordsTerminalFailure() throws Exception {
        doThrow(new IllegalStateException()).when(handler).handle(attempt);
        when(updates.fail(attempt.id(), attempt.executionId(), "Payment execution failed"))
            .thenReturn(Optional.of(ExecutionStatus.FAILED));
        runner.executePayment(attempt);
        verify(metrics).recordExecutionFailed(1);
    }

    @Test
    void retryableFailureDoesNotRecordTerminalFailure() throws Exception {
        doThrow(new IllegalStateException()).when(handler).handle(attempt);
        when(updates.fail(attempt.id(), attempt.executionId(), "Payment execution failed"))
            .thenReturn(Optional.of(ExecutionStatus.NEW));
        runner.executePayment(attempt);
        verify(metrics, never()).recordExecutionFailed(anyLong());
    }

    @Test
    void resultSaveFailureIsRecorded() {
        when(updates.complete(anyLong(), any(), anyString())).thenThrow(new IllegalStateException());
        runner.executePayment(attempt);
        verify(metrics).recordResultSaveError();
    }
}
