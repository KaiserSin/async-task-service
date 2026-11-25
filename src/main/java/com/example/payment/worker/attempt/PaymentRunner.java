package com.example.payment.worker.attempt;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.domain.ExecutionStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentRunner {

    private final PaymentUpdateService paymentUpdateService;
    private final PaymentMetrics paymentMetrics;
    private final PaymentHandler paymentHandler;

    public void executePayment(PaymentAttempt attempt) {
        long startedAt = System.nanoTime();
        try {
            executePaymentBody(attempt);
        } finally {
            paymentMetrics.recordExecution(System.nanoTime() - startedAt);
        }
    }

    private void executePaymentBody(PaymentAttempt attempt) {
        long leaseDeadline = attempt.leaseDeadlineNanos();
        try {
            if (leaseDeadline - nanoTime() <= 0) {
                logExpiredLease(attempt);
                return;
            }
            paymentHandler.handle(attempt);
        } catch (InterruptedException e) {
            log.info("Payment {} was interrupted", attempt.id());
            try {
                if (!paymentUpdateService.requeueInterruptedPayment(attempt.id(), attempt.executionId())) {
                    log.warn("Ignored interruption from old attempt {} of payment {}",
                        attempt.executionId(), attempt.id());
                }
            } catch (RuntimeException failure) {
                paymentMetrics.recordResultSaveError();
                log.error("Could not requeue interrupted payment {}; it will be recovered after its lease expires",
                    attempt.id(), failure);
            } finally {
                Thread.currentThread().interrupt();
            }
            return;
        } catch (RuntimeException e) {
            log.error("Attempt {} of payment {} failed", attempt.executionId(), attempt.id(), e);
            saveAttemptFailure(attempt, e);
            return;
        }

        if (leaseDeadline - nanoTime() <= 0) {
            logExpiredLease(attempt);
            return;
        }
        saveCompletion(attempt);
    }

    long nanoTime() {
        return System.nanoTime();
    }

    private void logExpiredLease(PaymentAttempt attempt) {
        log.warn("Confirmed lease expired for attempt {} of payment {}; stopping work",
            attempt.executionId(), attempt.id());
    }

    private void saveAttemptFailure(PaymentAttempt attempt, RuntimeException failure) {
        try {
            var status = failure instanceof StripeRequestRejectedException rejected
                ? paymentUpdateService.failRejectedRequest(attempt, rejected)
                : paymentUpdateService.fail(attempt.id(), attempt.executionId(), "Payment execution failed");
            if (status.isEmpty()) {
                log.warn("Ignored failure from old attempt {} of payment {}",
                    attempt.executionId(), attempt.id());
            } else if (status.get() == ExecutionStatus.FAILED) {
                paymentMetrics.recordExecutionFailed(1);
            }
        } catch (RuntimeException e) {
            paymentMetrics.recordResultSaveError();
            log.error("Could not save failed attempt of payment {}", attempt.id(), e);
        }
    }

    private void saveCompletion(PaymentAttempt attempt) {
        try {
            boolean updated = paymentUpdateService.complete(attempt.id(), attempt.executionId(), "Payment execution completed");
            if (!updated) {
                log.warn("Ignored result from old attempt {} of payment {}",
                    attempt.executionId(), attempt.id());
            } else {
                paymentMetrics.recordExecutionCompleted();
            }
        } catch (RuntimeException e) {
            paymentMetrics.recordResultSaveError();
            log.error("Could not save result for payment {}",
                attempt.id(), e);
        }
    }
}
