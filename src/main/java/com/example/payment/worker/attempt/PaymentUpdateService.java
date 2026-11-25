package com.example.payment.worker.attempt;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentUpdateService {

    private final PaymentRepository paymentRepository;
    private final WorkerProperties workerProperties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markStripeSubmission(PaymentAttempt attempt) {
        if (paymentRepository.markStripeSubmission(attempt.id(), attempt.executionId(), attempt.createIntent(),
                attempt.confirmationKey()) != 1) {
            throw new IllegalStateException("Payment attempt cannot submit a Stripe request");
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveStripeResult(PaymentAttempt attempt, String intentId, PaymentStatus status) {
        if (paymentRepository.saveStripeResult(attempt.id(), attempt.executionId(), intentId, status.name(),
                attempt.claimVersion()) != 1) {
            throw new IllegalStateException("Could not record Stripe response for the active attempt");
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean renewLease(Long id, UUID executionId) {
        return paymentRepository.renewLease(id, executionId, workerProperties.leaseSeconds()) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean requeueInterruptedPayment(Long id, UUID executionId) {
        return paymentRepository.requeueInterruptedExecution(id, executionId) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<ExecutionStatus> fail(Long id, UUID executionId, String result) {
        return paymentRepository.failExecution(
            id, executionId, result, workerProperties.retryBackoffSeconds()
        ).map(ExecutionStatus::valueOf);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<ExecutionStatus> failRejectedRequest(PaymentAttempt attempt, StripeRequestRejectedException rejection) {
        return paymentRepository.failExecution(attempt.id(), attempt.executionId(), rejection.getMessage(),
            workerProperties.retryBackoffSeconds(), rejection.allowsAutomaticRetry(), attempt.createIntent(),
            attempt.confirmationKey())
            .map(ExecutionStatus::valueOf);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean complete(Long id, UUID executionId, String result) {
        return paymentRepository.completeExecution(id, executionId, result, Instant.now()) == 1;
    }
}
