package com.example.payment.worker.attempt;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentClaimService {

    private final PaymentRepository paymentRepository;
    private final TransactionTemplate workerTransactionTemplate;
    private final WorkerProperties workerProperties;

    public Optional<PaymentAttempt> claimNextPayment() {
        long claimStartedAt = nanoTime();
        return workerTransactionTemplate.execute(transactionStatus -> {
            UUID executionId = UUID.randomUUID();
            return paymentRepository.claimNextExecution(executionId, workerProperties.leaseSeconds())
                .map(payment -> new PaymentAttempt(
                    payment.getId(), payment.getEventId(), executionId, payment.getPaymentOperationId(),
                    payment.getOrderId(), payment.getAmount(), payment.getCurrency(),
                    payment.getPaymentMethodId(), payment.getIdempotencyKey(),
                    claimStartedAt + workerProperties.leaseNanos(), payment.getCreateIntent(),
                    payment.getProviderPaymentId(), payment.getClaimVersion(), payment.getConfirmationKey(), payment.getRetry()
                ));
        });
    }

    long nanoTime() {
        return System.nanoTime();
    }
}
