package com.example.payment.worker;

import com.example.payment.worker.attempt.PaymentClaimService;
import com.example.payment.worker.attempt.PaymentAttempt;
import com.example.payment.worker.attempt.PaymentRunner;
import com.example.payment.database.DatabaseLogFilter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
@RequiredArgsConstructor
class PaymentPoller {

    private final WorkerLifecycle workerLifecycle;
    private final PaymentClaimService paymentClaimService;
    private final PaymentRunner paymentRunner;

    @Scheduled(fixedDelayString = "${worker.poll-delay-ms:1000}")
    public void startWorker() {
        workerLifecycle.startWorker(DatabaseLogFilter.backgroundTask(this::runWorker));
    }

    private void runWorker() {
        try {
            while (!workerLifecycle.isStopping() && !Thread.currentThread().isInterrupted()) {
                DatabaseLogFilter.setBackgroundOperation(null);
                Optional<PaymentAttempt> attempt = paymentClaimService.claimNextPayment();
                if (attempt.isEmpty()) {
                    return;
                }
                DatabaseLogFilter.setBackgroundOperation(attempt.get().paymentOperationId());
                startWorker();
                paymentRunner.executePayment(attempt.get());
            }
        } catch (RuntimeException e) {
            log.error("Worker loop failed", e);
        }
    }

}
