package com.example.payment.maintenance;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
@RequiredArgsConstructor
public class PaymentCleanupService {

    private static final int MAX_ROUNDS = 10;

    private final PaymentRepository paymentRepository;
    private final TransactionTemplate cleanupTransactionTemplate;

    private final WorkerProperties workerProperties;

    public int archiveExpiredPayments() {
        var cleanup = workerProperties.cleanup();
        Instant now = Instant.now();
        Instant completedCutoff = now.minus(cleanup.retentionDays(), ChronoUnit.DAYS);
        Instant failedCutoff = now.minus(cleanup.failedRetentionDays(), ChronoUnit.DAYS);
        int archived = 0;

        for (int round = 0; round < MAX_ROUNDS && !Thread.currentThread().isInterrupted(); round++) {
            int completed = archiveBatch(ExecutionStatus.COMPLETED, completedCutoff);
            archived += completed;
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            int failed = archiveBatch(ExecutionStatus.FAILED, failedCutoff);
            archived += failed;
            if (completed < cleanup.batchSize() && failed < cleanup.batchSize()) {
                break;
            }
        }
        return archived;
    }

    public void deleteExpiredWebhookEvents() {
        int batchSize = workerProperties.cleanup().batchSize();
        for (int round = 0; round < MAX_ROUNDS && !Thread.currentThread().isInterrupted(); round++) {
            int deleted = cleanupTransactionTemplate.execute(tx -> paymentRepository.deleteExpiredWebhookEvents(batchSize));
            if (deleted < batchSize) {
                break;
            }
        }
    }

    private int archiveBatch(ExecutionStatus status, Instant cutoff) {
        return cleanupTransactionTemplate.execute(transaction ->
            paymentRepository.archiveFinishedBefore(status.name(), cutoff, workerProperties.cleanup().batchSize())
        );
    }
}
