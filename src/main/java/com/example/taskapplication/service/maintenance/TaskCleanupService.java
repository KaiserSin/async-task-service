package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
@RequiredArgsConstructor
public class TaskCleanupService {

    private static final int MAX_ROUNDS = 10;

    private final TaskRepository taskRepository;
    private final TransactionTemplate cleanupTransactionTemplate;

    @Value("${worker.cleanup.retention-days:30}")
    private long retentionDays;

    @Value("${worker.cleanup.failed-retention-days:90}")
    private long failedRetentionDays;

    @Value("${worker.cleanup.batch-size:500}")
    private int batchSize;

    @PostConstruct
    private void validateSettings() {
        if (retentionDays < 1) {
            throw new IllegalArgumentException(
                "worker.cleanup.retention-days must be greater than 0"
            );
        }
        if (failedRetentionDays < 1 || batchSize < 1) {
            throw new IllegalArgumentException(
                "worker.cleanup.failed-retention-days and batch-size must be greater than 0"
            );
        }
    }

    public int deleteExpiredTasks() {
        Instant now = Instant.now();
        Instant completedCutoff = now.minus(retentionDays, ChronoUnit.DAYS);
        Instant failedCutoff = now.minus(failedRetentionDays, ChronoUnit.DAYS);
        int deleted = 0;

        // Bound one run well below the ShedLock lease and give both statuses a turn.
        for (int round = 0; round < MAX_ROUNDS && !Thread.currentThread().isInterrupted(); round++) {
            int completed = deleteBatch(TaskStatus.COMPLETED, completedCutoff);
            deleted += completed;
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
            int failed = deleteBatch(TaskStatus.FAILED, failedCutoff);
            deleted += failed;
            if (completed < batchSize && failed < batchSize) {
                break;
            }
        }
        return deleted;
    }

    private int deleteBatch(TaskStatus status, Instant cutoff) {
        return cleanupTransactionTemplate.execute(transaction ->
            taskRepository.deleteFinishedBefore(status.name(), cutoff, batchSize)
        );
    }
}
