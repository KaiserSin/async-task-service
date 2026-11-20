package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
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

    private final WorkerProperties workerProperties;

    public int deleteExpiredTasks() {
        var cleanup = workerProperties.cleanup();
        Instant now = Instant.now();
        Instant completedCutoff = now.minus(cleanup.retentionDays(), ChronoUnit.DAYS);
        Instant failedCutoff = now.minus(cleanup.failedRetentionDays(), ChronoUnit.DAYS);
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
            if (completed < cleanup.batchSize() && failed < cleanup.batchSize()) {
                break;
            }
        }
        return deleted;
    }

    private int deleteBatch(TaskStatus status, Instant cutoff) {
        return cleanupTransactionTemplate.execute(transaction ->
            taskRepository.deleteFinishedBefore(status.name(), cutoff, workerProperties.cleanup().batchSize())
        );
    }
}
