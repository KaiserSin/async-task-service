package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.TaskStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TaskRunner {

    private final TaskUpdateService taskUpdateService;
    private final TaskMetrics taskMetrics;
    private final TaskHandler taskHandler;
    private final WorkerProperties workerProperties;

    public void executeTask(TaskRun run) {
        long startedAt = System.nanoTime();
        try {
            executeTaskBody(run);
        } finally {
            taskMetrics.recordExecution(System.nanoTime() - startedAt);
        }
    }

    private void executeTaskBody(TaskRun run) {
        long elapsed = 0;
        int progress = 0;
        long leaseDeadline = run.leaseDeadlineNanos();

        try {
            while (elapsed < run.duration()) {
                long remainingLeaseMillis = TimeUnit.NANOSECONDS.toMillis(leaseDeadline - nanoTime());
                if (remainingLeaseMillis <= 0) {
                    logExpiredLease(run);
                    return;
                }
                long step = Math.min(Math.min(1000L, run.duration() - elapsed), remainingLeaseMillis);
                taskHandler.handleStep(run, elapsed, step);
                elapsed += step;
                progress = Math.min(
                    99, (int) ((double) elapsed / run.duration() * 100)
                );

                if (leaseDeadline - nanoTime() <= 0) {
                    logExpiredLease(run);
                    return;
                }
                if (elapsed < run.duration()) {
                    // Count SQL and commit time against the renewed lease as well.
                    long heartbeatStartedAt = nanoTime();
                    try {
                        if (!taskUpdateService.updateProgress(run.id(), run.executionId(), progress)) {
                            log.warn("Attempt {} of task {} is no longer active",
                                run.executionId(), run.id());
                            return;
                        }
                        leaseDeadline = heartbeatStartedAt + workerProperties.leaseNanos();
                    } catch (DataAccessException | TransactionException e) {
                        log.warn("Could not renew lease for attempt {} of task {}; continuing within its confirmed lease",
                            run.executionId(), run.id(), e);
                    }
                }
            }
        } catch (InterruptedException e) {
            log.info("Task {} was interrupted", run.id());
            try {
                if (!taskUpdateService.requeueInterruptedTask(run.id(), run.executionId())) {
                    log.warn("Ignored interruption from old attempt {} of task {}",
                        run.executionId(), run.id());
                }
            } catch (RuntimeException failure) {
                taskMetrics.recordResultSaveError();
                log.error("Could not requeue interrupted task {}; it will be recovered after its lease expires",
                    run.id(), failure);
            } finally {
                Thread.currentThread().interrupt();
            }
            return;
        } catch (RuntimeException e) {
            log.error("Attempt {} of task {} failed", run.executionId(), run.id(), e);
            saveAttemptFailure(run, progress);
            return;
        }

        if (leaseDeadline - nanoTime() <= 0) {
            logExpiredLease(run);
            return;
        }
        saveCompletion(run);
    }

    long nanoTime() {
        return System.nanoTime();
    }

    private void logExpiredLease(TaskRun run) {
        log.warn("Confirmed lease expired for attempt {} of task {}; stopping work",
            run.executionId(), run.id());
    }

    private void saveAttemptFailure(TaskRun run, int progress) {
        try {
            var status = taskUpdateService.fail(run.id(), run.executionId(), progress, "Task failed");
            if (status.isEmpty()) {
                log.warn("Ignored failure from old attempt {} of task {}",
                    run.executionId(), run.id());
            } else if (status.get() == TaskStatus.FAILED) {
                taskMetrics.recordFailed(1);
            }
        } catch (RuntimeException e) {
            taskMetrics.recordResultSaveError();
            log.error("Could not save failed attempt of task {}", run.id(), e);
        }
    }

    private void saveCompletion(TaskRun run) {
        try {
            boolean updated = taskUpdateService.complete(run.id(), run.executionId(), "Task completed");
            if (!updated) {
                log.warn("Ignored result from old attempt {} of task {}",
                    run.executionId(), run.id());
            } else {
                taskMetrics.recordCompleted();
            }
        } catch (RuntimeException e) {
            taskMetrics.recordResultSaveError();
            log.error("Could not save result for task {}",
                run.id(), e);
        }
    }
}
