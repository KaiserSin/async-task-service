package com.example.taskapplication.service.execution;

import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.TaskStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;

@Service
@Slf4j
@RequiredArgsConstructor
public class TaskStatusService {

    private final TaskUpdateService taskUpdateService;
    private final TaskMetrics taskMetrics;

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

        try {
            while (elapsed < run.duration()) {
                long step = Math.min(1000L, run.duration() - elapsed);
                performStep(step);
                elapsed += step;
                progress = Math.min(
                    99, (int) ((double) elapsed / run.duration() * 100)
                );

                if (elapsed < run.duration()
                    && !taskUpdateService.updateProgress(
                        run.id(), run.executionId(), progress
                    )) {
                    log.warn("Attempt {} of task {} is no longer active",
                        run.executionId(), run.id());
                    return;
                }
            }
        } catch (InterruptedException e) {
            log.info("Task {} was interrupted", run.id());
            try {
                saveFinal(run, TaskStatus.FAILED, progress, "Task was interrupted");
            } finally {
                Thread.currentThread().interrupt();
            }
            return;
        } catch (DataAccessException | TransactionException e) {

            log.error("Could not save progress for task {}",
                run.id(), e);
            return;
        } catch (RuntimeException e) {
            log.error("Task {} failed", run.id(), e);
            saveFinal(run, TaskStatus.FAILED, progress, "Task failed");
            return;
        }

        saveFinal(run, TaskStatus.COMPLETED, 100, "Task completed");
    }

    void performStep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private void saveFinal(
        TaskRun run, TaskStatus status, int progress, String result
    ) {
        try {
            boolean updated = taskUpdateService.updateTaskStatus(
                run.id(), run.executionId(), status, progress, result
            );
            if (!updated) {
                log.warn("Ignored result from old attempt {} of task {}",
                    run.executionId(), run.id());
            }
        } catch (RuntimeException e) {
            taskMetrics.recordResultSaveError();
            log.error("Could not save result for task {}",
                run.id(), e);
        }
    }
}
