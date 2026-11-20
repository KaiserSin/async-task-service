package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.service.execution.WorkerLifecycle;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockAssert;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class MaintenanceScheduler {

    private final WorkerLifecycle workerLifecycle;
    private final TaskRecoveryService taskRecoveryService;
    private final TaskCleanupService taskCleanupService;
    private final TaskMetrics taskMetrics;

    @Scheduled(fixedDelayString = "${worker.recovery-delay-ms:5000}")
    public void recoverExpiredTasks() {
        if (workerLifecycle.isStopping()) {
            return;
        }

        try {
            var counts = taskRecoveryService.recoverExpiredTasks();
            taskMetrics.recordRecovered(counts.requeued());
            taskMetrics.recordFailed(counts.failed());
        } catch (RuntimeException e) {
            log.error("Could not recover tasks with expired leases", e);
        }
    }

    @Scheduled(cron = "${worker.cleanup.cron:0 0 */12 * * *}", zone = "UTC")
    @SchedulerLock(name = "task-cleanup", lockAtMostFor = "PT10M", lockAtLeastFor = "PT1M")
    public void cleanupFinishedTasks() {
        if (workerLifecycle.isStopping()) {
            return;
        }
        LockAssert.assertLocked();

        try {
            int deleted = taskCleanupService.deleteExpiredTasks();
            if (deleted > 0) {
                log.info("Deleted {} expired completed or failed tasks", deleted);
            }
        } catch (RuntimeException e) {
            log.error("Could not clean up expired tasks", e);
        }
    }

}
