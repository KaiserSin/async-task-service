package com.example.taskapplication.service;

import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.service.execution.TaskExecutionService;
import com.example.taskapplication.service.execution.TaskStatusService;
import com.example.taskapplication.service.maintenance.TaskCleanupService;
import com.example.taskapplication.service.maintenance.TaskRecoveryService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class TaskService {

    private final ThreadPoolExecutor taskExecutor;
    private final TaskExecutionService taskExecutionService;
    private final TaskStatusService taskStatusService;
    private final TaskRecoveryService taskRecoveryService;
    private final TaskCleanupService taskCleanupService;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final TaskMetrics taskMetrics;

    @Value("${worker.shutdown.grace-period-seconds:45}")
    private long shutdownGracePeriodSeconds;

    @Value("${worker.shutdown.interrupt-wait-seconds:10}")
    private long shutdownInterruptWaitSeconds;

    private volatile boolean stopping;

    @PostConstruct
    private void validateShutdownSettings() {
        if (shutdownGracePeriodSeconds < 1 || shutdownInterruptWaitSeconds < 1) {
            throw new IllegalArgumentException("Worker shutdown wait times must be greater than 0");
        }
    }

    @Scheduled(fixedDelayString = "${worker.poll-delay-ms:1000}")
    public void processTask() {
        if (stopping || taskExecutor.isShutdown()) {
            return;
        }

        for (int i = 0; i < taskExecutor.getMaximumPoolSize(); i++) {
            try {
                taskExecutor.execute(this::runWorker);
            } catch (RejectedExecutionException e) {
                break;
            }
        }
    }

    @Scheduled(fixedDelayString = "${worker.recovery-delay-ms:5000}")
    public void recoverExpiredTasks() {
        if (stopping) {
            return;
        }

        try {
            var tasks = taskRecoveryService.recoverExpiredTasks();
            long recovered = tasks.stream()
                .filter(task -> TaskStatus.NEW.name().equals(task.get("status")))
                .count();
            taskMetrics.recordRecovered(recovered);
        } catch (RuntimeException e) {
            log.error("Could not recover tasks with expired leases", e);
        }
    }

    @Scheduled(cron = "${worker.cleanup.cron:0 0 3 * * *}", zone = "UTC")
    public void cleanupCompletedTasks() {
        if (stopping) {
            return;
        }

        try {
            int deleted = taskCleanupService.deleteCompletedTasks();
            if (deleted > 0) {
                log.info("Deleted {} completed tasks", deleted);
            }
        } catch (RuntimeException e) {
            log.error("Could not delete old completed tasks", e);
        }
    }

    private void runWorker() {
        try {
            while (!stopping && !Thread.currentThread().isInterrupted()) {
                Optional<TaskRun> run = taskExecutionService.claimNextTask();
                if (run.isEmpty()) {
                    return;
                }
                taskStatusService.executeTask(run.get());
            }
        } catch (RuntimeException e) {
            log.error("Worker loop failed", e);
        }
    }

    @EventListener(ContextClosedEvent.class)
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @PreDestroy
    public synchronized void shutdown() {
        if (stopping) {
            return;
        }
        stopping = true;

        long startedAt = System.nanoTime();
        long graceDeadline = startedAt + TimeUnit.SECONDS.toNanos(shutdownGracePeriodSeconds);
        long shutdownDeadline = graceDeadline + TimeUnit.SECONDS.toNanos(shutdownInterruptWaitSeconds);
        CountDownLatch kafkaStopped = new CountDownLatch(1);
        taskExecutor.shutdown();

        try {
            if (kafkaListenerEndpointRegistry.getListenerContainers().isEmpty()) {
                kafkaStopped.countDown();
            } else {
                kafkaListenerEndpointRegistry.stop(kafkaStopped::countDown);
            }

            log.info("Waiting for workers and Kafka listeners to stop");
            if (!taskExecutor.awaitTermination(remainingNanos(graceDeadline), TimeUnit.NANOSECONDS)) {
                taskExecutor.shutdownNow();
                if (!taskExecutor.awaitTermination(remainingNanos(shutdownDeadline), TimeUnit.NANOSECONDS)) {
                    log.warn("Some workers are still running; tasks will be recovered after their leases expire");
                }
            }
            if (!kafkaStopped.await(remainingNanos(shutdownDeadline), TimeUnit.NANOSECONDS)) {
                log.warn("Kafka listeners did not stop in time; unconfirmed messages will be sent again");
            }
        } catch (InterruptedException e) {
            taskExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn("App shutdown was interrupted", e);
        } catch (RuntimeException e) {
            taskExecutor.shutdownNow();
            log.error("App shutdown failed", e);
        }
    }

    private long remainingNanos(long deadline) {
        return Math.max(0L, deadline - System.nanoTime());
    }
}
