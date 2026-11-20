package com.example.taskapplication.metrics;

import com.example.taskapplication.repository.TaskRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class TaskMetrics {

    private final TaskRepository taskRepository;
    private final Timer executionDuration;
    private final Counter recoveredTasks;
    private final Counter completedTasks;
    private final Counter failedTasks;
    private final Counter resultSaveErrors;

    private volatile double newTasks = Double.NaN;
    private volatile double oldestReadyAgeSeconds = Double.NaN;

    public TaskMetrics(MeterRegistry registry, TaskRepository taskRepository) {
        this.taskRepository = taskRepository;

        Gauge.builder("task.queue.new", this, metrics -> metrics.newTasks)
            .description("Tasks waiting in NEW status")
            .register(registry);

        TimeGauge.builder(
                "task.queue.oldest.ready.age",
                this,
                TimeUnit.SECONDS,
                metrics -> metrics.oldestReadyAgeSeconds
            )
            .description("Age since creation of the oldest ready task")
            .register(registry);

        executionDuration = Timer.builder("task.execution.duration")
            .description("Task attempt time")
            .register(registry);

        recoveredTasks = Counter.builder("task.recovered")
            .description("Tasks returned to the queue after lease expiry")
            .register(registry);

        completedTasks = Counter.builder("task.completed")
            .description("Tasks transitioned to COMPLETED")
            .register(registry);

        failedTasks = Counter.builder("task.failed")
            .description("Tasks transitioned to FAILED")
            .register(registry);

        resultSaveErrors = Counter.builder("task.result.save.errors")
            .description("Failed final result writes")
            .register(registry);
    }

    @Scheduled(fixedDelayString = "${worker.metrics-delay-ms:60000}")
    public void updateQueueMetrics() {
        try {
            var stats = taskRepository.readQueueStats();
            newTasks = stats.newTasks();
            oldestReadyAgeSeconds = Math.max(0, stats.oldestReadyAgeSeconds());
        } catch (DataAccessException e) {
            newTasks = Double.NaN;
            oldestReadyAgeSeconds = Double.NaN;
            log.warn("Could not read queue metrics", e);
        }
    }

    public void recordExecution(long durationNanos) {
        executionDuration.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordRecovered(long count) {
        recoveredTasks.increment(count);
    }

    public void recordCompleted() {
        completedTasks.increment();
    }

    public void recordFailed(long count) {
        failedTasks.increment(count);
    }

    public void recordResultSaveError() {
        resultSaveErrors.increment();
    }
}
