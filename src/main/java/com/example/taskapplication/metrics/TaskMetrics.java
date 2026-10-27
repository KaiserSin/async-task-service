package com.example.taskapplication.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class TaskMetrics {

    private final JdbcTemplate jdbcTemplate;
    private final Timer executionDuration;
    private final Counter recoveredTasks;
    private final Counter resultSaveErrors;

    private volatile double newTasks = Double.NaN;
    private volatile double oldestReadyAgeSeconds = Double.NaN;

    public TaskMetrics(MeterRegistry registry, JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;

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

        resultSaveErrors = Counter.builder("task.result.save.errors")
            .description("Failed final result writes")
            .register(registry);
    }

    @Scheduled(fixedDelayString = "${worker.metrics-delay-ms:5000}")
    public void updateQueueMetrics() {
        try {
            var row = jdbcTemplate.queryForMap("""
                SELECT
                    COUNT(*) AS new_tasks,
                    COALESCE(
                        EXTRACT(EPOCH FROM (
                            CURRENT_TIMESTAMP -
                            MIN(created_at) FILTER (
                                WHERE attempts < max_attempts
                                  AND (
                                      next_retry_at IS NULL
                                      OR next_retry_at <= CURRENT_TIMESTAMP
                                  )
                            )
                        )),
                        0
                    ) AS oldest_ready_age
                FROM tasks
                WHERE status = 'NEW'
                """);

            newTasks = ((Number) row.get("new_tasks")).doubleValue();
            oldestReadyAgeSeconds = Math.max(
                0,
                ((Number) row.get("oldest_ready_age")).doubleValue()
            );
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

    public void recordResultSaveError() {
        resultSaveErrors.increment();
    }
}
