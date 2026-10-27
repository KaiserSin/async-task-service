package com.example.taskapplication.metrics;

import com.example.taskapplication.service.TaskService;
import com.example.taskapplication.service.execution.TaskExecutionService;
import com.example.taskapplication.service.execution.TaskStatusService;
import com.example.taskapplication.service.maintenance.TaskCleanupService;
import com.example.taskapplication.service.maintenance.TaskRecoveryService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskMetricsTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private SimpleMeterRegistry registry;
    private TaskMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new TaskMetrics(registry, jdbcTemplate);
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void readsQueue() {
        assertThat(registry.get("task.queue.new").gauge().value()).isNaN();
        when(jdbcTemplate.queryForMap(anyString()))
            .thenReturn(Map.of("new_tasks", 4L, "oldest_ready_age", 30));

        metrics.updateQueueMetrics();

        assertThat(registry.get("task.queue.new").gauge().value()).isEqualTo(4);
        assertThat(registry.get("task.queue.oldest.ready.age").timeGauge()
            .value(TimeUnit.SECONDS)).isEqualTo(30);
    }

    @Test
    void clearsStaleQueueValues() {
        when(jdbcTemplate.queryForMap(anyString()))
            .thenReturn(Map.of("new_tasks", 4L, "oldest_ready_age", 30))
            .thenThrow(new DataAccessResourceFailureException("Database is not available"));
        metrics.updateQueueMetrics();

        metrics.updateQueueMetrics();

        assertThat(registry.get("task.queue.new").gauge().value()).isNaN();
        assertThat(registry.get("task.queue.oldest.ready.age").timeGauge()
            .value(TimeUnit.SECONDS)).isNaN();
    }

    @Test
    void recordsAttemptMetrics() {
        metrics.recordExecution(TimeUnit.SECONDS.toNanos(2));
        metrics.recordExecution(TimeUnit.MILLISECONDS.toNanos(500));
        metrics.recordRecovered(3);
        metrics.recordRecovered(2);
        metrics.recordResultSaveError();

        var timer = registry.get("task.execution.duration").timer();
        assertThat(timer.count()).isEqualTo(2);
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isEqualTo(2.5);
        assertThat(registry.get("task.recovered").counter().count()).isEqualTo(5);
        assertThat(registry.get("task.result.save.errors").counter().count()).isEqualTo(1);
    }

    @Test
    void countsOnlyTasksReturnedToQueue() {
        TaskRecoveryService recovery = mock(TaskRecoveryService.class);
        when(recovery.recoverExpiredTasks())
            .thenReturn(List.of(Map.of("status", "NEW"), Map.of("status", "FAILED")))
            .thenThrow(new DataAccessResourceFailureException("Commit failed"));
        TaskService service = new TaskService(
            mock(ThreadPoolExecutor.class), mock(TaskExecutionService.class),
            mock(TaskStatusService.class), recovery, mock(TaskCleanupService.class),
            mock(KafkaListenerEndpointRegistry.class), metrics
        );

        service.recoverExpiredTasks();
        assertThat(registry.get("task.recovered").counter().count()).isEqualTo(1);

        service.recoverExpiredTasks();
        assertThat(registry.get("task.recovered").counter().count()).isEqualTo(1);
    }
}
