package com.example.taskapplication.metrics;

import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.repository.TaskRepository.QueueStats;
import com.example.taskapplication.repository.TaskRepository.RecoveryCounts;
import com.example.taskapplication.service.execution.WorkerLifecycle;
import com.example.taskapplication.service.maintenance.MaintenanceScheduler;
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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskMetricsTest {

    @Mock
    private TaskRepository taskRepository;

    private SimpleMeterRegistry registry;
    private TaskMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new TaskMetrics(registry, taskRepository);
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void readsQueue() {
        assertThat(registry.get("task.queue.new").gauge().value()).isNaN();
        when(taskRepository.readQueueStats())
            .thenReturn(new QueueStats(4, 30));

        metrics.updateQueueMetrics();

        assertThat(registry.get("task.queue.new").gauge().value()).isEqualTo(4);
        assertThat(registry.get("task.queue.oldest.ready.age").timeGauge()
            .value(TimeUnit.SECONDS)).isEqualTo(30);
        verify(taskRepository).readQueueStats();
        verifyNoMoreInteractions(taskRepository);
    }

    @Test
    void clearsStaleQueueValues() {
        when(taskRepository.readQueueStats())
            .thenReturn(new QueueStats(4, 30))
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
    void countsCompletedAndFailedTransitionsSeparately() {
        assertThat(registry.get("task.completed").counter().count()).isZero();
        assertThat(registry.get("task.failed").counter().count()).isZero();

        metrics.recordCompleted();
        metrics.recordCompleted();
        metrics.recordFailed(3);
        metrics.recordFailed(1);
        metrics.recordFailed(0);

        assertThat(registry.get("task.completed").counter().count()).isEqualTo(2);
        assertThat(registry.get("task.failed").counter().count()).isEqualTo(4);
        assertThat(registry.get("task.recovered").counter().count()).isZero();
    }

    @Test
    void countsRecoveryRetriesAndTerminalFailuresOnlyAfterSuccessfulReturn() {
        TaskRecoveryService recovery = mock(TaskRecoveryService.class);
        when(recovery.recoverExpiredTasks())
            .thenReturn(new RecoveryCounts(1, 2))
            .thenThrow(new DataAccessResourceFailureException("Commit failed"));
        MaintenanceScheduler scheduler = new MaintenanceScheduler(
            mock(WorkerLifecycle.class), recovery, mock(TaskCleanupService.class), metrics
        );

        scheduler.recoverExpiredTasks();
        assertThat(registry.get("task.recovered").counter().count()).isEqualTo(1);
        assertThat(registry.get("task.failed").counter().count()).isEqualTo(2);
        assertThat(registry.get("task.completed").counter().count()).isZero();

        scheduler.recoverExpiredTasks();
        assertThat(registry.get("task.recovered").counter().count()).isEqualTo(1);
        assertThat(registry.get("task.failed").counter().count()).isEqualTo(2);
        assertThat(registry.get("task.completed").counter().count()).isZero();
    }
}
