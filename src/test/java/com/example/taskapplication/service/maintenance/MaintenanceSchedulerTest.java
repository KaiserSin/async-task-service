package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.service.execution.WorkerLifecycle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MaintenanceSchedulerTest {

    @Mock
    private WorkerLifecycle lifecycle;

    @Mock
    private TaskRecoveryService recovery;

    @Mock
    private TaskCleanupService cleanup;

    @Mock
    private TaskMetrics metrics;

    private MaintenanceScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new MaintenanceScheduler(lifecycle, recovery, cleanup, metrics);
    }

    @Test
    void shutdownStopsRecoveryAndCleanupBeforeTheyTouchTheDatabase() {
        when(lifecycle.isStopping()).thenReturn(true);

        scheduler.recoverExpiredTasks();
        scheduler.cleanupFinishedTasks();

        verifyNoInteractions(recovery, cleanup, metrics);
    }

    @Test
    void cleanupCannotRunWithoutTheDistributedLock() {
        assertThatIllegalStateException().isThrownBy(scheduler::cleanupFinishedTasks);

        verifyNoInteractions(recovery, cleanup, metrics);
    }
}
