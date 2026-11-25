package com.example.payment.maintenance;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.worker.WorkerLifecycle;
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
    private PaymentRecoveryService recovery;

    @Mock
    private PaymentCleanupService cleanup;

    @Mock
    private PaymentMetrics metrics;

    private MaintenanceScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new MaintenanceScheduler(lifecycle, recovery, cleanup, metrics);
    }

    @Test
    void shutdownStopsRecoveryAndCleanupBeforeTheyTouchTheDatabase() {
        when(lifecycle.isStopping()).thenReturn(true);

        scheduler.recoverExpiredLeases();
        scheduler.runCleanup();

        verifyNoInteractions(recovery, cleanup, metrics);
    }

    @Test
    void cleanupCannotRunWithoutTheDistributedLock() {
        assertThatIllegalStateException().isThrownBy(scheduler::runCleanup);

        verifyNoInteractions(recovery, cleanup, metrics);
    }
}
