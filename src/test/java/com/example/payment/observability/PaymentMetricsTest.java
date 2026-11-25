package com.example.payment.observability;

import com.example.payment.observability.PaymentMetricsRepository.QueueStats;
import com.example.payment.domain.PaymentRepository.RecoveryCounts;
import com.example.payment.worker.WorkerLifecycle;
import com.example.payment.maintenance.MaintenanceScheduler;
import com.example.payment.maintenance.PaymentCleanupService;
import com.example.payment.maintenance.PaymentRecoveryService;
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
class PaymentMetricsTest {

    @Mock
    private PaymentMetricsRepository metricsRepository;

    private SimpleMeterRegistry registry;
    private PaymentMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new PaymentMetrics(registry, metricsRepository);
    }

    @AfterEach
    void tearDown() {
        registry.close();
    }

    @Test
    void readsQueue() {
        assertThat(registry.get("payment.queue.new").gauge().value()).isNaN();
        when(metricsRepository.readQueueStats())
            .thenReturn(new QueueStats(4, 30));

        metrics.updateQueueMetrics();

        assertThat(registry.get("payment.queue.new").gauge().value()).isEqualTo(4);
        assertThat(registry.get("payment.queue.oldest.ready.age").timeGauge()
            .value(TimeUnit.SECONDS)).isEqualTo(30);
        verify(metricsRepository).readQueueStats();
        verifyNoMoreInteractions(metricsRepository);
    }

    @Test
    void clearsStaleQueueValues() {
        when(metricsRepository.readQueueStats())
            .thenReturn(new QueueStats(4, 30))
            .thenThrow(new DataAccessResourceFailureException("Database is not available"));
        metrics.updateQueueMetrics();

        metrics.updateQueueMetrics();

        assertThat(registry.get("payment.queue.new").gauge().value()).isNaN();
        assertThat(registry.get("payment.queue.oldest.ready.age").timeGauge()
            .value(TimeUnit.SECONDS)).isNaN();
    }

    @Test
    void recordsAttemptMetrics() {
        metrics.recordExecution(TimeUnit.SECONDS.toNanos(2));
        metrics.recordExecution(TimeUnit.MILLISECONDS.toNanos(500));
        metrics.recordLeasesRecovered(3);
        metrics.recordLeasesRecovered(2);
        metrics.recordResultSaveError();

        var timer = registry.get("payment.execution.duration").timer();
        assertThat(timer.count()).isEqualTo(2);
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isEqualTo(2.5);
        assertThat(registry.get("payment.lease.recovered").counter().count()).isEqualTo(5);
        assertThat(registry.get("payment.result.save.errors").counter().count()).isEqualTo(1);
    }

    @Test
    void countsCompletedAndFailedTransitionsSeparately() {
        assertThat(registry.get("payment.execution.completed").counter().count()).isZero();
        assertThat(registry.get("payment.execution.failed").counter().count()).isZero();

        metrics.recordExecutionCompleted();
        metrics.recordExecutionCompleted();
        metrics.recordExecutionFailed(3);
        metrics.recordExecutionFailed(1);
        metrics.recordExecutionFailed(0);

        assertThat(registry.get("payment.execution.completed").counter().count()).isEqualTo(2);
        assertThat(registry.get("payment.execution.failed").counter().count()).isEqualTo(4);
        assertThat(registry.get("payment.lease.recovered").counter().count()).isZero();
    }

    @Test
    void countsRecoveryRetriesAndTerminalFailuresOnlyAfterSuccessfulReturn() {
        PaymentRecoveryService recovery = mock(PaymentRecoveryService.class);
        when(recovery.recoverExpiredLeases())
            .thenReturn(new RecoveryCounts(1, 2))
            .thenThrow(new DataAccessResourceFailureException("Commit failed"));
        MaintenanceScheduler scheduler = new MaintenanceScheduler(
            mock(WorkerLifecycle.class), recovery, mock(PaymentCleanupService.class), metrics
        );

        scheduler.recoverExpiredLeases();
        assertThat(registry.get("payment.lease.recovered").counter().count()).isEqualTo(1);
        assertThat(registry.get("payment.execution.failed").counter().count()).isEqualTo(2);
        assertThat(registry.get("payment.execution.completed").counter().count()).isZero();

        scheduler.recoverExpiredLeases();
        assertThat(registry.get("payment.lease.recovered").counter().count()).isEqualTo(1);
        assertThat(registry.get("payment.execution.failed").counter().count()).isEqualTo(2);
        assertThat(registry.get("payment.execution.completed").counter().count()).isZero();
    }
}
