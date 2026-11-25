package com.example.payment.maintenance;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.worker.WorkerLifecycle;
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
    private final PaymentRecoveryService paymentRecoveryService;
    private final PaymentCleanupService paymentCleanupService;
    private final PaymentMetrics paymentMetrics;

    @Scheduled(fixedDelayString = "${worker.recovery-delay-ms:5000}")
    public void recoverExpiredLeases() {
        if (workerLifecycle.isStopping()) {
            return;
        }

        try {
            var counts = paymentRecoveryService.recoverExpiredLeases();
            paymentMetrics.recordLeasesRecovered(counts.requeued());
            paymentMetrics.recordExecutionFailed(counts.failed());
        } catch (RuntimeException e) {
            log.error("Could not recover payments with expired leases", e);
        }
    }

    @Scheduled(fixedDelayString = "${worker.recovery-delay-ms:5000}")
    public void reconcileOrCancelPendingPayment() {
        if (!workerLifecycle.isStopping()) {
            paymentRecoveryService.reconcileOrCancelPendingPayment();
        }
    }

    @Scheduled(fixedDelay = 60_000)
    public void reconcileFailedUnknownPayment() {
        if (!workerLifecycle.isStopping()) {
            paymentRecoveryService.reconcileFailedUnknownPayment();
        }
    }

    @Scheduled(cron = "${worker.cleanup.cron:0 0 */12 * * *}", zone = "UTC")
    @SchedulerLock(name = "payment-cleanup", lockAtMostFor = "PT10M", lockAtLeastFor = "PT1M")
    public void runCleanup() {
        if (workerLifecycle.isStopping()) {
            return;
        }
        LockAssert.assertLocked();

        try {
            int archived = paymentCleanupService.archiveExpiredPayments();
            if (archived > 0) {
                log.info("Archived {} expired completed or failed payments", archived);
            }
            paymentCleanupService.deleteExpiredWebhookEvents();
        } catch (RuntimeException e) {
            log.error("Could not clean up expired payments or webhook events", e);
        }
    }

}
