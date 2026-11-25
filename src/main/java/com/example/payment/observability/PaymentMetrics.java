package com.example.payment.observability;

import com.example.payment.observability.PaymentMetricsRepository.LatencyP95;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class PaymentMetrics {

    private final PaymentMetricsRepository metricsRepository;
    private final Timer executionDuration;
    private final Counter recoveredLeases;
    private final Counter completedExecutions;
    private final Counter failedExecutions;
    private final Counter resultSaveErrors;
    private final Counter webhookProcessingRetries;
    private final Counter workerStripeRetryRequests;

    private static final LatencyP95 NO_SAMPLES = new LatencyP95(Double.NaN, Double.NaN, Double.NaN);
    private volatile LatencyP95 latencyP95 = NO_SAMPLES;

    private Set<Long> warnedProcessing = Set.of();
    private Optional<Long> warnedOverdue = Optional.empty();

    private volatile double newPayments = Double.NaN;
    private volatile double oldestReadyAgeSeconds = Double.NaN;
    private volatile double failedUnknownPayments = Double.NaN;
    private volatile double oldestFailedUnknownAgeSeconds = Double.NaN;

    public PaymentMetrics(MeterRegistry registry, PaymentMetricsRepository metricsRepository) {
        this.metricsRepository = metricsRepository;

        Gauge.builder("payment.queue.new", this, metrics -> metrics.newPayments)
            .description("Payments waiting in NEW status")
            .register(registry);

        TimeGauge.builder(
                "payment.queue.oldest.ready.age",
                this,
                TimeUnit.SECONDS,
                metrics -> metrics.oldestReadyAgeSeconds
            )
            .description("Age since creation of the oldest ready payment")
            .register(registry);

        Gauge.builder("payment.failed.unknown", this, metrics -> metrics.failedUnknownPayments)
            .description("Failed executions with an unknown payment outcome")
            .register(registry);

        TimeGauge.builder("payment.failed.unknown.oldest.age", this, TimeUnit.SECONDS,
                metrics -> metrics.oldestFailedUnknownAgeSeconds)
            .description("Time since the oldest unresolved execution failed")
            .register(registry);

        TimeGauge.builder("payment.webhook.processing.p95", this, TimeUnit.SECONDS,
                metrics -> metrics.latencyP95.webhookSeconds())
            .description("95th percentile from webhook receipt to processing over the last hour")
            .register(registry);
        TimeGauge.builder("payment.outcome.p95", this, TimeUnit.SECONDS, metrics -> metrics.latencyP95.succeededSeconds())
            .tag("outcome", "SUCCEEDED")
            .description("95th percentile from registration to first terminal status over the last hour")
            .register(registry);
        TimeGauge.builder("payment.outcome.p95", this, TimeUnit.SECONDS, metrics -> metrics.latencyP95.canceledSeconds())
            .tag("outcome", "CANCELED")
            .description("95th percentile from registration to first terminal status over the last hour")
            .register(registry);

        executionDuration = Timer.builder("payment.execution.duration")
            .description("Payment attempt time")
            .register(registry);

        recoveredLeases = Counter.builder("payment.lease.recovered")
            .description("Payment executions returned to the queue after lease expiry")
            .register(registry);

        completedExecutions = Counter.builder("payment.execution.completed")
            .description("Payment executions transitioned to COMPLETED")
            .register(registry);

        failedExecutions = Counter.builder("payment.execution.failed")
            .description("Payment executions transitioned to FAILED")
            .register(registry);

        webhookProcessingRetries = registry.counter("payment.webhook.processing.retries");
        workerStripeRetryRequests = registry.counter("payment.worker.stripe.retry.requests");

        resultSaveErrors = Counter.builder("payment.result.save.errors")
            .description("Failed final result writes")
            .register(registry);
    }

    @Scheduled(fixedDelayString = "${worker.metrics-delay-ms:60000}")
    public void updateQueueMetrics() {
        try {
            var stats = metricsRepository.readQueueStats();
            newPayments = stats.newPayments();
            oldestReadyAgeSeconds = Math.max(0, stats.oldestReadyAgeSeconds());
        } catch (DataAccessException e) {
            newPayments = Double.NaN;
            oldestReadyAgeSeconds = Double.NaN;
            log.warn("Could not read queue metrics", e);
        }
    }

    @Scheduled(fixedDelayString = "${worker.metrics-delay-ms:60000}")
    public void updateFailedUnknownMetrics() {
        try {
            var stats = metricsRepository.readFailedUnknownStats();
            double previousCount = failedUnknownPayments;
            failedUnknownPayments = stats.paymentCount();
            oldestFailedUnknownAgeSeconds = Math.max(0, stats.oldestAgeSeconds());

            if (stats.paymentCount() > 0 && (Double.isNaN(previousCount) || stats.paymentCount() > previousCount)) {
                log.warn("Payment executions in FAILED with UNKNOWN outcome: count={}, oldestAgeSeconds={}",
                    stats.paymentCount(), (long) oldestFailedUnknownAgeSeconds);
            }
        } catch (DataAccessException e) {
            failedUnknownPayments = Double.NaN;
            oldestFailedUnknownAgeSeconds = Double.NaN;
            log.warn("Could not read FAILED / UNKNOWN payment metrics", e);
        }
    }

    @Scheduled(fixedDelayString = "${worker.metrics-delay-ms:60000}")
    public void checkReconciliationWarnings() {
        var longProcessing = metricsRepository.findLongProcessingPaymentIds();
        for (Long id : longProcessing) {
            if (!warnedProcessing.contains(id)) {
                log.warn("Payment {} has been in PROCESSING for at least one hour", id);
            }
        }
        warnedProcessing = longProcessing;

        var overdue = metricsRepository.findOldestOverdueCheck();
        if (overdue.isPresent() && !overdue.equals(warnedOverdue)) {
            log.warn("The oldest Stripe status check is at least 30 minutes overdue for payment {}", overdue.get());
        }
        warnedOverdue = overdue;
    }

    @Scheduled(fixedDelayString = "${worker.metrics-delay-ms:60000}")
    public void updateLatencyMetrics() {
        try {
            latencyP95 = metricsRepository.readLatencyP95();
        } catch (DataAccessException e) {
            latencyP95 = NO_SAMPLES;
            log.warn("Could not read payment latency metrics", e);
        }
    }

    public void recordExecution(long durationNanos) {
        executionDuration.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordLeasesRecovered(long count) {
        recoveredLeases.increment(count);
    }

    public void recordExecutionCompleted() {
        completedExecutions.increment();
    }

    public void recordExecutionFailed(long count) {
        failedExecutions.increment(count);
    }

    public void recordWebhookProcessingRetry() {
        webhookProcessingRetries.increment();
    }

    public void recordWorkerStripeRetryRequest() {
        workerStripeRetryRequests.increment();
    }

    public void recordResultSaveError() {
        resultSaveErrors.increment();
    }
}
