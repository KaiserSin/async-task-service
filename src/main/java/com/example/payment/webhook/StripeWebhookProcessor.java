package com.example.payment.webhook;

import com.example.payment.database.DatabaseLogFilter;
import com.example.payment.worker.WorkerProperties;
import com.example.payment.observability.PaymentMetrics;
import com.example.payment.webhook.StripeWebhookEventRepository.PendingEvent;
import com.example.payment.stripe.StripeGateway;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
@RequiredArgsConstructor
public class StripeWebhookProcessor {

    private final StripeWebhookEventRepository events;
    private final StripeGateway stripe;
    private final TransactionTemplate workerTransactionTemplate;
    private final WorkerProperties workerProperties;
    private final PaymentMetrics metrics;

    @Scheduled(fixedDelayString = "${worker.poll-delay-ms:1000}")
    public void processNextEvent() {
        PendingEvent event = workerTransactionTemplate.execute(tx -> {
            events.handleNextArchivedOrInvalidEvent();
            return events.claimNextPaymentIntentEvent(workerProperties.leaseSeconds()).orElse(null);
        });
        if (event == null) {
            return;
        }
        DatabaseLogFilter.setBackgroundOperation(event.operationId());
        if (event.retry()) {
            metrics.recordWebhookProcessingRetry();
        }
        try {
            var status = event.needsStripeLookup()
                ? stripe.retrieveStatus(event.operationId(), event.intentId()) : event.proposedStatus();
            workerTransactionTemplate.executeWithoutResult(tx -> events.finishEvent(event, status));
        } catch (RuntimeException failure) {
            workerTransactionTemplate.executeWithoutResult(tx ->
                events.recordTechnicalFailure(event, workerProperties.retryBackoffSeconds()));
            throw failure;
        }
    }
}
