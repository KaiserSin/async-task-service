package com.example.payment.worker.attempt;

import java.util.UUID;

public record PaymentAttempt(Long id, UUID eventId, UUID executionId, UUID paymentOperationId,
                         UUID orderId, Long amount, String currency, String paymentMethodId,
                         String idempotencyKey, long leaseDeadlineNanos, boolean createIntent,
                         String providerPaymentId, long claimVersion, String confirmationKey, boolean retry) {

    public PaymentAttempt(Long id, UUID eventId, UUID executionId, UUID paymentOperationId, UUID orderId,
                      Long amount, String currency, String paymentMethodId, String idempotencyKey,
                      long leaseDeadlineNanos) {
        this(id, eventId, executionId, paymentOperationId, orderId, amount, currency, paymentMethodId,
            idempotencyKey, leaseDeadlineNanos, false, null, 0L, null, false);
    }
}
