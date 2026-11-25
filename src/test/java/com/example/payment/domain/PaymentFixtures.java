package com.example.payment.domain;

import java.time.Instant;
import java.util.UUID;

import static org.springframework.test.util.ReflectionTestUtils.setField;

public final class PaymentFixtures {
    private PaymentFixtures() {}

    public static Payment payment(UUID eventId, String ownerUsername, String paymentMethodId, long amount) {
        return payment(eventId, ownerUsername, UUID.randomUUID(), UUID.randomUUID(), amount, "usd", paymentMethodId);
    }

    public static Payment payment(UUID eventId, String ownerUsername, UUID paymentOperationId, UUID orderId,
                                  long amount, String currency, String paymentMethodId) {
        Payment payment = new Payment();
        setField(payment, "eventId", eventId);
        setField(payment, "ownerUsername", ownerUsername);
        setField(payment, "paymentOperationId", paymentOperationId);
        setField(payment, "orderId", orderId);
        setField(payment, "amount", amount);
        setField(payment, "currency", currency);
        setField(payment, "paymentMethodId", paymentMethodId);
        setField(payment, "idempotencyKey", paymentOperationId.toString());
        setField(payment, "executionStatus", ExecutionStatus.NEW);
        setField(payment, "paymentStatus", PaymentStatus.NOT_SUBMITTED);
        setField(payment, "attempts", 0);
        setField(payment, "maxAttempts", 3);
        setField(payment, "createdAt", Instant.parse("2026-01-01T10:00:00Z"));
        return payment;
    }
}
