package com.example.payment.api;

import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentFixtures;

import java.time.Instant;
import java.util.UUID;

import static org.springframework.test.util.ReflectionTestUtils.setField;

/**
 * Builds payments with a fixed creation time for API tests.
 * A completed attempt also has a result and fixed start and finish times.
 */
public final class ApiFixtures {
    private ApiFixtures() {}

    public static Payment payment(ExecutionStatus executionStatus, String owner) {
        Payment payment = PaymentFixtures.payment(UUID.randomUUID(), owner, "pm_card_visa", 2500L);
        Instant createdAt = Instant.parse("2026-01-01T10:00:00Z");
        setField(payment, "createdAt", createdAt);
        if (executionStatus == ExecutionStatus.COMPLETED) {
            setField(payment, "executionStatus", ExecutionStatus.COMPLETED);
            setField(payment, "result", "Done");
            setField(payment, "executionId", UUID.randomUUID());
            setField(payment, "attempts", 1);
            setField(payment, "startedAt", createdAt.plusSeconds(1));
            setField(payment, "finishedAt", createdAt.plusSeconds(4));
        }
        return payment;
    }
}
