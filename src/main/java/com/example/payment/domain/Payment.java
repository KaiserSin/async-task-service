package com.example.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Payment implements PaymentRegistrationDetails {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "owner_username", nullable = false)
    private String ownerUsername;

    @Column(name = "payment_operation_id", nullable = false, unique = true, updatable = false)
    private UUID paymentOperationId;

    @Column(name = "order_id", nullable = false, updatable = false)
    private UUID orderId;

    @Column(nullable = false, updatable = false)
    private Long amount;

    @Column(nullable = false, length = 3, updatable = false)
    private String currency;

    @Column(name = "payment_method_id", nullable = false, updatable = false)
    private String paymentMethodId;

    @Column(name = "replacement_payment_method_id")
    private String replacementPaymentMethodId;

    @Column(name = "abandonment_started", insertable = false, updatable = false)
    private boolean abandonmentStarted;

    @Column(name = "pending_confirmation_key")
    private String pendingConfirmationKey;

    @Column(name = "idempotency_key", nullable = false, unique = true, updatable = false)
    private String idempotencyKey;

    @Column(name = "provider_payment_id", unique = true)
    private String providerPaymentId;

    @Enumerated(EnumType.STRING)
    @Column(name = "payment_status", nullable = false, length = 32)
    private PaymentStatus paymentStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "execution_status", nullable = false, length = 20)
    private ExecutionStatus executionStatus;

    @Column(columnDefinition = "text")
    private String result;

    @Column(name = "execution_id")
    private UUID executionId;

    @Column(name = "lease_until")
    private Instant leaseUntil;

    @Column(name = "action_expires_at", insertable = false, updatable = false)
    private Instant actionExpiresAt;

    @Column(name = "next_retry_at")
    private Instant nextRetryAt;

    @Column(name = "worker_attempted", nullable = false)
    private boolean workerAttempted;

    @Column(nullable = false)
    private Integer attempts;

    @Column(name = "max_attempts", nullable = false)
    private Integer maxAttempts;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    @Version
    @Column(nullable = false)
    private Long version;

    public boolean resetForRetry() {
        if (executionStatus != ExecutionStatus.FAILED) {
            return false;
        }

        workerAttempted = true;
        queueExecution();
        return true;
    }

    public String currentPaymentMethodId() {
        return replacementPaymentMethodId == null ? paymentMethodId : replacementPaymentMethodId;
    }

    public boolean changePaymentMethod(String newMethodId, long expectedVersion) {
        if (paymentStatus != PaymentStatus.REQUIRES_PAYMENT_METHOD || providerPaymentId == null
                || (executionStatus != ExecutionStatus.COMPLETED && executionStatus != ExecutionStatus.FAILED)
                || version != expectedVersion || newMethodId.equals(currentPaymentMethodId())) {
            return false;
        }
        workerAttempted = false;
        replacementPaymentMethodId = newMethodId;
        pendingConfirmationKey = "confirm-" + paymentOperationId + "-" + expectedVersion;
        queueExecution();
        return true;
    }

    private void queueExecution() {
        executionStatus = ExecutionStatus.NEW;
        executionId = null;
        leaseUntil = null;
        nextRetryAt = null;
        attempts = 0;
        startedAt = null;
        finishedAt = null;
        result = null;
    }
}
