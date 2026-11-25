package com.example.payment.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

    @Query(value = """
        WITH picked AS (
            SELECT id, worker_attempted AS retry,
                   (payment_status IN ('NOT_SUBMITTED', 'REQUEST_REJECTED')
                        AND provider_payment_id IS NULL) AS create_intent,
                   CASE WHEN payment_status = 'REQUIRES_PAYMENT_METHOD' AND provider_payment_id IS NOT NULL
                        THEN pending_confirmation_key END AS confirmation_key
            FROM payments
            WHERE execution_status = 'NEW'
              AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
              AND attempts < max_attempts
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE payments t
        SET execution_status = 'IN_PROGRESS', worker_attempted = TRUE,
            execution_id = :executionId,
            lease_until = CURRENT_TIMESTAMP + (:leaseSeconds * INTERVAL '1 second'),
            next_retry_at = NULL,
            attempts = t.attempts + 1,
            started_at = CURRENT_TIMESTAMP,
            finished_at = NULL,
            result = NULL,
            version = t.version + 1
        FROM picked
        WHERE t.id = picked.id
        RETURNING t.id, t.event_id AS "eventId", t.payment_operation_id AS "paymentOperationId",
                  t.order_id AS "orderId", t.amount, t.currency,
                  COALESCE(t.replacement_payment_method_id, t.payment_method_id) AS "paymentMethodId",
                  t.idempotency_key AS "idempotencyKey", picked.confirmation_key AS "confirmationKey",
                  picked.create_intent AS "createIntent", t.provider_payment_id AS "providerPaymentId",
                  t.version AS "claimVersion", picked.retry AS "retry"
        """, nativeQuery = true)
    Optional<ClaimedPayment> claimNextExecution(
        @Param("executionId") UUID executionId,
        @Param("leaseSeconds") int leaseSeconds
    );

    interface ClaimedPayment {
        Long getId();
        UUID getEventId();
        UUID getPaymentOperationId();
        UUID getOrderId();
        Long getAmount();
        String getCurrency();
        String getPaymentMethodId();
        String getIdempotencyKey();
        String getConfirmationKey();
        boolean getCreateIntent();
        String getProviderPaymentId();
        long getClaimVersion();
        boolean getRetry();
    }

    record PendingIntent(String intentId, long version, String paymentStatus, UUID operationId,
                         boolean abandonmentStarted) {}

    @Transactional
    @Query(value = """
        WITH due AS (
            SELECT id FROM payments
            WHERE execution_status IN ('COMPLETED', 'FAILED')
              AND (payment_status IN ('PROCESSING', 'REQUIRES_ACTION', 'AWAITING_WEBHOOK')
                   OR (abandonment_started AND payment_status NOT IN ('SUCCEEDED', 'CANCELED')))
              AND provider_payment_id IS NOT NULL
              AND next_status_check_at <= CURRENT_TIMESTAMP
            ORDER BY next_status_check_at, id
            LIMIT 1 FOR UPDATE SKIP LOCKED
        ), reserved AS (
            UPDATE payments p
            SET next_status_check_at = CURRENT_TIMESTAMP + CASE WHEN p.payment_status = 'REQUIRES_ACTION' AND NOT p.abandonment_started
                    THEN LEAST(INTERVAL '2 hours', GREATEST(INTERVAL '1 minute', CURRENT_TIMESTAMP - p.action_expires_at))
                    ELSE :intervalMinutes * INTERVAL '1 minute' END,
                version = p.version + 1
            FROM due WHERE p.id = due.id
            RETURNING p.provider_payment_id, p.version, p.payment_status, p.payment_operation_id, p.abandonment_started
        )
        SELECT provider_payment_id, version, payment_status, payment_operation_id, abandonment_started FROM reserved
        """, nativeQuery = true)
    Optional<PendingIntent> reservePendingIntent(@Param("intervalMinutes") int intervalMinutes);

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Modifying
    @Query(value = """
        UPDATE payments SET abandonment_started = TRUE, version = version + 1
        WHERE provider_payment_id = :intentId AND version = :version
          AND execution_status IN ('COMPLETED', 'FAILED')
          AND payment_status NOT IN ('SUCCEEDED', 'CANCELED')
          AND (abandonment_started OR (payment_status = 'REQUIRES_ACTION'
                                       AND action_expires_at <= CURRENT_TIMESTAMP))
        """, nativeQuery = true)
    int beginAbandonment(@Param("intentId") String intentId, @Param("version") long version);

    @Transactional
    @Modifying
    @Query(value = """
        UPDATE payments SET payment_status = :status, version = version + 1
        WHERE provider_payment_id = :intentId AND version = :version
        """, nativeQuery = true)
    int updateReconciledStatus(@Param("intentId") String intentId, @Param("version") long version,
                               @Param("status") String status);

    record UnknownPayment(UUID operationId, UUID orderId, long amount, String currency, String intentId, long version) {}

    @Transactional
    @Query(value = """
        WITH due AS (
            SELECT id FROM payments
            WHERE execution_status = 'FAILED' AND payment_status = 'UNKNOWN'
              AND finished_at <= CURRENT_TIMESTAMP - INTERVAL '1 hour'
              AND (next_status_check_at IS NULL OR next_status_check_at <= CURRENT_TIMESTAMP)
            ORDER BY COALESCE(next_status_check_at, finished_at), id
            LIMIT 1 FOR UPDATE SKIP LOCKED
        ), reserved AS (
            UPDATE payments p SET next_status_check_at = CURRENT_TIMESTAMP + INTERVAL '1 hour',
                                  version = p.version + 1
            FROM due WHERE p.id = due.id
            RETURNING p.payment_operation_id, p.order_id, p.amount, p.currency, p.provider_payment_id, p.version
        )
        SELECT payment_operation_id, order_id, amount, currency, provider_payment_id, version FROM reserved
        """, nativeQuery = true)
    Optional<UnknownPayment> reserveFailedUnknownPayment();

    @Transactional
    @Modifying
    @Query(value = """
        UPDATE payments SET provider_payment_id = :intentId, payment_status = :status, version = version + 1
        WHERE payment_operation_id = :operationId AND version = :version
          AND execution_status = 'FAILED' AND payment_status = 'UNKNOWN'
          AND (provider_payment_id IS NULL OR provider_payment_id = :intentId)
        """, nativeQuery = true)
    int resolveFailedUnknownPayment(@Param("operationId") UUID operationId, @Param("version") long version,
                                    @Param("intentId") String intentId, @Param("status") String status);

    @Query(value = "SELECT 1 FROM pg_advisory_xact_lock(hashtextextended(:ownerUsername, 0))", nativeQuery = true)
    int lockOwnerAdmission(@Param("ownerUsername") String ownerUsername);

    @Query(value = """
        SELECT COUNT(*) FROM payments WHERE owner_username = :ownerUsername
           AND created_at >= statement_timestamp() - INTERVAL '1 minute'
        """, nativeQuery = true)
    long countRecentByOwnerUsername(@Param("ownerUsername") String ownerUsername);

    @Modifying
    @Query(value = """
        INSERT INTO payment_registration_events (event_id, payment_operation_id) VALUES (:eventId, :operationId)
        ON CONFLICT (event_id) DO NOTHING
        """, nativeQuery = true)
    int recordRegistrationEvent(@Param("eventId") UUID eventId, @Param("operationId") UUID operationId);

    @Query(value = "SELECT payment_operation_id FROM payment_registration_events WHERE event_id = :eventId",
           nativeQuery = true)
    UUID registeredOperationForEvent(@Param("eventId") UUID eventId);

    Optional<Payment> findByEventId(UUID eventId);

    Optional<Payment> findByPaymentOperationId(UUID paymentOperationId);

    interface ArchivedRegistration extends PaymentRegistrationDetails {}

    @Query(value = """
        SELECT payment_operation_id AS "paymentOperationId", order_id AS "orderId",
               owner_username AS "ownerUsername", amount, currency, payment_method_id AS "paymentMethodId"
        FROM archived_payments WHERE event_id = :eventId OR payment_operation_id = :operationId
        ORDER BY (event_id = :eventId) DESC LIMIT 1
        """, nativeQuery = true)
    Optional<ArchivedRegistration> findArchivedRegistration(@Param("eventId") UUID eventId,
                                                            @Param("operationId") UUID operationId);

    @Modifying
    @Query(value = """
        DELETE FROM payments WHERE event_id = :eventId AND payment_operation_id = :operationId
          AND execution_status = 'NEW' AND attempts = 0
        """, nativeQuery = true)
    int discardUncommittedRegistration(@Param("eventId") UUID eventId, @Param("operationId") UUID operationId);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE payments
        SET payment_status = 'UNKNOWN', pending_confirmation_key = NULL,
            action_expires_at = NULL, version = version + 1
        WHERE id = :id AND execution_id = :executionId
          AND execution_status = 'IN_PROGRESS' AND lease_until > clock_timestamp()
          AND ((:createIntent AND provider_payment_id IS NULL
                AND payment_status IN ('NOT_SUBMITTED', 'REQUEST_REJECTED'))
               OR (CAST(:confirmationKey AS text) IS NOT NULL AND provider_payment_id IS NOT NULL
                   AND payment_status = 'REQUIRES_PAYMENT_METHOD'
                   AND pending_confirmation_key = :confirmationKey))
        """, nativeQuery = true)
    int markStripeSubmission(@Param("id") Long id, @Param("executionId") UUID executionId,
                             @Param("createIntent") boolean createIntent,
                             @Param("confirmationKey") String confirmationKey);

    @Modifying
    @Query(value = """
        UPDATE payments
        SET provider_payment_id = :intentId,
            payment_status = CASE
                WHEN payment_status IN ('SUCCEEDED', 'CANCELED', 'AWAITING_WEBHOOK') THEN payment_status
                WHEN :status IN ('SUCCEEDED', 'CANCELED') THEN 'AWAITING_WEBHOOK'
                WHEN payment_status = 'UNKNOWN' OR version = :claimVersion THEN :status
                ELSE payment_status
            END,
            version = version + 1
        WHERE id = :id AND execution_id = :executionId
          AND execution_status = 'IN_PROGRESS' AND lease_until > CURRENT_TIMESTAMP
          AND (provider_payment_id IS NULL OR provider_payment_id = :intentId)
        """, nativeQuery = true)
    int saveStripeResult(@Param("id") Long id, @Param("executionId") UUID executionId,
                         @Param("intentId") String intentId, @Param("status") String status,
                         @Param("claimVersion") long claimVersion);

    @Query(value = """
        SELECT *
        FROM payments
        WHERE id = :id
        FOR NO KEY UPDATE
        """, nativeQuery = true)
    Optional<Payment> findByIdForUpdate(@Param("id") Long id);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        INSERT INTO payments (event_id, owner_username, payment_operation_id, order_id,
                              amount, currency, payment_method_id, idempotency_key, created_at)
        SELECT :eventId, :ownerUsername, :paymentOperationId, :orderId,
               :amount, :currency, :paymentMethodId, :idempotencyKey, statement_timestamp()
        WHERE NOT EXISTS (SELECT 1 FROM archived_payments
                          WHERE event_id = :eventId OR payment_operation_id = :paymentOperationId)
        ON CONFLICT DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(
        @Param("eventId") UUID eventId,
        @Param("ownerUsername") String ownerUsername,
        @Param("paymentOperationId") UUID paymentOperationId,
        @Param("orderId") UUID orderId,
        @Param("amount") long amount,
        @Param("currency") String currency,
        @Param("paymentMethodId") String paymentMethodId,
        @Param("idempotencyKey") String idempotencyKey
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE payments
        SET lease_until = CURRENT_TIMESTAMP + (:leaseSeconds * INTERVAL '1 second'),
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND execution_status = 'IN_PROGRESS'
          AND lease_until > CURRENT_TIMESTAMP
        """, nativeQuery = true)
    int renewLease(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("leaseSeconds") int leaseSeconds
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE payments
        SET execution_status = 'NEW',
            execution_id = NULL,
            lease_until = NULL,
            next_retry_at = NULL,
            attempts = GREATEST(attempts - 1, 0),
            started_at = NULL,
            finished_at = NULL,
            result = NULL,
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND execution_status = 'IN_PROGRESS'
          AND lease_until > CURRENT_TIMESTAMP
        """, nativeQuery = true)
    int requeueInterruptedExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId
    );

    @Query(value = """
        WITH updated AS (
            UPDATE payments
            SET execution_status = CASE
                    WHEN :allowRetry AND attempts < max_attempts THEN 'NEW'
                    ELSE 'FAILED'
                END,
                payment_status = CASE
                    WHEN :createRejected AND payment_status = 'UNKNOWN' AND provider_payment_id IS NULL
                        THEN 'REQUEST_REJECTED'
                    WHEN CAST(:confirmationKey AS text) IS NOT NULL AND payment_status = 'UNKNOWN'
                        THEN 'REQUIRES_PAYMENT_METHOD'
                    ELSE payment_status
                END,
                pending_confirmation_key = CASE WHEN payment_status = 'UNKNOWN' THEN :confirmationKey
                                                ELSE pending_confirmation_key END,
                execution_id = CASE
                    WHEN :allowRetry AND attempts < max_attempts THEN NULL
                    ELSE execution_id
                END,
                lease_until = NULL,
                next_retry_at = CASE
                    WHEN :allowRetry AND attempts < max_attempts
                        THEN CURRENT_TIMESTAMP + (:retryBackoffSeconds * INTERVAL '1 second')
                             * CASE WHEN attempts <= 1 THEN 1 ELSE 2 END
                             * (1 + random() / 2)
                    ELSE NULL
                END,
                started_at = CASE
                    WHEN :allowRetry AND attempts < max_attempts THEN NULL
                    ELSE started_at
                END,
                finished_at = CASE
                    WHEN NOT :allowRetry OR attempts >= max_attempts THEN CURRENT_TIMESTAMP
                    ELSE NULL
                END,
                result = :result,
                version = version + 1
            WHERE id = :id
              AND execution_id = :executionId
              AND execution_status = 'IN_PROGRESS'
              AND lease_until > CURRENT_TIMESTAMP
            RETURNING execution_status
        )
        SELECT execution_status FROM updated
        """, nativeQuery = true)
    Optional<String> failExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("result") String result,
        @Param("retryBackoffSeconds") int retryBackoffSeconds,
        @Param("allowRetry") boolean allowRetry,
        @Param("createRejected") boolean createRejected,
        @Param("confirmationKey") String confirmationKey
    );

    default Optional<String> failExecution(Long id, UUID executionId, String result, int retryBackoffSeconds) {
        return failExecution(id, executionId, result, retryBackoffSeconds, true, false, null);
    }

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE payments
        SET execution_status = 'COMPLETED',
            result = :result,
            finished_at = :finishedAt,
            lease_until = NULL,
            next_retry_at = NULL,
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND execution_status = 'IN_PROGRESS'
          AND lease_until > CURRENT_TIMESTAMP
        """, nativeQuery = true)
    int completeExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("result") String result,
        @Param("finishedAt") Instant finishedAt
    );

    @Query(value = """
        WITH expired AS (
            SELECT id
            FROM payments
            WHERE execution_status = 'IN_PROGRESS'
              AND lease_until < CURRENT_TIMESTAMP
            ORDER BY lease_until, id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
        ), updated AS (
            UPDATE payments t
            SET execution_status = CASE
                    WHEN t.attempts < t.max_attempts THEN 'NEW'
                    ELSE 'FAILED'
                END,
                execution_id = NULL,
                lease_until = NULL,
                next_retry_at = CASE
                    WHEN t.attempts < t.max_attempts
                        THEN CURRENT_TIMESTAMP + (:retryBackoffSeconds * INTERVAL '1 second')
                             * CASE WHEN t.attempts <= 1 THEN 1 ELSE 2 END
                             * (1 + random() / 2)
                    ELSE NULL
                END,
                started_at = CASE
                    WHEN t.attempts < t.max_attempts THEN NULL
                    ELSE t.started_at
                END,
                finished_at = CASE
                    WHEN t.attempts >= t.max_attempts THEN CURRENT_TIMESTAMP
                    ELSE NULL
                END,
                result = 'Payment lease expired',
                version = t.version + 1
            FROM expired e
            WHERE t.id = e.id
              AND t.execution_status = 'IN_PROGRESS'
              AND t.lease_until < CURRENT_TIMESTAMP
            RETURNING t.execution_status
        )
        SELECT COUNT(*) FILTER (WHERE execution_status = 'NEW') AS requeued,
               COUNT(*) FILTER (WHERE execution_status = 'FAILED') AS failed
        FROM updated
        """, nativeQuery = true)
    RecoveryCounts recoverExpiredLeases(@Param("retryBackoffSeconds") int retryBackoffSeconds);

    record RecoveryCounts(long requeued, long failed) {
    }

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        WITH batch AS (
            SELECT id, payment_operation_id, event_id, order_id, owner_username, amount, currency,
                   payment_method_id, replacement_payment_method_id, provider_payment_id,
                   created_at, first_terminal_at, payment_status
            FROM payments
            WHERE execution_status IN ('COMPLETED', 'FAILED')
              AND (CASE WHEN payment_status = 'SUCCEEDED' THEN 'COMPLETED' ELSE execution_status END) = :status
              AND payment_status NOT IN ('UNKNOWN', 'REQUIRES_ACTION', 'PROCESSING', 'AWAITING_WEBHOOK')
              AND (NOT abandonment_started OR payment_status IN ('SUCCEEDED', 'CANCELED'))
              AND finished_at < :cutoff
              AND (payment_status <> 'SUCCEEDED' OR first_terminal_at < :cutoff)
            ORDER BY finished_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
        ), archived AS (
            INSERT INTO archived_payments (payment_id, payment_operation_id, event_id, order_id, owner_username,
                                         amount, currency, payment_method_id, replacement_payment_method_id,
                                         provider_payment_id, created_at, paid_at, payment_status)
            SELECT id, payment_operation_id, event_id, order_id, owner_username,
                   amount, currency, payment_method_id, replacement_payment_method_id,
                   provider_payment_id, created_at,
                   CASE WHEN payment_status = 'SUCCEEDED' THEN first_terminal_at END, payment_status
            FROM batch
        )
        DELETE FROM payments p USING batch WHERE p.id = batch.id
        """, nativeQuery = true)
    int archiveFinishedBefore(
        @Param("status") String status,
        @Param("cutoff") Instant cutoff,
        @Param("batchSize") int batchSize
    );

    @Modifying
    @Query(value = """
        DELETE FROM stripe_webhook_events WHERE event_id IN (
            SELECT event_id FROM stripe_webhook_events
            WHERE processed_at <= CURRENT_TIMESTAMP - INTERVAL '35 days'
            ORDER BY processed_at, event_id
            LIMIT :batchSize FOR UPDATE SKIP LOCKED
        )
        """, nativeQuery = true)
    int deleteExpiredWebhookEvents(@Param("batchSize") int batchSize);
}
