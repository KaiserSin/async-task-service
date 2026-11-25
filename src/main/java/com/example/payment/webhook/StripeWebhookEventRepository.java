package com.example.payment.webhook;

import com.example.payment.domain.PaymentStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class StripeWebhookEventRepository {

    private final JdbcTemplate jdbc;

    public boolean insertIfAbsent(String eventId, String eventType, String payload) {
        return jdbc.update("""
            INSERT INTO stripe_webhook_events (event_id, event_type, payload)
            VALUES (?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """, eventId, eventType, payload) == 1;
    }

    public record PendingEvent(String eventId, long paymentId, String intentId, long version,
                               PaymentStatus currentStatus, PaymentStatus proposedStatus, boolean cardChanged,
                               OffsetDateTime claimedUntil, UUID operationId, boolean retry) {
        public boolean needsStripeLookup() {
            if (currentStatus == proposedStatus || proposedStatus == PaymentStatus.SUCCEEDED
                    || proposedStatus == PaymentStatus.CANCELED) {
                return false;
            }
            return switch (currentStatus) {
                case UNKNOWN -> cardChanged;
                case NOT_SUBMITTED, REQUEST_REJECTED -> false;
                default -> true;
            };
        }
    }

    public void handleNextArchivedOrInvalidEvent() {
        jdbc.update("""
            WITH candidate AS (
                SELECT e.event_id, review.reason
                FROM stripe_webhook_events e
                CROSS JOIN LATERAL (SELECT e.payload::json #> '{data,object}' AS intent) stripe
                CROSS JOIN LATERAL (SELECT intent #>> '{metadata,paymentOperationId}' AS operation_id) metadata
                CROSS JOIN LATERAL (SELECT CASE WHEN pg_input_is_valid(operation_id, 'uuid')
                                                THEN operation_id::uuid END AS operation_uuid) operation
                LEFT JOIN payments p ON p.payment_operation_id = operation_uuid
                LEFT JOIN archived_payments a ON p.id IS NULL AND a.payment_operation_id = operation_uuid
                CROSS JOIN LATERAL (SELECT CASE
                    WHEN COALESCE(operation_id, '') ~ '^[[:space:]]*$' THEN 'MISSING_OPERATION_ID'
                    WHEN operation_uuid IS NULL THEN 'INVALID_OPERATION_ID'
                    WHEN (
                        EXISTS (SELECT 1 FROM payments linked WHERE linked.provider_payment_id = intent ->> 'id'
                                AND linked.payment_operation_id <> operation_uuid)
                        OR EXISTS (SELECT 1 FROM archived_payments linked WHERE linked.provider_payment_id = intent ->> 'id'
                                AND linked.payment_operation_id <> operation_uuid)
                    ) THEN 'OPERATION_MISMATCH'
                    WHEN COALESCE(p.provider_payment_id, a.provider_payment_id) IS NOT NULL
                        AND COALESCE(p.provider_payment_id, a.provider_payment_id) <> intent ->> 'id'
                        THEN 'INTENT_MISMATCH'
                    WHEN COALESCE(p.amount, a.amount)::text IS DISTINCT FROM (intent ->> 'amount')
                         AND (p.id IS NOT NULL OR a.payment_operation_id IS NOT NULL) THEN 'AMOUNT_MISMATCH'
                    WHEN COALESCE(p.order_id, a.order_id)::text IS DISTINCT FROM (intent #>> '{metadata,orderId}')
                         AND (p.id IS NOT NULL OR a.payment_operation_id IS NOT NULL) THEN 'ORDER_MISMATCH'
                    WHEN COALESCE(p.currency, a.currency) IS DISTINCT FROM (intent ->> 'currency')
                         AND (p.id IS NOT NULL OR a.payment_operation_id IS NOT NULL) THEN 'CURRENCY_MISMATCH'
                    END AS reason) review
                WHERE e.processed_at IS NULL AND e.review_reason IS NULL
                  AND e.payload::json ->> 'livemode' = 'false' AND intent ->> 'object' = 'payment_intent'
                  AND (review.reason IS NOT NULL OR (
                      a.provider_payment_id = intent ->> 'id' AND starts_with(a.provider_payment_id, 'pi_')
                      AND (a.payment_status IN ('SUCCEEDED', 'CANCELED') OR upper(intent ->> 'status') = a.payment_status)
                      AND a.order_id::text = intent #>> '{metadata,orderId}'
                      AND a.amount::text = intent ->> 'amount' AND a.currency = intent ->> 'currency'
                      AND starts_with(e.event_type, 'payment_intent.')
                      AND (e.available_at IS NULL OR e.available_at <= CURRENT_TIMESTAMP)
                      AND NOT EXISTS (SELECT 1 FROM payments active WHERE active.provider_payment_id = a.provider_payment_id)
                  ))
                ORDER BY e.received_at, e.event_id
                LIMIT 1 FOR UPDATE OF e SKIP LOCKED
            )
            UPDATE stripe_webhook_events e SET review_reason = candidate.reason, available_at = NULL,
                processed_at = CASE WHEN candidate.reason IS NULL THEN CURRENT_TIMESTAMP END,
                payload = CASE WHEN candidate.reason IS NOT NULL THEN e.payload END
            FROM candidate WHERE e.event_id = candidate.event_id
            """);
    }

    public Optional<PendingEvent> claimNextPaymentIntentEvent(int leaseSeconds) {
        return jdbc.query("""
            WITH picked AS (
                SELECT e.event_id, p.id, p.payment_operation_id, intent ->> 'id' AS intent_id, p.version, p.payment_status AS current_status,
                       p.replacement_payment_method_id IS NOT NULL AS card_changed, e.available_at IS NOT NULL AS retry,
                       CASE WHEN e.event_type = 'payment_intent.created' OR p.payment_status IN ('SUCCEEDED', 'CANCELED')
                            THEN p.payment_status ELSE upper(intent ->> 'status') END AS proposed_status
                FROM stripe_webhook_events e
                CROSS JOIN LATERAL (SELECT e.payload::json #> '{data,object}' AS intent) stripe
                JOIN payments p ON p.provider_payment_id = intent ->> 'id'
                    OR (p.provider_payment_id IS NULL
                        AND NOT EXISTS (SELECT 1 FROM payments linked WHERE linked.provider_payment_id = intent ->> 'id')
                        AND NOT EXISTS (SELECT 1 FROM archived_payments archived WHERE archived.provider_payment_id = intent ->> 'id'))
                WHERE e.processed_at IS NULL AND e.review_reason IS NULL
                  AND starts_with(e.event_type, 'payment_intent.')
                  AND (e.available_at IS NULL OR e.available_at <= CURRENT_TIMESTAMP)
                  AND (p.replacement_payment_method_id IS NULL OR (p.pending_confirmation_key IS NULL AND p.payment_status <> 'UNKNOWN')
                       OR p.execution_status NOT IN ('NEW', 'IN_PROGRESS')
                       OR e.event_type IN ('payment_intent.succeeded', 'payment_intent.canceled'))
                  AND p.payment_operation_id::text = intent #>> '{metadata,paymentOperationId}'
                  AND p.order_id::text = intent #>> '{metadata,orderId}'
                  AND p.amount::text = intent ->> 'amount' AND p.currency = intent ->> 'currency'
                  AND e.payload::json ->> 'livemode' = 'false'
                  AND intent ->> 'object' = 'payment_intent' AND starts_with(intent ->> 'id', 'pi_')
                  AND intent ->> 'status' IN (
                      'requires_payment_method', 'requires_confirmation', 'requires_action',
                      'processing', 'requires_capture', 'canceled', 'succeeded')
                ORDER BY COALESCE(e.available_at, e.received_at), e.event_id
                LIMIT 1 FOR UPDATE OF e, p SKIP LOCKED
            )
            UPDATE stripe_webhook_events e SET available_at = clock_timestamp() + (? * INTERVAL '1 second')
            FROM picked WHERE e.event_id = picked.event_id
            RETURNING picked.*, e.available_at AS claimed_until
            """, (rs, row) -> new PendingEvent(rs.getString("event_id"), rs.getLong("id"),
                rs.getString("intent_id"), rs.getLong("version"), PaymentStatus.valueOf(rs.getString("current_status")),
                PaymentStatus.valueOf(rs.getString("proposed_status")), rs.getBoolean("card_changed"),
                rs.getObject("claimed_until", OffsetDateTime.class), rs.getObject("payment_operation_id", UUID.class),
                rs.getBoolean("retry")), leaseSeconds).stream().findFirst();
    }

    public void recordTechnicalFailure(PendingEvent event, int backoffSeconds) {
        jdbc.update("""
            UPDATE stripe_webhook_events SET technical_failures = technical_failures + 1,
                review_reason = CASE WHEN technical_failures = 2 THEN 'TECHNICAL_ERROR' END,
                available_at = CASE WHEN technical_failures < 2
                    THEN CURRENT_TIMESTAMP + (? * INTERVAL '1 second') END
            WHERE event_id = ? AND available_at = ? AND processed_at IS NULL AND review_reason IS NULL
              AND EXISTS (SELECT 1 FROM payments WHERE id = ? AND version = ?)
            """, backoffSeconds, event.eventId(), event.claimedUntil(), event.paymentId(), event.version());
    }

    public void finishEvent(PendingEvent event, PaymentStatus status) {
        int updated = jdbc.update("""
            UPDATE payments SET provider_payment_id = ?, payment_status = ?, version = version + 1
            WHERE id = ? AND version = ?
              AND EXISTS (SELECT 1 FROM stripe_webhook_events WHERE event_id = ? AND available_at = ?
                          AND processed_at IS NULL AND review_reason IS NULL FOR UPDATE)
            """, event.intentId(), status.name(), event.paymentId(), event.version(), event.eventId(), event.claimedUntil());
        if (updated == 1) {
            jdbc.update("""
                UPDATE stripe_webhook_events SET processed_at = CURRENT_TIMESTAMP, available_at = NULL, payload = NULL
                WHERE event_id = ?
                """, event.eventId());
        }
    }
}
