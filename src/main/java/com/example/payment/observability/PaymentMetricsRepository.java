package com.example.payment.observability;

import com.example.payment.domain.Payment;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;

import java.util.Optional;
import java.util.Set;

public interface PaymentMetricsRepository extends Repository<Payment, Long> {

    @Query(value = """
        SELECT
            COUNT(*) AS new_payments,
            CAST(COALESCE(
                EXTRACT(EPOCH FROM (
                    CURRENT_TIMESTAMP -
                    MIN(created_at) FILTER (
                        WHERE attempts < max_attempts
                          AND (
                              next_retry_at IS NULL
                              OR next_retry_at <= CURRENT_TIMESTAMP
                          )
                    )
                )),
                0
            ) AS double precision) AS oldest_ready_age_seconds
        FROM payments
        WHERE execution_status = 'NEW'
        """, nativeQuery = true)
    QueueStats readQueueStats();

    record QueueStats(long newPayments, double oldestReadyAgeSeconds) {
    }

    @Query(value = """
        WITH durations AS (
            SELECT 'WEBHOOK' AS outcome, EXTRACT(EPOCH FROM processed_at - received_at) AS seconds
            FROM stripe_webhook_events WHERE processed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
            UNION ALL
            SELECT payment_status, EXTRACT(EPOCH FROM first_terminal_at - created_at)
            FROM payments WHERE first_terminal_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        )
        SELECT COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds) FILTER (WHERE outcome = 'WEBHOOK'),
                        CAST('NaN' AS double precision)) AS webhook_seconds,
               COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds) FILTER (WHERE outcome = 'SUCCEEDED'),
                        CAST('NaN' AS double precision)) AS succeeded_seconds,
               COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY seconds) FILTER (WHERE outcome = 'CANCELED'),
                        CAST('NaN' AS double precision)) AS canceled_seconds
        FROM durations
        """, nativeQuery = true)
    LatencyP95 readLatencyP95();

    record LatencyP95(double webhookSeconds, double succeededSeconds, double canceledSeconds) {}

    @Query(value = """
        SELECT COUNT(*) AS payment_count,
               CAST(COALESCE(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MIN(finished_at))), 0)
                   AS double precision) AS oldest_age_seconds
        FROM payments
        WHERE execution_status = 'FAILED' AND payment_status = 'UNKNOWN'
        """, nativeQuery = true)
    FailedUnknownStats readFailedUnknownStats();

    record FailedUnknownStats(long paymentCount, double oldestAgeSeconds) {
    }

    @Query(value = """
        SELECT id FROM payments
        WHERE payment_status = 'PROCESSING' AND processing_since <= CURRENT_TIMESTAMP - INTERVAL '1 hour'
        """, nativeQuery = true)
    Set<Long> findLongProcessingPaymentIds();

    @Query(value = """
        WITH scheduled_checks AS (
            SELECT id, next_status_check_at AS due_at FROM payments
            WHERE execution_status IN ('COMPLETED', 'FAILED') AND provider_payment_id IS NOT NULL
              AND (payment_status IN ('PROCESSING', 'REQUIRES_ACTION', 'AWAITING_WEBHOOK')
                   OR (abandonment_started AND payment_status NOT IN ('SUCCEEDED', 'CANCELED')))
              AND next_status_check_at IS NOT NULL
            UNION ALL
            SELECT id, GREATEST(finished_at + INTERVAL '1 hour', next_status_check_at) AS due_at
            FROM payments WHERE execution_status = 'FAILED' AND payment_status = 'UNKNOWN'
        )
        SELECT id FROM scheduled_checks WHERE due_at <= CURRENT_TIMESTAMP - INTERVAL '30 minutes'
        ORDER BY due_at, id LIMIT 1
        """, nativeQuery = true)
    Optional<Long> findOldestOverdueCheck();
}
