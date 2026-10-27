package com.example.taskapplication.service.maintenance;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class TaskRecoveryService {

    private final JdbcTemplate jdbcTemplate;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<Map<String, Object>> recoverExpiredTasks() {
        return jdbcTemplate.queryForList("""
            WITH expired AS (
                SELECT id
                FROM tasks
                WHERE status = 'IN_PROGRESS'
                  AND lease_until < CURRENT_TIMESTAMP
                ORDER BY lease_until, id
                LIMIT 100
                FOR UPDATE SKIP LOCKED
            )
            UPDATE tasks t
            SET status = CASE
                    WHEN t.attempts < t.max_attempts THEN 'NEW'
                    ELSE 'FAILED'
                END,
                execution_id = NULL,
                lease_until = NULL,
                next_retry_at = CASE
                    WHEN t.attempts < t.max_attempts
                        THEN CURRENT_TIMESTAMP + INTERVAL '30 seconds'
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
                progress = CASE
                    WHEN t.attempts < t.max_attempts THEN 0
                    ELSE t.progress
                END,
                result = 'Task lease expired',
                version = t.version + 1
            FROM expired e
            WHERE t.id = e.id
              AND t.status = 'IN_PROGRESS'
              AND t.lease_until < CURRENT_TIMESTAMP
            RETURNING t.id, t.status
            """);
    }
}
