package com.example.taskapplication.repository;

import com.example.taskapplication.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {

    @Query(value = """
        WITH picked AS (
            SELECT id
            FROM tasks
            WHERE status = 'NEW'
              AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
              AND attempts < max_attempts
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE tasks t
        SET status = 'IN_PROGRESS',
            execution_id = :executionId,
            lease_until = CURRENT_TIMESTAMP + (:leaseSeconds * INTERVAL '1 second'),
            next_retry_at = NULL,
            attempts = t.attempts + 1,
            started_at = CURRENT_TIMESTAMP,
            finished_at = NULL,
            progress = 0,
            result = NULL,
            version = t.version + 1
        FROM picked
        WHERE t.id = picked.id
        RETURNING t.id, t.event_id AS "eventId", t.duration
        """, nativeQuery = true)
    Optional<ClaimedTask> claimNextExecution(
        @Param("executionId") UUID executionId,
        @Param("leaseSeconds") int leaseSeconds
    );

    interface ClaimedTask {
        Long getId();
        UUID getEventId();
        long getDuration();
    }

    Optional<Task> findByEventId(UUID eventId);

    @Query(value = """
        SELECT *
        FROM tasks
        WHERE id = :id
        FOR NO KEY UPDATE
        """, nativeQuery = true)
    Optional<Task> findByIdForUpdate(@Param("id") Long id);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        INSERT INTO tasks (event_id, name, duration, owner_username)
        VALUES (:eventId, :name, :duration, :ownerUsername)
        ON CONFLICT (event_id) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(
        @Param("eventId") UUID eventId,
        @Param("name") String name,
        @Param("duration") long duration,
        @Param("ownerUsername") String ownerUsername
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE tasks
        SET progress = :progress,
            lease_until = CURRENT_TIMESTAMP + (:leaseSeconds * INTERVAL '1 second'),
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND status = 'IN_PROGRESS'
          AND lease_until > CURRENT_TIMESTAMP
          AND progress <= :progress
        """, nativeQuery = true)
    int updateProgress(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("progress") int progress,
        @Param("leaseSeconds") int leaseSeconds
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE tasks
        SET status = 'NEW',
            execution_id = NULL,
            lease_until = NULL,
            next_retry_at = NULL,
            attempts = GREATEST(attempts - 1, 0),
            started_at = NULL,
            finished_at = NULL,
            progress = 0,
            result = NULL,
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND status = 'IN_PROGRESS'
          AND lease_until > CURRENT_TIMESTAMP
        """, nativeQuery = true)
    int requeueInterruptedExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId
    );

    @Query(value = """
        WITH updated AS (
            UPDATE tasks
            SET status = CASE
                    WHEN attempts < max_attempts THEN 'NEW'
                    ELSE 'FAILED'
                END,
                execution_id = CASE
                    WHEN attempts < max_attempts THEN NULL
                    ELSE execution_id
                END,
                lease_until = NULL,
                next_retry_at = CASE
                    WHEN attempts < max_attempts
                        THEN CURRENT_TIMESTAMP + (:retryBackoffSeconds * INTERVAL '1 second')
                    ELSE NULL
                END,
                started_at = CASE
                    WHEN attempts < max_attempts THEN NULL
                    ELSE started_at
                END,
                finished_at = CASE
                    WHEN attempts >= max_attempts THEN CURRENT_TIMESTAMP
                    ELSE NULL
                END,
                progress = CASE
                    WHEN attempts < max_attempts THEN 0
                    ELSE :progress
                END,
                result = :result,
                version = version + 1
            WHERE id = :id
              AND execution_id = :executionId
              AND status = 'IN_PROGRESS'
              AND lease_until > CURRENT_TIMESTAMP
            RETURNING status
        )
        SELECT status FROM updated
        """, nativeQuery = true)
    Optional<String> failExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("progress") int progress,
        @Param("result") String result,
        @Param("retryBackoffSeconds") int retryBackoffSeconds
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE tasks
        SET status = 'COMPLETED',
            progress = 100,
            result = :result,
            finished_at = :finishedAt,
            lease_until = NULL,
            next_retry_at = NULL,
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND status = 'IN_PROGRESS'
          AND lease_until > CURRENT_TIMESTAMP
        """, nativeQuery = true)
    int completeExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("result") String result,
        @Param("finishedAt") Instant finishedAt
    );

    @Query(value = """
        SELECT
            COUNT(*) AS new_tasks,
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
        FROM tasks
        WHERE status = 'NEW'
        """, nativeQuery = true)
    QueueStats readQueueStats();

    record QueueStats(long newTasks, double oldestReadyAgeSeconds) {
    }

    @Query(value = """
        WITH expired AS (
            SELECT id
            FROM tasks
            WHERE status = 'IN_PROGRESS'
              AND lease_until < CURRENT_TIMESTAMP
            ORDER BY lease_until, id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
        ), updated AS (
            UPDATE tasks t
            SET status = CASE
                    WHEN t.attempts < t.max_attempts THEN 'NEW'
                    ELSE 'FAILED'
                END,
                execution_id = NULL,
                lease_until = NULL,
                next_retry_at = CASE
                    WHEN t.attempts < t.max_attempts
                        THEN CURRENT_TIMESTAMP + (:retryBackoffSeconds * INTERVAL '1 second')
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
            RETURNING t.status
        )
        SELECT COUNT(*) FILTER (WHERE status = 'NEW') AS requeued,
               COUNT(*) FILTER (WHERE status = 'FAILED') AS failed
        FROM updated
        """, nativeQuery = true)
    RecoveryCounts recoverExpiredExecutions(@Param("retryBackoffSeconds") int retryBackoffSeconds);

    record RecoveryCounts(long requeued, long failed) {
    }

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        WITH batch AS (
            SELECT id
            FROM tasks
            WHERE status = :status
              AND status IN ('COMPLETED', 'FAILED')
              AND finished_at < :cutoff
            ORDER BY finished_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM tasks t
        USING batch b
        WHERE t.id = b.id
          AND t.status = :status
          AND t.status IN ('COMPLETED', 'FAILED')
          AND t.finished_at < :cutoff
        """, nativeQuery = true)
    int deleteFinishedBefore(
        @Param("status") String status,
        @Param("cutoff") Instant cutoff,
        @Param("batchSize") int batchSize
    );
}
