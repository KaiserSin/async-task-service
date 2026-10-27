package com.example.taskapplication.repository;

import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("""
        SELECT t FROM Task t
        WHERE t.status = :status
          AND (t.nextRetryAt IS NULL OR t.nextRetryAt <= CURRENT_TIMESTAMP)
          AND t.attempts < t.maxAttempts
        ORDER BY t.id
        """)
    List<Task> findAndLockNewTasks(
        @Param("status") TaskStatus status,
        Pageable pageable
    );

    @Query(value = """
        SELECT *
        FROM tasks
        WHERE status = 'NEW'
          AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
          AND attempts < max_attempts
        ORDER BY id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    Optional<Task> findAndLockFirstNewTask();

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE tasks
        SET status = 'IN_PROGRESS',
            execution_id = :executionId,
            lease_until = CURRENT_TIMESTAMP + INTERVAL '30 seconds',
            next_retry_at = NULL,
            attempts = attempts + 1,
            started_at = CURRENT_TIMESTAMP,
            finished_at = NULL,
            progress = 0,
            result = NULL,
            version = version + 1
        WHERE id = :id
          AND status = 'NEW'
          AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP)
          AND attempts < max_attempts
        """, nativeQuery = true)
    int startExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId
    );

    Optional<Task> findByEventId(UUID eventId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM Task t WHERE t.id = :id")
    Optional<Task> findByIdForUpdate(@Param("id") Long id);

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        INSERT INTO tasks (event_id, name, duration)
        VALUES (:eventId, :name, :duration)
        ON CONFLICT (event_id) DO NOTHING
        """, nativeQuery = true)
    int insertIfAbsent(
        @Param("eventId") UUID eventId,
        @Param("name") String name,
        @Param("duration") long duration
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE tasks
        SET progress = :progress,
            lease_until = CURRENT_TIMESTAMP + INTERVAL '30 seconds',
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND status = :runningStatus
          AND lease_until > CURRENT_TIMESTAMP
          AND progress <= :progress
        """, nativeQuery = true)
    int updateProgress(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("runningStatus") String runningStatus,
        @Param("progress") int progress
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(value = """
        UPDATE tasks
        SET status = :newStatus,
            progress = :progress,
            result = :result,
            finished_at = :finishedAt,
            lease_until = NULL,
            next_retry_at = NULL,
            version = version + 1
        WHERE id = :id
          AND execution_id = :executionId
          AND status = :runningStatus
          AND lease_until > CURRENT_TIMESTAMP
        """, nativeQuery = true)
    int finishExecution(
        @Param("id") Long id,
        @Param("executionId") UUID executionId,
        @Param("runningStatus") String runningStatus,
        @Param("newStatus") String newStatus,
        @Param("progress") int progress,
        @Param("result") String result,
        @Param("finishedAt") Instant finishedAt
    );

    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query("""
        DELETE FROM Task t
        WHERE t.status = :status
          AND t.finishedAt < :cutoff
        """)
    int deleteFinishedBefore(
        @Param("status") TaskStatus status,
        @Param("cutoff") Instant cutoff
    );
}
