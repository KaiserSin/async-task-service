package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.config.TransactionConfig;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@DataJpaTest(properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "worker.cleanup.batch-size=2",
    "worker.cleanup.retention-days=30",
    "worker.cleanup.failed-retention-days=90"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({TransactionConfig.class, TaskCleanupService.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskCleanupTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @MockitoSpyBean
    private TaskRepository taskRepository;

    @Autowired
    private TaskCleanupService cleanup;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private DataSource dataSource;
    @Autowired
    @Qualifier("cleanupTransactionTemplate")
    private TransactionTemplate batchTransaction;

    @BeforeEach
    void clearTasks() {
        taskRepository.deleteAllInBatch();
    }

    @Test
    @DisplayName("Cleanup applies independent terminal retention periods and leaves active and recent tasks untouched")
    void deletesOldCompletedAndFailedTasksInMultipleBatches() {
        Instant now = Instant.now();
        for (int i = 0; i < 5; i++) {
            insert(TaskStatus.COMPLETED, now.minus(31 + i, ChronoUnit.DAYS));
            insert(TaskStatus.FAILED, now.minus(91 + i, ChronoUnit.DAYS));
        }
        long recentCompleted = insert(TaskStatus.COMPLETED, now.minus(29, ChronoUnit.DAYS));
        long recentFailed = insert(TaskStatus.FAILED, now.minus(89, ChronoUnit.DAYS));
        long failedPastCompletedRetention = insert(TaskStatus.FAILED, now.minus(31, ChronoUnit.DAYS));
        long queued = insert(TaskStatus.NEW, now.minus(100, ChronoUnit.DAYS));
        long running = insert(TaskStatus.IN_PROGRESS, now.minus(100, ChronoUnit.DAYS));

        assertThat(cleanup.deleteExpiredTasks()).isEqualTo(10);

        assertThat(ids()).containsExactlyInAnyOrder(recentCompleted, recentFailed,
            failedPastCompletedRetention, queued, running);
        assertThat(cleanup.deleteExpiredTasks()).isZero();
    }

    @Test
    @DisplayName("A delete batch is limited and removes the oldest eligible terminal rows first")
    void limitsEachBatchAndOrdersItByFinishedAt() {
        Instant cutoff = Instant.parse("2026-10-01T00:00:00Z");
        long newer = insert(TaskStatus.COMPLETED, cutoff.minusSeconds(1));
        long oldest = insert(TaskStatus.COMPLETED, cutoff.minusSeconds(3));
        long middle = insert(TaskStatus.COMPLETED, cutoff.minusSeconds(2));
        long atBoundary = insert(TaskStatus.COMPLETED, cutoff);
        long otherStatus = insert(TaskStatus.FAILED, cutoff.minusSeconds(4));

        assertThat(deleteBatch(TaskStatus.COMPLETED, cutoff)).isEqualTo(2);

        assertThat(ids()).containsExactlyInAnyOrder(newer, atBoundary, otherStatus)
            .doesNotContain(oldest, middle);
        assertThat(deleteBatch(TaskStatus.COMPLETED, cutoff)).isEqualTo(1);
        assertThat(ids()).containsExactlyInAnyOrder(atBoundary, otherStatus);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"NEW", "IN_PROGRESS"})
    @DisplayName("The repository itself refuses to delete active statuses even with an old finished timestamp")
    void refusesActiveStatusesAtRepositoryBoundary(TaskStatus status) {
        long task = insert(status, Instant.now().minus(100, ChronoUnit.DAYS));

        assertThat(deleteBatch(status, Instant.now())).isZero();

        assertThat(ids()).containsExactly(task);
    }

    @Test
    @DisplayName("A locked terminal row is skipped so cleanup does not wait for a concurrent retry")
    void skipsLockedRowsAndDeletesThemAfterTheLockIsReleased() throws Exception {
        Instant cutoff = Instant.now();
        long locked = insert(TaskStatus.FAILED, cutoff.minus(100, ChronoUnit.DAYS));
        long available = insert(TaskStatus.FAILED, cutoff.minus(99, ChronoUnit.DAYS));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("SELECT id FROM tasks WHERE id = ? FOR UPDATE")) {
                statement.setLong(1, locked);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();

                    assertThat(deleteBatch(TaskStatus.FAILED, cutoff)).isEqualTo(1);
                    assertThat(ids()).containsExactly(locked).doesNotContain(available);
                }
            } finally {
                connection.rollback();
            }
        }

        assertThat(cleanup.deleteExpiredTasks()).isEqualTo(1);
        assertThat(ids()).isEmpty();
    }

    @Test
    @DisplayName("An error in a later batch does not roll back a previously committed batch")
    void commitsBatchesIndependently() {
        Instant finishedAt = Instant.now().minus(100, ChronoUnit.DAYS);
        long first = insert(TaskStatus.COMPLETED, finishedAt.minusSeconds(2));
        long second = insert(TaskStatus.COMPLETED, finishedAt.minusSeconds(1));
        long remaining = insert(TaskStatus.COMPLETED, finishedAt);
        long failed = insert(TaskStatus.FAILED, finishedAt);
        doThrow(new DataAccessResourceFailureException("Later cleanup batch failed"))
            .when(taskRepository).deleteFinishedBefore(eq(TaskStatus.FAILED.name()), any(Instant.class), eq(2));

        assertThatThrownBy(cleanup::deleteExpiredTasks)
            .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(ids()).containsExactlyInAnyOrder(remaining, failed).doesNotContain(first, second);
    }

    @Test
    @DisplayName("One cleanup run is bounded even when more eligible work remains")
    void leavesBacklogForTheNextScheduledRunAfterItsBatchBudget() {
        Instant finishedAt = Instant.now().minus(100, ChronoUnit.DAYS);
        for (int i = 0; i < 23; i++) {
            insert(TaskStatus.COMPLETED, finishedAt);
            insert(TaskStatus.FAILED, finishedAt);
        }

        assertThat(cleanup.deleteExpiredTasks()).isEqualTo(40);

        assertThat(ids()).hasSize(6);
        assertThat(cleanup.deleteExpiredTasks()).isEqualTo(6);
        assertThat(ids()).isEmpty();
    }

    @Test
    @DisplayName("An interrupted scheduler thread does not begin another cleanup transaction")
    void doesNotStartCleanupWhenInterrupted() {
        Thread.currentThread().interrupt();
        try {
            assertThat(cleanup.deleteExpiredTasks()).isZero();
        } finally {
            Thread.interrupted();
        }

        verify(taskRepository, never()).deleteFinishedBefore(any(String.class), any(Instant.class), eq(2));
    }

    private int deleteBatch(TaskStatus status, Instant cutoff) {
        return batchTransaction.execute(transaction -> taskRepository.deleteFinishedBefore(status.name(), cutoff, 2));
    }

    private long insert(TaskStatus status, Instant finishedAt) {
        boolean running = status == TaskStatus.IN_PROGRESS;
        return jdbc.queryForObject("""
            INSERT INTO tasks (event_id, name, duration, status, progress, execution_id, started_at, finished_at)
            VALUES (?, 'Cleanup test', 1000, ?, 0, ?, ?, ?)
            RETURNING id
            """, Long.class, UUID.randomUUID(), status.name(), running ? UUID.randomUUID() : null,
            running ? Timestamp.from(Instant.now()) : null, Timestamp.from(finishedAt));
    }

    private List<Long> ids() {
        return jdbc.queryForList("SELECT id FROM tasks ORDER BY id", Long.class);
    }
}
