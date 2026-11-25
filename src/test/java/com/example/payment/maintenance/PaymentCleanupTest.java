package com.example.payment.maintenance;

import com.example.payment.worker.config.WorkerTransactionConfig;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
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
@Import({WorkerTransactionConfig.class, CleanupTransactionConfig.class, PaymentCleanupService.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentCleanupTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @MockitoSpyBean
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentCleanupService cleanup;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private DataSource dataSource;
    @Autowired
    @Qualifier("cleanupTransactionTemplate")
    private TransactionTemplate batchTransaction;

    @BeforeEach
    void clearPayments() {
        paymentRepository.deleteAllInBatch();
    }

    @Test
    @DisplayName("Cleanup applies independent terminal retention periods and leaves active and recent payments untouched")
    void deletesOldCompletedAndFailedPaymentsInMultipleBatches() {
        Instant now = Instant.now();
        for (int i = 0; i < 5; i++) {
            insert(ExecutionStatus.COMPLETED, now.minus(31 + i, ChronoUnit.DAYS));
            insert(ExecutionStatus.FAILED, now.minus(91 + i, ChronoUnit.DAYS));
        }
        long recentCompleted = insert(ExecutionStatus.COMPLETED, now.minus(29, ChronoUnit.DAYS));
        long recentFailed = insert(ExecutionStatus.FAILED, now.minus(89, ChronoUnit.DAYS));
        long failedPastCompletedRetention = insert(ExecutionStatus.FAILED, now.minus(31, ChronoUnit.DAYS));
        long queued = insert(ExecutionStatus.NEW, now.minus(100, ChronoUnit.DAYS));
        long running = insert(ExecutionStatus.IN_PROGRESS, now.minus(100, ChronoUnit.DAYS));

        assertThat(cleanup.archiveExpiredPayments()).isEqualTo(10);

        assertThat(ids()).containsExactlyInAnyOrder(recentCompleted, recentFailed,
            failedPastCompletedRetention, queued, running);
        assertThat(cleanup.archiveExpiredPayments()).isZero();
    }

    @Test
    @DisplayName("A delete batch is limited and removes the oldest eligible terminal rows first")
    void limitsEachBatchAndOrdersItByFinishedAt() {
        Instant cutoff = Instant.parse("2026-10-01T00:00:00Z");
        long newer = insert(ExecutionStatus.COMPLETED, cutoff.minusSeconds(1));
        long oldest = insert(ExecutionStatus.COMPLETED, cutoff.minusSeconds(3));
        long middle = insert(ExecutionStatus.COMPLETED, cutoff.minusSeconds(2));
        long atBoundary = insert(ExecutionStatus.COMPLETED, cutoff);
        long otherStatus = insert(ExecutionStatus.FAILED, cutoff.minusSeconds(4));

        assertThat(archiveBatch(ExecutionStatus.COMPLETED, cutoff)).isEqualTo(2);

        assertThat(ids()).containsExactlyInAnyOrder(newer, atBoundary, otherStatus)
            .doesNotContain(oldest, middle);
        assertThat(archiveBatch(ExecutionStatus.COMPLETED, cutoff)).isEqualTo(1);
        assertThat(ids()).containsExactlyInAnyOrder(atBoundary, otherStatus);
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"NEW", "IN_PROGRESS"})
    @DisplayName("The repository itself refuses to delete active statuses even with an old finished timestamp")
    void refusesActiveStatusesAtRepositoryBoundary(ExecutionStatus status) {
        long payment = insert(status, Instant.now().minus(100, ChronoUnit.DAYS));

        assertThat(archiveBatch(status, Instant.now())).isZero();

        assertThat(ids()).containsExactly(payment);
    }

    @Test
    @DisplayName("A locked terminal row is skipped so cleanup does not wait for a concurrent retry")
    void skipsLockedRowsAndDeletesThemAfterTheLockIsReleased() throws Exception {
        Instant cutoff = Instant.now();
        long locked = insert(ExecutionStatus.FAILED, cutoff.minus(100, ChronoUnit.DAYS));
        long available = insert(ExecutionStatus.FAILED, cutoff.minus(99, ChronoUnit.DAYS));
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("SELECT id FROM payments WHERE id = ? FOR UPDATE")) {
                statement.setLong(1, locked);
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue();

                    assertThat(archiveBatch(ExecutionStatus.FAILED, cutoff)).isEqualTo(1);
                    assertThat(ids()).containsExactly(locked).doesNotContain(available);
                }
            } finally {
                connection.rollback();
            }
        }

        assertThat(cleanup.archiveExpiredPayments()).isEqualTo(1);
        assertThat(ids()).isEmpty();
    }

    @Test
    @DisplayName("An error in a later batch does not roll back a previously committed batch")
    void commitsBatchesIndependently() {
        Instant finishedAt = Instant.now().minus(100, ChronoUnit.DAYS);
        long first = insert(ExecutionStatus.COMPLETED, finishedAt.minusSeconds(2));
        long second = insert(ExecutionStatus.COMPLETED, finishedAt.minusSeconds(1));
        long remaining = insert(ExecutionStatus.COMPLETED, finishedAt);
        long failed = insert(ExecutionStatus.FAILED, finishedAt);
        doThrow(new DataAccessResourceFailureException("Later cleanup batch failed"))
            .when(paymentRepository).archiveFinishedBefore(eq(ExecutionStatus.FAILED.name()), any(Instant.class), eq(2));

        assertThatThrownBy(cleanup::archiveExpiredPayments)
            .isInstanceOf(DataAccessResourceFailureException.class);

        assertThat(ids()).containsExactlyInAnyOrder(remaining, failed).doesNotContain(first, second);
    }

    @Test
    @DisplayName("One cleanup run is bounded even when more eligible work remains")
    void leavesBacklogForTheNextScheduledRunAfterItsBatchBudget() {
        Instant finishedAt = Instant.now().minus(100, ChronoUnit.DAYS);
        for (int i = 0; i < 23; i++) {
            insert(ExecutionStatus.COMPLETED, finishedAt);
            insert(ExecutionStatus.FAILED, finishedAt);
        }

        assertThat(cleanup.archiveExpiredPayments()).isEqualTo(40);

        assertThat(ids()).hasSize(6);
        assertThat(cleanup.archiveExpiredPayments()).isEqualTo(6);
        assertThat(ids()).isEmpty();
    }

    @Test
    @DisplayName("An interrupted scheduler thread does not begin another cleanup transaction")
    void doesNotStartCleanupWhenInterrupted() {
        Thread.currentThread().interrupt();
        try {
            assertThat(cleanup.archiveExpiredPayments()).isZero();
        } finally {
            Thread.interrupted();
        }

        verify(paymentRepository, never()).archiveFinishedBefore(any(String.class), any(Instant.class), eq(2));
    }

    private int archiveBatch(ExecutionStatus status, Instant cutoff) {
        return batchTransaction.execute(transaction -> paymentRepository.archiveFinishedBefore(status.name(), cutoff, 2));
    }

    private long insert(ExecutionStatus status, Instant finishedAt) {
        boolean running = status == ExecutionStatus.IN_PROGRESS;
        return jdbc.queryForObject("""
            INSERT INTO payments (event_id, owner_username, payment_operation_id, order_id, amount, currency, payment_method_id, idempotency_key, execution_status, execution_id, started_at, finished_at)
            VALUES (?, 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, ?, ?, ?, ?)
            RETURNING id
            """, Long.class, UUID.randomUUID(), status.name(), running ? UUID.randomUUID() : null,
            running ? Timestamp.from(Instant.now()) : null, Timestamp.from(finishedAt));
    }

    private List<Long> ids() {
        return jdbc.queryForList("SELECT id FROM payments ORDER BY id", Long.class);
    }
}
