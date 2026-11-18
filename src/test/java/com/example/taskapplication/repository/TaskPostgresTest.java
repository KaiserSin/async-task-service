package com.example.taskapplication.repository;

import com.example.taskapplication.config.TransactionConfig;
import com.example.taskapplication.config.WorkerTimingProperties;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.service.maintenance.MaintenanceScheduler;
import com.example.taskapplication.service.execution.DemoTaskHandler;
import com.example.taskapplication.service.execution.WorkerLifecycle;
import com.example.taskapplication.service.execution.TaskClaimService;
import com.example.taskapplication.service.execution.TaskStatusService;
import com.example.taskapplication.service.execution.TaskUpdateService;
import com.example.taskapplication.service.maintenance.TaskRecoveryService;
import com.example.taskapplication.service.maintenance.TaskCleanupService;
import db.migration.V4__add_task_cleanup_index;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceContext;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DataJpaTest(properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "worker.lease-seconds=47",
    "worker.retry-backoff-seconds=7"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({TransactionConfig.class, TaskClaimService.class, TaskUpdateService.class, TaskRecoveryService.class})
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskPostgresTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private TaskClaimService taskClaimService;

    @Autowired
    private TaskUpdateService taskUpdateService;

    @Autowired
    private TaskRecoveryService taskRecoveryService;

    @Autowired
    private WorkerTimingProperties timingProperties;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private DataSource dataSource;

    @PersistenceContext
    private EntityManager entityManager;

    private TransactionTemplate transactionTemplate;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        taskRepository.deleteAllInBatch();
        transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transactionTemplate.setTimeout(30);
        executor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (executor != null) {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @DisplayName("The claim query uses the partial NEW index when completed tasks dominate the table")
    void claimsThroughPartialNewIndex() throws Exception {
        jdbcTemplate.update("""
            INSERT INTO tasks (event_id, name, duration, status, progress, finished_at)
            SELECT gen_random_uuid(), 'Old completed task', 1000, 'COMPLETED', 100, CURRENT_TIMESTAMP
            FROM generate_series(1, 10000)
            """);
        Task ready = createTask();
        jdbcTemplate.execute("ANALYZE tasks");
        String claimSql = TaskRepository.class.getMethod("findAndLockFirstNewTask")
            .getAnnotation(org.springframework.data.jpa.repository.Query.class).value();

        String plan = String.join("\n", jdbcTemplate.queryForList(
            "EXPLAIN (ANALYZE, BUFFERS) " + claimSql, String.class));

        assertThat(plan).contains("Index Scan using idx_tasks_new_id").doesNotContain("Sort");
        assertThat(taskClaimService.claimNextTask().orElseThrow().id()).isEqualTo(ready.getId());
    }

    @Test
    @DisplayName("Configured lease and retry backoff reach claim, heartbeat and lease recovery SQL")
    void usesConfiguredLeaseAndRecoveryBackoff() {
        assertThat(timingProperties.leaseSeconds()).isEqualTo(47);
        assertThat(timingProperties.retryBackoffSeconds()).isEqualTo(7);
        Task task = createTask();
        Instant claimStarted = databaseNow();

        TaskRun run = taskClaimService.claimNextTask().orElseThrow();

        Instant claimSaved = databaseNow();
        assertThat(findTask(task.getId()).getLeaseUntil())
            .isBetween(claimStarted.plusSeconds(47), claimSaved.plusSeconds(47));

        Instant heartbeatStarted = databaseNow();
        assertThat(taskUpdateService.updateProgress(run.id(), run.executionId(), 40)).isTrue();
        Instant heartbeatSaved = databaseNow();
        assertThat(findTask(task.getId()).getLeaseUntil())
            .isBetween(heartbeatStarted.plusSeconds(47), heartbeatSaved.plusSeconds(47));

        expireLease(task.getId());
        Instant recoveryStarted = databaseNow();
        assertThat(taskRecoveryService.recoverExpiredTasks()).hasSize(1);
        Instant recoverySaved = databaseNow();
        Task recovered = findTask(task.getId());
        assertThat(recovered.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(recovered.getNextRetryAt())
            .isBetween(recoveryStarted.plusSeconds(7), recoverySaved.plusSeconds(7));
        assertThat(taskClaimService.claimNextTask()).isEmpty();
    }

    @Test
    @DisplayName("Two workers claim different tasks")
    void claimsTwoTasks() throws Exception {
        Task first = createTask();
        Task second = createTask();
        CyclicBarrier start = new CyclicBarrier(2);
        Callable<Optional<TaskRun>> claim = () -> {

            start.await(10, TimeUnit.SECONDS);
            return taskClaimService.claimNextTask();
        };

        Future<Optional<TaskRun>> firstClaim = executor.submit(claim);
        Future<Optional<TaskRun>> secondClaim = executor.submit(claim);
        TaskRun firstRun = firstClaim.get(10, TimeUnit.SECONDS).orElseThrow();
        TaskRun secondRun = secondClaim.get(10, TimeUnit.SECONDS).orElseThrow();

        assertThat(List.of(firstRun.id(), secondRun.id()))
            .containsExactlyInAnyOrder(first.getId(), second.getId());
        assertThat(firstRun.executionId()).isNotNull().isNotEqualTo(secondRun.executionId());
        assertThat(secondRun.executionId()).isNotNull();
        for (Task task : List.of(first, second)) {
            Task saved = findTask(task.getId());
            assertThat(saved.getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);
            assertThat(saved.getAttempts()).isEqualTo(1);
            assertThat(saved.getVersion()).isEqualTo(task.getVersion() + 1);
        }
        assertThat(findTask(firstRun.id()).getExecutionId()).isEqualTo(firstRun.executionId());
        assertThat(findTask(secondRun.id()).getExecutionId()).isEqualTo(secondRun.executionId());
    }

    @Test
    @DisplayName("Native SQL skips a locked task")
    void skipsLockedTask() throws Exception {
        Task first = createTask();
        Task second = createTask();
        createTask();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> holder = lockTask(first.getId(), locked, release);

        try {
            awaitLatch(locked);
            Future<Optional<TaskRun>> claim = executor.submit(taskClaimService::claimNextTask);

            TaskRun run = claim.get(10, TimeUnit.SECONDS).orElseThrow();

            assertThat(run.id()).isEqualTo(second.getId());
        } finally {
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        }
        assertThat(findTask(first.getId()).getStatus()).isEqualTo(TaskStatus.NEW);
    }

    @Test
    @DisplayName("Hibernate skips a locked task and selects only one task")
    void hibernateSkipsLockedTask() throws Exception {
        Task first = createTask();
        Task second = createTask();
        createTask();
        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> holder = lockTask(first.getId(), locked, release);

        try {
            awaitLatch(locked);
            Future<List<Long>> claim = executor.submit(() -> transactionTemplate.execute(status ->
                taskRepository.findAndLockNewTasks(TaskStatus.NEW, PageRequest.of(0, 1))
                    .stream()
                    .map(Task::getId)
                    .toList()
            ));

            List<Long> ids = claim.get(10, TimeUnit.SECONDS);

            assertThat(ids).containsExactly(second.getId());
        } finally {
            release.countDown();
            holder.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("An old entity version does not overwrite a claimed task")
    void rejectsOldVersion() throws Exception {
        Task task = createTask();
        CountDownLatch loaded = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> writer = executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
            Task oldTask = entityManager.find(Task.class, task.getId());
            loaded.countDown();
            awaitLatch(release);
            oldTask.setName("Old change");
            entityManager.flush();
        }));

        try {
            awaitLatch(loaded);
            TaskRun run = taskClaimService.claimNextTask().orElseThrow();

            assertThat(run.id()).isEqualTo(task.getId());
        } finally {
            release.countDown();
            assertThatThrownBy(() -> writer.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OptimisticLockException.class);
        }

        Task saved = findTask(task.getId());
        assertThat(saved.getName()).isEqualTo(task.getName());
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);
        assertThat(saved.getVersion()).isEqualTo(task.getVersion() + 1);
    }

    @Test
    @DisplayName("A timed-out heartbeat recovers and completes the same attempt while its lease is valid")
    void completesSameAttemptAfterTransientHeartbeatFailure() {
        Task task = createTask();
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        Instant initialLease = findTask(task.getId()).getLeaseUntil();
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicReference<DataAccessException> heartbeatFailure = new AtomicReference<>();
        AtomicReference<Instant> renewedLease = new AtomicReference<>();
        TaskUpdateService updates = new TaskUpdateService(taskRepository, timingProperties) {
            @Override
            public boolean updateProgress(Long id, UUID executionId, int progress) {
                if (heartbeats.incrementAndGet() == 1) {
                    try {
                        transactionTemplate.executeWithoutResult(status -> {
                            jdbcTemplate.execute("SET LOCAL statement_timeout = '25ms'");
                            jdbcTemplate.execute("SELECT pg_sleep(0.1)");
                        });
                    } catch (DataAccessException failure) {
                        heartbeatFailure.set(failure);
                        throw failure;
                    }
                }
                boolean updated = taskUpdateService.updateProgress(id, executionId, progress);
                renewedLease.set(findTask(id).getLeaseUntil());
                return updated;
            }

            @Override
            public boolean updateTaskStatus(
                Long id, UUID executionId, TaskStatus status, int progress, String result
            ) {
                return taskUpdateService.updateTaskStatus(id, executionId, status, progress, result);
            }
        };

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            new TaskStatusService(updates, new TaskMetrics(registry, jdbcTemplate), new DemoTaskHandler(), timingProperties).executeTask(run);
        } finally {
            registry.close();
        }

        assertThat(heartbeatFailure.get()).isInstanceOf(QueryTimeoutException.class);
        assertThat(heartbeats).hasValue(2);
        assertThat(renewedLease.get()).isAfter(initialLease);
        Task completed = findTask(task.getId());
        assertThat(completed.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(completed.getExecutionId()).isEqualTo(run.executionId());
        assertThat(completed.getAttempts()).isEqualTo(1);
        assertThat(completed.getProgress()).isEqualTo(100);
        assertThat(completed.getResult()).isEqualTo("Task completed");
        assertThat(completed.getLeaseUntil()).isNull();
        assertThat(completed.getNextRetryAt()).isNull();
    }

    @Test
    @DisplayName("An expired lease cannot be revived by a heartbeat or completed by its old worker")
    void rejectsHeartbeatAndCompletionAfterLeaseExpiry() {
        Task task = createTask();
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        expireLease(task.getId());
        Task before = findTask(task.getId());

        assertThat(taskUpdateService.updateProgress(run.id(), run.executionId(), 40)).isFalse();
        assertThat(taskUpdateService.updateTaskStatus(
            run.id(), run.executionId(), TaskStatus.COMPLETED, 100, "Late result"
        )).isFalse();

        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @Test
    @DisplayName("An old worker does not change a new execution attempt")
    void rejectsOldWorker() {
        Task task = createTask();
        TaskRun oldRun = taskClaimService.claimNextTask().orElseThrow();

        expireLease(task.getId());
        assertThat(taskRecoveryService.recoverExpiredTasks()).hasSize(1);
        jdbcTemplate.update("""
            UPDATE tasks
            SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, task.getId());
        TaskRun newRun = taskClaimService.claimNextTask().orElseThrow();
        assertThat(newRun.id()).isEqualTo(oldRun.id());
        assertThat(newRun.executionId()).isNotEqualTo(oldRun.executionId());
        assertThat(taskUpdateService.updateProgress(newRun.id(), newRun.executionId(), 40)).isTrue();
        Task before = findTask(task.getId());

        boolean progressUpdated = taskUpdateService.updateProgress(oldRun.id(), oldRun.executionId(), 50);
        boolean finished = taskUpdateService.updateTaskStatus(
            oldRun.id(), oldRun.executionId(), TaskStatus.COMPLETED, 100, "Old result"
        );
        boolean requeued = taskUpdateService.requeueInterruptedTask(oldRun.id(), oldRun.executionId());
        Optional<TaskStatus> failureSaved = taskUpdateService.failTaskAttempt(oldRun.id(), oldRun.executionId(), 50, "Old failure");

        assertThat(progressUpdated).isFalse();
        assertThat(finished).isFalse();
        assertThat(requeued).isFalse();
        assertThat(failureSaved).isEmpty();
        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(taskUpdateService.updateTaskStatus(
            newRun.id(), newRun.executionId(), TaskStatus.COMPLETED, 100, "New result"
        )).isTrue();
        Task saved = findTask(task.getId());
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(saved.getProgress()).isEqualTo(100);
        assertThat(saved.getResult()).isEqualTo("New result");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    @DisplayName("An interruption resets execution state and preserves the retry budget, including the last attempt")
    void requeuesInterruptedAttempt(int previousAttempts) {
        Task task = createTask();
        jdbcTemplate.update("UPDATE tasks SET attempts = ? WHERE id = ?", previousAttempts, task.getId());
        TaskRun interrupted = taskClaimService.claimNextTask().orElseThrow();
        assertThat(taskUpdateService.updateProgress(task.getId(), interrupted.executionId(), 40)).isTrue();
        jdbcTemplate.update("UPDATE tasks SET result = 'Partial result' WHERE id = ?", task.getId());
        Task before = findTask(task.getId());

        assertThat(taskUpdateService.requeueInterruptedTask(task.getId(), interrupted.executionId())).isTrue();

        Task requeued = findTask(task.getId());
        assertThat(requeued.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(requeued.getAttempts()).isEqualTo(previousAttempts);
        assertThat(requeued.getProgress()).isZero();
        assertThat(requeued.getResult()).isNull();
        assertThat(requeued.getExecutionId()).isNull();
        assertThat(requeued.getLeaseUntil()).isNull();
        assertThat(requeued.getNextRetryAt()).isNull();
        assertThat(requeued.getStartedAt()).isNull();
        assertThat(requeued.getFinishedAt()).isNull();
        assertThat(requeued.getCreatedAt()).isEqualTo(before.getCreatedAt());
        assertThat(requeued.getVersion()).isEqualTo(before.getVersion() + 1);
        assertThat(taskUpdateService.requeueInterruptedTask(task.getId(), interrupted.executionId())).isFalse();
        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(requeued);

        TaskRun resumed = taskClaimService.claimNextTask().orElseThrow();
        assertThat(resumed.id()).isEqualTo(task.getId());
        assertThat(resumed.executionId()).isNotEqualTo(interrupted.executionId());
        assertThat(findTask(task.getId()).getAttempts()).isEqualTo(previousAttempts + 1);
        assertThat(taskUpdateService.updateTaskStatus(
            resumed.id(), resumed.executionId(), TaskStatus.COMPLETED, 100, "Task completed"
        )).isTrue();
        assertThat(findTask(task.getId()).getStatus()).isEqualTo(TaskStatus.COMPLETED);
    }

    @Test
    @DisplayName("An interrupted attempt with an expired lease cannot bypass lease recovery")
    void rejectsRequeueAfterLeaseExpiry() {
        Task task = createTask();
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        expireLease(task.getId());
        Task before = findTask(task.getId());

        assertThat(taskUpdateService.requeueInterruptedTask(task.getId(), run.executionId())).isFalse();

        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(taskRecoveryService.recoverExpiredTasks()).hasSize(1);
        assertThat(findTask(task.getId()).getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(findTask(task.getId()).getAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("A late interruption cannot put a completed task back into the queue")
    void rejectsRequeueOfCompletedTask() {
        Task task = createTask();
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        assertThat(taskUpdateService.updateTaskStatus(
            run.id(), run.executionId(), TaskStatus.COMPLETED, 100, "Task completed"
        )).isTrue();
        Task before = findTask(task.getId());

        assertThat(taskUpdateService.requeueInterruptedTask(task.getId(), run.executionId())).isFalse();

        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4})
    @DisplayName("Failed executions use the configured backoff and stop at the task's attempt limit")
    void retriesFailuresUntilAttemptLimit(int maxAttempts) {
        Task task = createTask();
        jdbcTemplate.update("UPDATE tasks SET max_attempts = ? WHERE id = ?", maxAttempts, task.getId());

        UUID previousExecutionId = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            TaskRun run = taskClaimService.claimNextTask().orElseThrow();
            assertThat(run.id()).isEqualTo(task.getId());
            assertThat(run.executionId()).isNotEqualTo(previousExecutionId);
            assertThat(taskUpdateService.updateProgress(run.id(), run.executionId(), 40)).isTrue();
            Task before = findTask(task.getId());
            Instant failureStarted = databaseNow();
            String failure = "Failure on attempt " + attempt;

            assertThat(taskUpdateService.failTaskAttempt(run.id(), run.executionId(), 40, failure))
                .contains(attempt < maxAttempts ? TaskStatus.NEW : TaskStatus.FAILED);

            Instant failureSaved = databaseNow();
            Task saved = findTask(task.getId());
            assertThat(saved.getAttempts()).isEqualTo(attempt);
            assertThat(saved.getMaxAttempts()).isEqualTo(maxAttempts);
            assertThat(saved.getLeaseUntil()).isNull();
            assertThat(saved.getResult()).isEqualTo(failure);
            assertThat(saved.getVersion()).isEqualTo(before.getVersion() + 1);
            assertThat(saved.getCreatedAt()).isEqualTo(before.getCreatedAt());

            if (attempt < maxAttempts) {
                assertThat(saved.getStatus()).isEqualTo(TaskStatus.NEW);
                assertThat(saved.getNextRetryAt())
                    .isBetween(failureStarted.plusSeconds(timingProperties.retryBackoffSeconds()),
                        failureSaved.plusSeconds(timingProperties.retryBackoffSeconds()));
                assertThat(saved.getProgress()).isZero();
                assertThat(saved.getExecutionId()).isNull();
                assertThat(saved.getStartedAt()).isNull();
                assertThat(saved.getFinishedAt()).isNull();
            } else {
                assertThat(saved.getStatus()).isEqualTo(TaskStatus.FAILED);
                assertThat(saved.getNextRetryAt()).isNull();
                assertThat(saved.getProgress()).isEqualTo(40);
                assertThat(saved.getExecutionId()).isEqualTo(run.executionId());
                assertThat(saved.getStartedAt()).isEqualTo(before.getStartedAt());
                assertThat(saved.getFinishedAt()).isBetween(failureStarted, failureSaved);
            }

            assertThat(taskClaimService.claimNextTask()).isEmpty();
            List<Task> ready = transactionTemplate.execute(status -> taskRepository.findAndLockNewTasks(
                TaskStatus.NEW, PageRequest.of(0, 1)
            ));
            assertThat(ready).isEmpty();
            assertThat(taskUpdateService.failTaskAttempt(run.id(), run.executionId(), 40, "Duplicate failure"))
                .isEmpty();
            assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(saved);

            if (attempt < maxAttempts) {
                allowRetry(task.getId());
            }
            previousExecutionId = run.executionId();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    @DisplayName("Only a committed terminal handler failure increments the failed counter")
    void countsOnlyTerminalHandlerFailure(int maxAttempts) {
        Task task = createTask();
        jdbcTemplate.update("UPDATE tasks SET max_attempts = ? WHERE id = ?", maxAttempts, task.getId());
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            TaskMetrics metrics = new TaskMetrics(registry, jdbcTemplate);
            TaskStatusService statusService = new TaskStatusService(taskUpdateService, metrics,
                (claimed, elapsed, step) -> { throw new IllegalStateException("Handler failed"); },
                timingProperties);

            statusService.executeTask(run);

            assertThat(findTask(task.getId()).getStatus())
                .isEqualTo(maxAttempts == 1 ? TaskStatus.FAILED : TaskStatus.NEW);
            assertThat(registry.get("task.failed").counter().count()).isEqualTo(maxAttempts == 1 ? 1 : 0);
            assertThat(registry.get("task.completed").counter().count()).isZero();

            statusService.executeTask(run);

            assertThat(registry.get("task.failed").counter().count()).isEqualTo(maxAttempts == 1 ? 1 : 0);
            assertThat(findTask(task.getId()).getAttempts()).isEqualTo(1);
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("Only a committed completion increments the completed counter; a stale final write does not")
    void countsCompletionOnceAcrossStaleFinalWrites() {
        Task task = createTask();
        jdbcTemplate.update("UPDATE tasks SET duration = 500 WHERE id = ?", task.getId());
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            TaskMetrics metrics = new TaskMetrics(registry, jdbcTemplate);
            TaskStatusService statusService = new TaskStatusService(taskUpdateService, metrics,
                (claimed, elapsed, step) -> {}, timingProperties);

            statusService.executeTask(run);
            statusService.executeTask(run);

            assertThat(findTask(task.getId()).getStatus()).isEqualTo(TaskStatus.COMPLETED);
            assertThat(registry.get("task.completed").counter().count()).isEqualTo(1);
            assertThat(registry.get("task.failed").counter().count()).isZero();
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("Recovery counts committed failures separately from retries without counting a task twice")
    void countsTerminalLeaseRecoveryOnce() {
        Task exhausted = createTask();
        jdbcTemplate.update("UPDATE tasks SET attempts = 2 WHERE id = ?", exhausted.getId());
        Task retryable = createTask();
        taskClaimService.claimNextTask().orElseThrow();
        taskClaimService.claimNextTask().orElseThrow();
        expireLease(exhausted.getId());
        expireLease(retryable.getId());
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            TaskMetrics metrics = new TaskMetrics(registry, jdbcTemplate);
            MaintenanceScheduler scheduler = new MaintenanceScheduler(mock(WorkerLifecycle.class),
                taskRecoveryService, mock(TaskCleanupService.class), metrics);

            scheduler.recoverExpiredTasks();
            scheduler.recoverExpiredTasks();

            assertThat(findTask(exhausted.getId()).getStatus()).isEqualTo(TaskStatus.FAILED);
            assertThat(findTask(retryable.getId()).getStatus()).isEqualTo(TaskStatus.NEW);
            assertThat(registry.get("task.failed").counter().count()).isEqualTo(1);
            assertThat(registry.get("task.recovered").counter().count()).isEqualTo(1);
            assertThat(registry.get("task.completed").counter().count()).isZero();
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("A task can complete after a failed attempt without losing or refunding its consumed retry")
    void completesAfterTransientExecutionFailure() {
        Task task = createTask();
        TaskRun firstRun = taskClaimService.claimNextTask().orElseThrow();
        assertThat(taskUpdateService.failTaskAttempt(firstRun.id(), firstRun.executionId(), 0, "Transient failure"))
            .contains(TaskStatus.NEW);
        assertThat(taskClaimService.claimNextTask()).isEmpty();
        allowRetry(task.getId());

        TaskRun retriedRun = taskClaimService.claimNextTask().orElseThrow();

        assertThat(retriedRun.executionId()).isNotEqualTo(firstRun.executionId());
        assertThat(findTask(task.getId()).getAttempts()).isEqualTo(2);
        assertThat(findTask(task.getId()).getResult()).isNull();
        assertThat(taskUpdateService.updateTaskStatus(
            retriedRun.id(), retriedRun.executionId(), TaskStatus.COMPLETED, 100, "Task completed"
        )).isTrue();
        Task completed = findTask(task.getId());
        assertThat(completed.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(completed.getProgress()).isEqualTo(100);
        assertThat(completed.getAttempts()).isEqualTo(2);
        assertThat(completed.getResult()).isEqualTo("Task completed");
        assertThat(completed.getNextRetryAt()).isNull();
        assertThat(taskClaimService.claimNextTask()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"expired", "completed"})
    @DisplayName("A late execution failure cannot change an expired or completed task")
    void rejectsLateExecutionFailure(String state) {
        Task task = createTask();
        TaskRun run = taskClaimService.claimNextTask().orElseThrow();
        if (state.equals("expired")) {
            expireLease(task.getId());
        } else {
            assertThat(taskUpdateService.updateTaskStatus(
                run.id(), run.executionId(), TaskStatus.COMPLETED, 100, "Task completed"
            )).isTrue();
        }
        Task before = findTask(task.getId());

        assertThat(taskUpdateService.failTaskAttempt(run.id(), run.executionId(), 40, "Late failure")).isEmpty();

        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @Test
    @DisplayName("An expired task becomes FAILED after the last attempt")
    void stopsAfterLastAttempt() {
        Task task = createTask();
        jdbcTemplate.update("UPDATE tasks SET attempts = 2 WHERE id = ?", task.getId());

        taskClaimService.claimNextTask().orElseThrow();
        assertThat(findTask(task.getId()).getAttempts()).isEqualTo(3);
        expireLease(task.getId());
        assertThat(taskRecoveryService.recoverExpiredTasks()).hasSize(1);

        Task saved = findTask(task.getId());
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(saved.getAttempts()).isEqualTo(3);
        assertThat(saved.getFinishedAt()).isNotNull();
        assertThat(saved.getExecutionId()).isNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(taskClaimService.claimNextTask()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 4})
    @DisplayName("A task at or above the attempt limit is not claimed")
    void rejectsSpentAttempts(int attempts) {
        Task task = createTask();
        jdbcTemplate.update("UPDATE tasks SET attempts = ? WHERE id = ?", attempts, task.getId());
        Task before = findTask(task.getId());

        Optional<TaskRun> run = taskClaimService.claimNextTask();

        assertThat(run).isEmpty();
        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    @Test
    @DisplayName("Queue metrics use the partial NEW index and cache counts and ready age")
    void readsQueueMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        JdbcTemplate observedJdbc = spy(jdbcTemplate);
        try {
            TaskMetrics metrics = new TaskMetrics(registry, observedJdbc);
            metrics.updateQueueMetrics();
            assertThat(registry.get("task.queue.new").gauge().value()).isZero();
            assertThat(registry.get("task.queue.oldest.ready.age").timeGauge()
                .value(TimeUnit.SECONDS)).isZero();

            transactionTemplate.executeWithoutResult(status -> {
                jdbcTemplate.update("""
                    INSERT INTO tasks (
                        event_id, name, duration, created_at, next_retry_at, attempts
                    ) VALUES
                        (?, 'Ready', 1000, CURRENT_TIMESTAMP - INTERVAL '30 seconds', NULL, 0),
                        (?, 'Delayed', 1000, CURRENT_TIMESTAMP - INTERVAL '1 hour',
                            CURRENT_TIMESTAMP + INTERVAL '1 minute', 1),
                        (?, 'No attempts left', 1000, CURRENT_TIMESTAMP - INTERVAL '1 hour', NULL, 3),
                        (?, 'Ready retry', 1000, CURRENT_TIMESTAMP - INTERVAL '20 seconds',
                            CURRENT_TIMESTAMP - INTERVAL '1 second', 1)
                    """, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
                jdbcTemplate.update("""
                    INSERT INTO tasks (event_id, name, duration, status, finished_at)
                    SELECT gen_random_uuid(), 'Completed', 1000, 'COMPLETED', CURRENT_TIMESTAMP
                    FROM generate_series(1, 10000)
                    """);
                jdbcTemplate.execute("ANALYZE tasks");

                metrics.updateQueueMetrics();

                assertThat(registry.get("task.queue.new").gauge().value()).isEqualTo(4);
                assertThat(registry.get("task.queue.oldest.ready.age").timeGauge()
                    .value(TimeUnit.SECONDS)).isEqualTo(30);
                ArgumentCaptor<String> metricsSql = ArgumentCaptor.forClass(String.class);
                verify(observedJdbc, times(2)).queryForMap(metricsSql.capture());
                String plan = String.join("\n", jdbcTemplate.queryForList(
                    "EXPLAIN (ANALYZE, BUFFERS) " + metricsSql.getValue(), String.class));
                assertThat(plan).contains("idx_tasks_new_id").doesNotContain("Seq Scan on tasks");
            });
        } finally {
            registry.close();
        }
    }

    @Test
    @DisplayName("Flyway initializes an empty schema and preserves data on a repeated run")
    void migratesSchema() {
        String schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        Flyway flyway = Flyway.configure()
            .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
            .dataSource(dataSource)
            .schemas(schema)
            .defaultSchema(schema)
            .locations("classpath:db/migration")
            .load();

        try {
            assertThat(flyway.migrate().migrationsExecuted).isEqualTo(4);
            assertThat(jdbcTemplate.queryForList("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = ? AND table_name = 'tasks'
                ORDER BY ordinal_position
                """, String.class, schema)).containsExactly(
                    "id", "event_id", "name", "duration", "status", "progress", "result",
                    "execution_id", "lease_until", "next_retry_at", "attempts", "max_attempts",
                    "created_at", "started_at", "finished_at", "version", "owner_username"
                );

            UUID eventId = UUID.randomUUID();
            Map<String, Object> before = jdbcTemplate.queryForMap("""
                INSERT INTO %s.tasks (event_id, name, duration)
                VALUES (?, ?, ?) RETURNING *
                """.formatted(schema), eventId, "Task", 60000L);
            assertThat(((Number) before.get("id")).longValue()).isPositive();
            assertThat(before).containsEntry("event_id", eventId)
                .containsEntry("name", "Task")
                .containsEntry("duration", 60000L)
                .containsEntry("status", "NEW")
                .containsEntry("progress", 0)
                .containsEntry("attempts", 0)
                .containsEntry("max_attempts", 3)
                .containsEntry("version", 0L);
            assertThat(before.get("created_at")).isNotNull();
            for (String column : List.of(
                "result", "execution_id", "lease_until", "next_retry_at", "started_at", "finished_at", "owner_username"
            )) {
                assertThat(before.get(column)).as(column).isNull();
            }
            assertThat(jdbcTemplate.queryForObject("""
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = ? AND indexname = 'idx_tasks_new_id'
                """, String.class, schema)).contains("(id) WHERE ((status)::text = 'NEW'::text)");
            assertCleanupInfrastructure(schema);
            assertThat(jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM %s.flyway_schema_history
                WHERE version = '1' AND success
                """.formatted(schema), Long.class)).isEqualTo(1L);

            assertThat(flyway.migrate().migrationsExecuted).isZero();
            flyway.validate();
            assertThat(jdbcTemplate.queryForMap(
                "SELECT * FROM " + schema + ".tasks WHERE id = ?", before.get("id")
            )).isEqualTo(before);
        } finally {

            jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    @Test
    @DisplayName("The owner migration preserves legacy tasks without assigning them to a user")
    void upgradesLegacyTasksWithoutInventingOwners() {
        String schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        Flyway original = Flyway.configure()
            .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
            .dataSource(dataSource)
            .schemas(schema)
            .defaultSchema(schema)
            .locations("classpath:db/migration")
            .target("1")
            .load();

        try {
            assertThat(original.migrate().migrationsExecuted).isEqualTo(1);
            Map<String, Object> before = jdbcTemplate.queryForMap("""
                INSERT INTO %s.tasks (event_id, name, duration)
                VALUES (?, 'Legacy task', 1000) RETURNING *
                """.formatted(schema), UUID.randomUUID());
            Flyway upgraded = Flyway.configure()
                .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
                .dataSource(dataSource)
                .schemas(schema)
                .defaultSchema(schema)
                .locations("classpath:db/migration")
                .load();

            assertThat(upgraded.migrate().migrationsExecuted).isEqualTo(3);

            assertThat(jdbcTemplate.queryForMap(
                "SELECT * FROM " + schema + ".tasks WHERE id = ?", before.get("id")
            )).containsAllEntriesOf(before).containsEntry("owner_username", null);
            upgraded.validate();
        } finally {
            jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    @Test
    @DisplayName("Cleanup migrations preserve existing tasks and create a valid index and shared lock table")
    void upgradesExistingTasksForCleanup() {
        String schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        Flyway original = Flyway.configure()
            .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
            .dataSource(dataSource)
            .schemas(schema)
            .defaultSchema(schema)
            .locations("classpath:db/migration")
            .target("2")
            .load();

        try {
            assertThat(original.migrate().migrationsExecuted).isEqualTo(2);
            Map<String, Object> before = jdbcTemplate.queryForMap("""
                INSERT INTO %s.tasks (event_id, name, duration, status, finished_at, owner_username)
                VALUES (?, 'Old failure', 1000, 'FAILED', CURRENT_TIMESTAMP - INTERVAL '100 days', 'alice')
                RETURNING *
                """.formatted(schema), UUID.randomUUID());
            Flyway upgraded = Flyway.configure()
                .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
                .dataSource(dataSource)
                .schemas(schema)
                .defaultSchema(schema)
                .locations("classpath:db/migration")
                .load();

            assertThat(upgraded.migrate().migrationsExecuted).isEqualTo(2);

            assertThat(jdbcTemplate.queryForMap("SELECT * FROM " + schema + ".tasks WHERE id = ?",
                before.get("id"))).isEqualTo(before);
            assertCleanupInfrastructure(schema);
            assertThat(upgraded.migrate().migrationsExecuted).isZero();
            upgraded.validate();
        } finally {
            jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    @Test
    @DisplayName("The concurrent index migration restores the session timeout after success and failure")
    void restoresIndexMigrationStatementTimeout() throws Exception {
        String schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        jdbcTemplate.execute("CREATE SCHEMA " + schema);
        jdbcTemplate.execute("CREATE TABLE " + schema + ".tasks (status VARCHAR(20), finished_at TIMESTAMPTZ, id BIGINT)");

        try (Connection connection = dataSource.getConnection()) {
            String timeout = connectionSetting(connection, "statement_timeout");
            String searchPath = connectionSetting(connection, "search_path");
            try {
                setConnectionSetting(connection, "search_path", schema);
                setConnectionSetting(connection, "statement_timeout", "1234ms");
                Context context = mock(Context.class);
                when(context.getConnection()).thenReturn(connection);
                var migration = new V4__add_task_cleanup_index();
                assertThat(migration.canExecuteInTransaction()).isFalse();

                migration.migrate(context);

                assertThat(connectionSetting(connection, "statement_timeout")).isEqualTo("1234ms");
                assertThatThrownBy(() -> migration.migrate(context)).isInstanceOf(SQLException.class);
                assertThat(connectionSetting(connection, "statement_timeout")).isEqualTo("1234ms");
            } finally {
                setConnectionSetting(connection, "statement_timeout", timeout);
                setConnectionSetting(connection, "search_path", searchPath);
            }
        } finally {
            jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    private void assertCleanupInfrastructure(String schema) {
        assertThat(jdbcTemplate.queryForObject("""
            SELECT indexdef FROM pg_indexes
            WHERE schemaname = ? AND indexname = 'idx_tasks_status_finished_at_id'
            """, String.class, schema)).contains("(status, finished_at, id)");
        assertThat(jdbcTemplate.queryForObject("""
            SELECT i.indisvalid FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ? AND c.relname = 'idx_tasks_status_finished_at_id'
            """, Boolean.class, schema)).isTrue();
        assertThat(jdbcTemplate.queryForList("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = ? AND table_name = 'shedlock'
            ORDER BY ordinal_position
            """, String.class, schema)).containsExactly("name", "lock_until", "locked_at", "locked_by");
    }

    private String connectionSetting(Connection connection, String name) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT current_setting(?)")) {
            statement.setString(1, name);
            try (var result = statement.executeQuery()) {
                result.next();
                return result.getString(1);
            }
        }
    }

    private void setConnectionSetting(Connection connection, String name, String value) throws SQLException {
        try (var statement = connection.prepareStatement("SELECT set_config(?, ?, false)")) {
            statement.setString(1, name);
            statement.setString(2, value);
            statement.execute();
        }
    }

    @Test
    @DisplayName("JPA saves and reads all fields of a completed task")
    void storesTask() {
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
        task.setOwnerUsername("alice");
        task.setName("Completed task");
        task.setDuration(60000L);
        task.setStatus(TaskStatus.COMPLETED);
        task.setProgress(100);
        task.setResult("Done");
        task.setExecutionId(UUID.randomUUID());
        task.setAttempts(2);
        task.setMaxAttempts(5);
        task.setCreatedAt(Instant.parse("2026-01-01T10:00:00Z"));
        task.setStartedAt(Instant.parse("2026-01-01T10:01:00Z"));
        task.setFinishedAt(Instant.parse("2026-01-01T10:02:00Z"));

        Task saved = taskRepository.saveAndFlush(task);

        assertThat(saved.getId()).isPositive();
        assertThat(saved.getVersion()).isZero();
        assertThat(findTask(saved.getId())).usingRecursiveComparison().isEqualTo(saved);
    }

    @Test
    @DisplayName("PostgreSQL rejects duplicate eventId values")
    void rejectsDuplicate() {
        UUID eventId = UUID.randomUUID();
        String sql = "INSERT INTO tasks (event_id, name, duration) VALUES (?, ?, ?)";
        jdbcTemplate.update(sql, eventId, "First task", 1000L);

        assertThatThrownBy(() -> jdbcTemplate.update(sql, eventId, "Duplicate", 1000L))
            .isInstanceOf(DataIntegrityViolationException.class);

        assertThat(taskRepository.count()).isEqualTo(1L);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "duration = 0", "duration = 60001", "progress = -1", "progress = 101",
        "status = 'UNKNOWN'",
        "status = 'IN_PROGRESS', started_at = CURRENT_TIMESTAMP",
        "status = 'IN_PROGRESS', execution_id = '11111111-1111-1111-1111-111111111111'::uuid",
        "status = 'COMPLETED'", "status = 'FAILED'",
        "attempts = -1", "max_attempts = 0", "event_id = NULL", "name = NULL"
    })
    @DisplayName("PostgreSQL constraints reject invalid data")
    void rejectsBadData(String change) {
        Task task = createTask();
        Task before = findTask(task.getId());

        assertThatThrownBy(() -> jdbcTemplate.update(
            "UPDATE tasks SET " + change + " WHERE id = ?", task.getId()
        )).isInstanceOf(DataIntegrityViolationException.class);

        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
    }

    private Task createTask() {
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
        task.setName("Task");
        task.setDuration(2500L);
        return taskRepository.saveAndFlush(task);
    }

    private Task findTask(Long id) {
        return taskRepository.findById(id).orElseThrow();
    }

    private Future<?> lockTask(Long id, CountDownLatch locked, CountDownLatch release) {
        return executor.submit(() -> transactionTemplate.executeWithoutResult(status -> {
            Task task = taskRepository.findAndLockFirstNewTask().orElseThrow();
            assertThat(task.getId()).isEqualTo(id);

            locked.countDown();
            awaitLatch(release);
        }));
    }

    private void expireLease(Long id) {
        jdbcTemplate.update("""
            UPDATE tasks
            SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private Instant databaseNow() {
        return jdbcTemplate.queryForObject("SELECT CURRENT_TIMESTAMP", Timestamp.class).toInstant();
    }

    private void allowRetry(Long id) {
        jdbcTemplate.update("""
            UPDATE tasks
            SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Test wait was interrupted", e);
        }
    }
}
