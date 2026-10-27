package com.example.taskapplication.repository;

import com.example.taskapplication.config.TransactionConfig;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.service.execution.TaskClaimService;
import com.example.taskapplication.service.execution.TaskUpdateService;
import com.example.taskapplication.service.maintenance.TaskRecoveryService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.persistence.EntityManager;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceContext;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
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

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
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

        assertThat(progressUpdated).isFalse();
        assertThat(finished).isFalse();
        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(taskUpdateService.updateTaskStatus(
            newRun.id(), newRun.executionId(), TaskStatus.COMPLETED, 100, "New result"
        )).isTrue();
        Task saved = findTask(task.getId());
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(saved.getProgress()).isEqualTo(100);
        assertThat(saved.getResult()).isEqualTo("New result");
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
    @DisplayName("Queue metrics count all NEW tasks and age only ready tasks")
    void readsQueueMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try {
            TaskMetrics metrics = new TaskMetrics(registry, jdbcTemplate);
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
                    VALUES (?, 'Completed', 1000, 'COMPLETED', CURRENT_TIMESTAMP)
                    """, UUID.randomUUID());

                metrics.updateQueueMetrics();

                assertThat(registry.get("task.queue.new").gauge().value()).isEqualTo(4);
                assertThat(registry.get("task.queue.oldest.ready.age").timeGauge()
                    .value(TimeUnit.SECONDS)).isEqualTo(30);
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
            .dataSource(dataSource)
            .schemas(schema)
            .defaultSchema(schema)
            .locations("classpath:db/migration")
            .load();

        try {
            assertThat(flyway.migrate().migrationsExecuted).isEqualTo(1);
            assertThat(jdbcTemplate.queryForList("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = ? AND table_name = 'tasks'
                ORDER BY ordinal_position
                """, String.class, schema)).containsExactly(
                    "id", "event_id", "name", "duration", "status", "progress", "result",
                    "execution_id", "lease_until", "next_retry_at", "attempts", "max_attempts",
                    "created_at", "started_at", "finished_at", "version"
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
                "result", "execution_id", "lease_until", "next_retry_at", "started_at", "finished_at"
            )) {
                assertThat(before.get(column)).as(column).isNull();
            }
            assertThat(jdbcTemplate.queryForObject("""
                SELECT indexdef FROM pg_indexes
                WHERE schemaname = ? AND indexname = 'idx_tasks_status_id'
                """, String.class, schema)).contains("(status, id)");
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
    @DisplayName("JPA saves and reads all fields of a completed task")
    void storesTask() {
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
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

    private void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Test wait was interrupted", e);
        }
    }
}
