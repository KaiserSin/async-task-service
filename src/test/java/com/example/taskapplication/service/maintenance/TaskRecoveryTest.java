package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskRecoveryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.16")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @TempDir
    Path logs;

    private final List<Instance> instances = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS test_calls (task_id BIGINT NOT NULL)");
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS test_effects (task_id BIGINT PRIMARY KEY)");
        jdbcTemplate.update("DELETE FROM test_calls");
        jdbcTemplate.update("DELETE FROM test_effects");
        taskRepository.deleteAllInBatch();
    }

    @AfterEach
    void tearDown() throws InterruptedException {

        for (Instance instance : instances) {
            instance.process().destroyForcibly();
        }
        for (Instance instance : instances) {
            assertThat(instance.process().waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    @DisplayName("A crash before commit rolls back the claim and releases the task")
    void rollsBackClaim() throws Exception {
        Task task = createTask(2500);
        Instance first = start("beforeCommit");
        UUID oldExecutionId = awaitRun(first, "BEFORE_COMMIT", task.getId());

        kill(first);

        Task saved = findTask(task.getId());
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(saved.getAttempts()).isZero();
        assertThat(saved.getVersion()).isEqualTo(task.getVersion());
        assertThat(saved.getExecutionId()).isNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(saved.getStartedAt()).isNull();

        Instance second = start("worker");
        assertThat(second.process().pid()).isNotEqualTo(first.process().pid());
        UUID executionId = awaitRun(second, "CLAIMED", task.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, task.getId(), executionId, 1);
    }

    @Test
    @DisplayName("Another instance recovers the task after the claim commits")
    void recoversClaim() throws Exception {
        Task task = createTask(2500);
        Instance first = start("afterCommit");
        UUID oldExecutionId = awaitRun(first, "AFTER_COMMIT", task.getId());
        assertThat(findTask(task.getId()).getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);

        Instance second = recover(first, task.getId(), "worker");

        UUID executionId = awaitRun(second, "CLAIMED", task.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, task.getId(), executionId, 2);
    }

    @Test
    @DisplayName("Another instance recovers the task after a crash during execution")
    void recoversWork() throws Exception {
        Task task = createTask(2500);
        Instance first = start("duringWork");
        UUID oldExecutionId = awaitRun(first, "WORKING", task.getId());
        Task working = findTask(task.getId());
        assertThat(working.getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);
        assertThat(working.getProgress()).isBetween(1, 99);

        Instance second = recover(first, task.getId(), "worker");

        UUID executionId = awaitRun(second, "CLAIMED", task.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, task.getId(), executionId, 2);
    }

    @Test
    @DisplayName("A retry after an external effect uses the same operation key")
    void keepsEffect() throws Exception {
        Task task = createTask(1000);
        Instance first = start("afterEffect");
        UUID oldExecutionId = awaitRun(first, "EFFECT_DONE", task.getId());
        assertThat(findTask(task.getId()).getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);
        assertThat(findTask(task.getId()).getResult()).isNull();
        assertThat(countCalls(task.getId())).isEqualTo(1);
        assertThat(countEffects(task.getId())).isEqualTo(1);

        Instance second = recover(first, task.getId(), "effectWorker");

        UUID executionId = awaitRun(second, "CLAIMED", task.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        assertCompleted(second, task.getId(), executionId, 2);

        assertThat(countCalls(task.getId())).isEqualTo(2);
        assertThat(countEffects(task.getId())).isEqualTo(1);
    }

    @Test
    @DisplayName("The old instance does not overwrite the new attempt")
    void rejectsOldWorker() throws Exception {
        Task task = createTask(2500);
        Instance first = start("oldWorker");
        UUID oldExecutionId = awaitRun(first, "PAUSED", task.getId());
        assertThat(findTask(task.getId()).getProgress()).isEqualTo(20);
        expireLease(task.getId());
        Instance second = start("hold");
        assertThat(second.process().pid()).isNotEqualTo(first.process().pid());
        waitForRetry(second, task.getId(), 1);
        allowRetry(task.getId());
        UUID executionId = awaitRun(second, "HOLD", task.getId());
        assertThat(executionId).isNotEqualTo(oldExecutionId);
        Task before = findTask(task.getId());
        assertThat(before.getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);
        assertThat(before.getExecutionId()).isEqualTo(executionId);
        assertThat(before.getProgress()).isEqualTo(40);
        assertThat(before.getAttempts()).isEqualTo(2);

        proceed(first);

        assertThat(awaitMessage(first, "OLD_RESULT"))
            .isEqualTo("TEST:OLD_RESULT:false:false:false:END");
        assertThat(findTask(task.getId())).usingRecursiveComparison().isEqualTo(before);
        assertThat(first.process().isAlive()).isTrue();
        proceed(second);
        assertCompleted(second, task.getId(), executionId, 2);
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    @DisplayName("SIGTERM finishes active work, stops Kafka, and leaves queued work untouched")
    void shutsDown() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000
        ))) {
            admin.createTopics(List.of(
                new NewTopic("tasks", 1, (short) 1),
                new NewTopic("tasks.DLT", 1, (short) 1)
            )).all().get(15, TimeUnit.SECONDS);
        }

        Instance instance = start("shutdown");
        Task active = createTask(500);
        UUID executionId = awaitRun(instance, "WORKING", active.getId());
        Task queued = createTask(500);
        assertThat(findTask(active.getId()).getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);

        long startedAt = System.nanoTime();
        assertThat(instance.process().toHandle().destroy()).isTrue();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(instance.process().isAlive()).as(readLog(instance)).isTrue();
            assertThat(readLog(instance)).contains("Waiting for workers and Kafka listeners to stop");
        });
        proceed(instance);

        long budget = TimeUnit.SECONDS.toNanos(75);
        long remaining = budget - (System.nanoTime() - startedAt);
        assertThat(remaining).isPositive();
        assertThat(instance.process().waitFor(remaining, TimeUnit.NANOSECONDS))
            .as(readLog(instance)).isTrue();
        assertThat(System.nanoTime() - startedAt).isLessThan(budget);
        assertThat(instance.process().exitValue()).isEqualTo(143);
        assertThat(message(instance, "STOPPED")).isEqualTo("TEST:STOPPED:true:true:END");
        assertThat(message(instance, "DONE"))
            .isEqualTo("TEST:DONE:" + active.getId() + ":" + executionId + ":END");

        Task completed = findTask(active.getId());
        assertThat(completed.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(completed.getProgress()).isEqualTo(100);
        assertThat(completed.getResult()).isEqualTo("Task completed");
        assertThat(completed.getExecutionId()).isEqualTo(executionId);
        assertThat(completed.getAttempts()).isEqualTo(1);
        assertThat(completed.getFinishedAt()).isNotNull();
        assertThat(completed.getLeaseUntil()).isNull();

        Task waiting = findTask(queued.getId());
        assertThat(waiting.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(waiting.getAttempts()).isZero();
        assertThat(waiting.getExecutionId()).isNull();
        assertThat(waiting.getStartedAt()).isNull();
    }

    private Task createTask(long duration) {
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
        task.setName("Task");
        task.setDuration(duration);
        return taskRepository.saveAndFlush(task);
    }

    private Task findTask(Long id) {
        return taskRepository.findById(id).orElseThrow();
    }

    private Instance start(String mode) throws IOException {
        Path log = logs.resolve(mode + "-" + instances.size() + ".log");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty(
            "surefire.test.class.path", System.getProperty("java.class.path")
        );
        ProcessBuilder builder = new ProcessBuilder(
            java, "-cp", classpath,
            "com.example.taskapplication.service.execution.TaskTestInstance", mode
        );
        builder.environment().put("TEST_DB_URL", postgres.getJdbcUrl());
        builder.environment().put("TEST_DB_USERNAME", postgres.getUsername());
        builder.environment().put("TEST_DB_PASSWORD", postgres.getPassword());
        builder.environment().put("TEST_KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        builder.redirectErrorStream(true);
        builder.redirectOutput(log.toFile());
        Instance instance = new Instance(builder.start(), log);
        instances.add(instance);
        assertThat(awaitMessage(instance, "READY"))
            .isEqualTo("TEST:READY:" + instance.process().pid() + ":END");
        return instance;
    }

    private Instance recover(Instance first, Long id, String mode) throws Exception {
        int attempts = findTask(id).getAttempts();
        kill(first);
        assertThat(findTask(id).getStatus()).isEqualTo(TaskStatus.IN_PROGRESS);
        expireLease(id);
        Instance second = start(mode);
        assertThat(second.process().pid()).isNotEqualTo(first.process().pid());
        waitForRetry(second, id, attempts);
        allowRetry(id);
        return second;
    }

    private void waitForRetry(Instance instance, Long id, int attempts) {

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(instance.process().isAlive()).as(readLog(instance)).isTrue();
            assertThat(findTask(id).getStatus()).isEqualTo(TaskStatus.NEW);
        });
        Task saved = findTask(id);
        assertThat(saved.getAttempts()).isEqualTo(attempts);
        assertThat(saved.getExecutionId()).isNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(saved.getStartedAt()).isNull();
        assertThat(saved.getFinishedAt()).isNull();
        assertThat(saved.getNextRetryAt()).isNotNull();
        assertThat(saved.getProgress()).isZero();
    }

    private void expireLease(Long id) {
        jdbcTemplate.update("""
            UPDATE tasks
            SET lease_until = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private void allowRetry(Long id) {
        jdbcTemplate.update("""
            UPDATE tasks
            SET next_retry_at = CURRENT_TIMESTAMP - INTERVAL '1 second'
            WHERE id = ?
            """, id);
    }

    private UUID awaitRun(Instance instance, String stage, Long id) {
        String[] parts = awaitMessage(instance, stage).split(":");
        assertThat(Long.valueOf(parts[2])).isEqualTo(id);
        return UUID.fromString(parts[3]);
    }

    private void assertCompleted(Instance instance, Long id, UUID executionId, int attempts) {
        assertThat(awaitRun(instance, "DONE", id)).isEqualTo(executionId);
        Task saved = findTask(id);
        assertThat(saved.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(saved.getProgress()).isEqualTo(100);
        assertThat(saved.getAttempts()).isEqualTo(attempts);
        assertThat(saved.getExecutionId()).isEqualTo(executionId);
        assertThat(saved.getResult()).isEqualTo("Task completed");
        assertThat(saved.getFinishedAt()).isNotNull();
        assertThat(saved.getLeaseUntil()).isNull();
        assertThat(saved.getNextRetryAt()).isNull();
        assertThat(taskRepository.count()).isEqualTo(1);
        assertThat(instance.process().isAlive()).isTrue();
    }

    private String awaitMessage(Instance instance, String stage) {
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(instance.process().isAlive()).as(readLog(instance)).isTrue();
            assertThat(message(instance, stage)).as(readLog(instance)).isNotNull();
        });
        return message(instance, stage);
    }

    private String message(Instance instance, String stage) {
        return readLog(instance).lines()
            .filter(line -> line.startsWith("TEST:" + stage + ":") && line.endsWith(":END"))
            .findFirst()
            .orElse(null);
    }

    private String readLog(Instance instance) {
        try {
            return new String(Files.readAllBytes(instance.log()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void proceed(Instance instance) throws IOException {
        instance.process().getOutputStream().write("continue\n".getBytes(StandardCharsets.UTF_8));
        instance.process().getOutputStream().flush();
    }

    private void kill(Instance instance) throws InterruptedException {
        instance.process().destroyForcibly();
        assertThat(instance.process().waitFor(10, TimeUnit.SECONDS)).isTrue();
        assertThat(instance.process().isAlive()).isFalse();
    }

    private int countCalls(Long id) {
        return jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM test_calls WHERE task_id = ?", Integer.class, id
        );
    }

    private int countEffects(Long id) {
        return jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM test_effects WHERE task_id = ?", Integer.class, id
        );
    }

    private record Instance(Process process, Path log) {
    }
}
