package com.example.taskapplication.load;

import com.example.taskapplication.TaskApplication;
import com.example.taskapplication.model.TaskStatus;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock("task-load")

abstract class TaskLoadSupport {

    @Container
    protected static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    protected static ConfluentKafkaContainer kafka =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:7.5.16")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    protected final List<ConfigurableApplicationContext> instances = new ArrayList<>();
    protected JdbcTemplate jdbc;
    protected TaskLoadClient client;
    private AdminClient admin;
    private HttpClient http;
    private String topic;
    private String group;
    private boolean databasePaused;

    @BeforeEach
    void setUpLoad() throws Exception {

        var dataSource = new DriverManagerDataSource(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()
        );
        Properties timeouts = new Properties();
        timeouts.setProperty("connectTimeout", "5");
        timeouts.setProperty("socketTimeout", "10");
        timeouts.setProperty("options", "-c statement_timeout=5000 -c lock_timeout=2000");
        dataSource.setConnectionProperties(timeouts);
        jdbc = new JdbcTemplate(dataSource);
        topic = "tasks-load-" + UUID.randomUUID();
        group = "group-" + UUID.randomUUID();
        admin = AdminClient.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000
        ));
        admin.createTopics(List.of(new NewTopic(topic, 3, (short) 1),
            new NewTopic(topic + ".DLT", 3, (short) 1))).all().get(20, TimeUnit.SECONDS);
        http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        client = new TaskLoadClient(kafka.getBootstrapServers(), topic, jdbc);
    }

    protected void startInstances(int count) {
        for (int i = 0; i < count; i++) {

            instances.add(new SpringApplicationBuilder(TaskApplication.class)
                .registerShutdownHook(false)
                .initializers(context -> context.getBeanFactory().registerSingleton(
                    "loadTestTypeExcludeFilter", new TypeExcludeFilter() {
                        @Override
                        public boolean match(MetadataReader reader, MetadataReaderFactory factory) {

                            return reader.getAnnotationMetadata().isAnnotated(TestComponent.class.getName());
                        }
                    }))
                .run(
                    "--server.port=0",
                    "--spring.datasource.url=" + postgres.getJdbcUrl(),
                    "--spring.datasource.username=" + postgres.getUsername(),
                    "--spring.datasource.password=" + postgres.getPassword(),
                    "--spring.datasource.hikari.register-mbeans=false",
                    "--spring.datasource.hikari.pool-name=LoadPool-" + i,
                    "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                    "--spring.kafka.consumer.group-id=" + group,
                    "--kafka.topic=" + topic,
                    "--worker.pool.size=4",
                    "--worker.cleanup.cron=-",
                    "--worker.shutdown.grace-period-seconds=5",
                    "--worker.shutdown.interrupt-wait-seconds=5",
                    "--spring.jmx.enabled=false",
                    "--spring.main.banner-mode=off",
                    "--logging.level.root=WARN",
                    "--logging.level.org.hibernate.SQL=OFF"
                ));
            if (i == 0) {
                jdbc.update("DELETE FROM tasks");
            }
        }
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            int assigned = 0;
            for (var instance : instances) {
                var containers = instance.getBean(KafkaListenerEndpointRegistry.class).getListenerContainers();
                assertThat(containers).hasSize(1);
                var container = containers.iterator().next();
                assertThat(container.isRunning()).isTrue();
                assertThat(container.getAssignedPartitions()).isNotEmpty();
                assigned += container.getAssignedPartitions().size();
            }
            assertThat(assigned).isEqualTo(3);
        });
    }

    protected int count(TaskStatus status) {
        return jdbc.queryForObject("SELECT count(*) FROM tasks WHERE status = ?", Integer.class, status.name());
    }

    protected int activeWorkers() {
        return instances.stream().mapToInt(instance ->
            instance.getBean(ThreadPoolExecutor.class).getActiveCount()).sum();
    }

    protected void assertFinished(int expected, boolean singleAttempt) throws Exception {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tasks", Integer.class)).isEqualTo(expected);
        assertThat(jdbc.queryForObject("SELECT count(DISTINCT event_id) FROM tasks", Integer.class))
            .isEqualTo(expected);
        assertThat(count(TaskStatus.COMPLETED)).isEqualTo(expected);
        assertThat(jdbc.queryForObject("""
            SELECT count(*) FROM tasks
            WHERE progress <> 100 OR finished_at IS NULL OR lease_until IS NOT NULL
               OR attempts < 1 OR attempts > max_attempts
            """, Integer.class)).isZero();
        if (singleAttempt) {
            assertThat(jdbc.queryForObject("SELECT count(*) FROM tasks WHERE attempts <> 1", Integer.class))
                .isZero();
        }
        awaitOffsets();
        assertNoDlt();
    }

    private void awaitOffsets() {
        Map<TopicPartition, OffsetSpec> offsets = new HashMap<>();
        for (int partition = 0; partition < 3; partition++) {
            offsets.put(new TopicPartition(topic, partition), OffsetSpec.latest());
        }
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            var ends = admin.listOffsets(offsets).all().get(5, TimeUnit.SECONDS);
            var committed = admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
            for (var entry : ends.entrySet()) {
                var offset = committed.get(entry.getKey());
                assertThat(offset == null ? 0L : offset.offset()).isEqualTo(entry.getValue().offset());
            }
        });
    }

    protected void assertNoDlt() throws Exception {
        Map<TopicPartition, OffsetSpec> offsets = new HashMap<>();
        for (int partition = 0; partition < 3; partition++) {
            offsets.put(new TopicPartition(topic + ".DLT", partition), OffsetSpec.latest());
        }
        var results = admin.listOffsets(offsets).all().get(10, TimeUnit.SECONDS);
        assertThat(results.values()).allSatisfy(offset -> assertThat(offset.offset()).isZero());
    }

    protected HttpResponse<String> response(int instanceIndex, String path) throws Exception {
        var context = (ServletWebServerApplicationContext) instances.get(instanceIndex);
        var request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + context.getWebServer().getPort() + path))
            .timeout(Duration.ofSeconds(15)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    protected void pauseDatabase() {
        DockerClientFactory.instance().client().pauseContainerCmd(postgres.getContainerId()).exec();
        databasePaused = true;
    }

    protected void resumeDatabase() {
        if (databasePaused) {
            DockerClientFactory.instance().client().unpauseContainerCmd(postgres.getContainerId()).exec();
            databasePaused = false;
        }
    }

    @AfterEach
    void stopLoad() throws Exception {
        List<AutoCloseable> resources = new ArrayList<>();
        resources.add(this::resumeDatabase);
        if (client != null) {
            resources.add(client);
        }
        resources.addAll(instances.reversed());
        if (http != null) {
            resources.add(http);
        }
        if (admin != null) {
            resources.add(() -> admin.close(Duration.ofSeconds(5)));
        }
        Exception failure = null;
        for (var resource : resources) {
            try {
                resource.close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
