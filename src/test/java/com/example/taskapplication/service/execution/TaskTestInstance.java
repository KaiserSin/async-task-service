package com.example.taskapplication.service.execution;

import com.example.taskapplication.TaskApplication;
import com.example.taskapplication.config.TransactionConfig;
import com.example.taskapplication.config.WorkerConfig;
import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.service.TaskPoller;
import com.example.taskapplication.service.maintenance.MaintenanceScheduler;
import com.example.taskapplication.service.maintenance.TaskCleanupService;
import com.example.taskapplication.service.maintenance.TaskRecoveryService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

public class TaskTestInstance {

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        boolean shutdown = mode.equals("shutdown") || mode.equals("shutdownInterrupted");
        boolean worker = switch (mode) {
            case "worker", "hold", "effectWorker", "shutdown", "shutdownInterrupted" -> true;
            default -> false;
        };

        SpringApplicationBuilder application = new SpringApplicationBuilder(
            shutdown ? TaskApplication.class : Config.class
        ).profiles("test");
        if (shutdown) {
            application.sources(Shutdown.class).initializers(context ->
                context.getBeanFactory().registerSingleton("shutdownTestTypeExcludeFilter", new TypeExcludeFilter() {
                    @Override
                    public boolean match(MetadataReader reader, MetadataReaderFactory factory) {
                        return reader.getAnnotationMetadata().isAnnotated(TestComponent.class.getName());
                    }
                })
            );
        }

        List<String> arguments = new ArrayList<>(List.of(
            "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
            "--spring.datasource.username=" + System.getenv("TEST_DB_USERNAME"),
            "--spring.datasource.password=" + System.getenv("TEST_DB_PASSWORD"),
            "--spring.kafka.bootstrap-servers=" + System.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"),
            "--server.port=0",
            "--spring.datasource.hikari.maximum-pool-size=3",
            "--spring.datasource.hikari.minimum-idle=0",
            "--spring.datasource.hikari.register-mbeans=false",
            "--spring.jpa.hibernate.ddl-auto=validate",
            "--spring.main.banner-mode=off",
            "--spring.main.log-startup-info=false",
            "--logging.level.root=WARN",
            "--logging.level.com.example.taskapplication.service.execution.WorkerLifecycle=" + (shutdown ? "INFO" : "WARN"),
            "--logging.level.org.hibernate.SQL=OFF",
            "--test.mode=" + mode,
            "--test.worker=" + worker,
            "--worker.pool.size=1",
            "--worker.poll-delay-ms=100",
            "--worker.recovery-delay-ms=100",
            "--worker.cleanup.cron=-"
        ));
        if (mode.equals("beforeCommit")) {
            arguments.add("--worker.claim-timeout-seconds=60");
        }
        if (!shutdown || mode.equals("shutdownInterrupted")) {
            arguments.add("--worker.shutdown.grace-period-seconds=1");
            arguments.add("--worker.shutdown.interrupt-wait-seconds=" + (shutdown ? 10 : 1));
        }

        try (ConfigurableApplicationContext context = application
            .web(shutdown ? WebApplicationType.SERVLET : WebApplicationType.NONE)
            .run(arguments.toArray(String[]::new))) {
            if (shutdown) {
                var registry = context.getBean(KafkaListenerEndpointRegistry.class);
                await().atMost(Duration.ofSeconds(30)).until(() ->
                    !registry.getListenerContainers().isEmpty()
                        && registry.getListenerContainers().stream().allMatch(container ->
                            container.isRunning() && container.getAssignedPartitions() != null
                                && !container.getAssignedPartitions().isEmpty())
                );
            }
            signal("READY", Long.toString(ProcessHandle.current().pid()));
            if (worker) {

                new CountDownLatch(1).await();
                return;
            }

            TaskRun run = context.getBean(TaskClaimService.class).claimNextTask().orElseThrow();
            TaskUpdateService updates = context.getBean(TaskUpdateService.class);
            switch (mode) {
                case "beforeCommit" -> throw new IllegalStateException("Expected a crash before commit");
                case "afterCommit" -> {
                    signal("AFTER_COMMIT", run);
                    awaitContinue();
                }
                case "duringWork", "afterEffect" -> context.getBean(TaskRunner.class).executeTask(run);
                case "oldWorker" -> {
                    if (!updates.updateProgress(run.id(), run.executionId(), 20)) {
                        throw new IllegalStateException("Failed to save progress for the old attempt");
                    }
                    signal("PAUSED", run);
                    awaitContinue();
                    boolean progress = updates.updateProgress(run.id(), run.executionId(), 50);
                    boolean completed = updates.updateTaskStatus(
                        run.id(), run.executionId(), TaskStatus.COMPLETED, 100, "Old result"
                    );
                    boolean failed = updates.updateTaskStatus(
                        run.id(), run.executionId(), TaskStatus.FAILED, 50, "Old error"
                    );
                    signal("OLD_RESULT", progress + ":" + completed + ":" + failed);

                    new CountDownLatch(1).await();
                }
                default -> throw new IllegalArgumentException("Unknown mode: " + mode);
            }
        }
    }

    private static void signal(String event, TaskRun run) {
        signal(event, run.id() + ":" + run.executionId());
    }

    private static void signal(String event, String value) {
        System.out.println("TEST:" + event + ":" + value + ":END");
        System.out.flush();
    }

    private static void awaitContinue() {
        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            if (!"continue".equals(input.readLine())) {
                throw new IllegalStateException("No command received to continue the test");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read the test command", e);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
    @EnableConfigurationProperties(WorkerProperties.class)
    @EntityScan(basePackageClasses = Task.class)
    @EnableJpaRepositories(basePackageClasses = TaskRepository.class)
    @Import({TaskClaimService.class, TaskUpdateService.class, TaskRecoveryService.class, TaskMetrics.class, Worker.class})
    static class Config {

        @Bean
        TransactionTemplate claimTransactionTemplate(
            PlatformTransactionManager transactionManager,
            WorkerProperties workerProperties,
            @Value("${test.mode}") String mode
        ) {
            TransactionTemplate settings = new TransactionConfig()
                .claimTransactionTemplate(transactionManager, workerProperties);
            return new TransactionTemplate(transactionManager, settings) {
                @Override
                public <T> T execute(TransactionCallback<T> action) {
                    return super.execute(status -> {
                        T result = action.doInTransaction(status);
                        if (mode.equals("beforeCommit")
                            && result instanceof Optional<?> value
                            && value.orElse(null) instanceof TaskRun run) {

                            signal("BEFORE_COMMIT", run);
                            awaitContinue();
                        }
                        return result;
                    });
                }
            };
        }

        @Bean
        TaskRunner taskRunner(
            TaskUpdateService updates,
            TaskMetrics taskMetrics,
            WorkerProperties workerProperties,
            JdbcTemplate jdbcTemplate,
            @Value("${test.mode}") String mode
        ) {
            DemoTaskHandler demo = new DemoTaskHandler();
            TaskHandler handler = (run, elapsedMillis, stepMillis) -> {
                if (mode.equals("duringWork") && elapsedMillis == 1000L) {
                    signal("WORKING", run);
                    awaitContinue();
                }
                demo.handleStep(run, elapsedMillis, stepMillis);
                if (mode.equals("afterEffect") || mode.equals("effectWorker")) {
                    jdbcTemplate.update("INSERT INTO test_calls (task_id) VALUES (?)", run.id());
                    jdbcTemplate.update("""
                        INSERT INTO test_effects (task_id) VALUES (?)
                        ON CONFLICT (task_id) DO NOTHING
                        """, run.id());
                    if (mode.equals("afterEffect")) {
                        signal("EFFECT_DONE", run);
                        awaitContinue();
                    }
                }
            };
            return new TaskRunner(updates, taskMetrics, handler, workerProperties) {
                @Override
                public void executeTask(TaskRun run) {
                    signal("CLAIMED", run);
                    if (mode.equals("hold")) {
                        if (!updates.updateProgress(run.id(), run.executionId(), 40)) {
                            throw new IllegalStateException("Failed to save progress for the new attempt");
                        }
                        signal("HOLD", run);
                        awaitContinue();
                    }
                    super.executeTask(run);
                    signal("DONE", run);
                }
            };
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "test.worker", havingValue = "true")
    @EnableScheduling
    @Import({WorkerConfig.class, TaskPoller.class, MaintenanceScheduler.class, WorkerLifecycle.class,
        TaskCleanupService.class})
    static class Worker {

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {

            return new KafkaListenerEndpointRegistry();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Shutdown {

        @Bean
        @Primary
        TaskRunner shutdownTaskRunner(
            TaskUpdateService updates,
            TaskMetrics metrics,
            WorkerProperties workerProperties,
            @Value("${test.mode}") String mode
        ) {
            AtomicBoolean started = new AtomicBoolean();
            DemoTaskHandler demo = new DemoTaskHandler();
            TaskHandler handler = (run, elapsedMillis, stepMillis) -> {
                if (started.compareAndSet(false, true)) {
                    signal("WORKING", run);
                    if (mode.equals("shutdown")) {
                        awaitContinue();
                    }
                }
                demo.handleStep(run, elapsedMillis, stepMillis);
            };
            return new TaskRunner(updates, metrics, handler, workerProperties) {
                @Override
                public void executeTask(TaskRun run) {
                    super.executeTask(run);
                    signal("DONE", run);
                }
            };
        }

        @Bean
        ApplicationListener<ContextClosedEvent> shutdownState(
            ThreadPoolExecutor taskExecutor,
            KafkaListenerEndpointRegistry registry
        ) {
            return event -> signal("STOPPED", taskExecutor.isTerminated() + ":"
                + registry.getListenerContainers().stream().noneMatch(container -> container.isRunning()));
        }
    }

}
