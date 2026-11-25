package com.example.payment.worker;

import com.example.payment.worker.attempt.PaymentClaimService;
import com.example.payment.worker.attempt.PaymentHandler;
import com.example.payment.worker.attempt.PaymentAttempt;
import com.example.payment.worker.attempt.PaymentRunner;
import com.example.payment.worker.attempt.PaymentUpdateService;
import com.example.payment.worker.attempt.StripeStub;
import com.example.payment.PaymentApplication;
import com.example.payment.api.security.SecurityConfig;
import com.example.payment.webhook.StripeWebhookController;
import com.example.payment.observability.PaymentMetrics;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.webhook.StripeWebhookEventRepository;
import com.example.payment.webhook.StripeWebhookProcessor;
import com.example.payment.maintenance.MaintenanceScheduler;
import com.example.payment.maintenance.PaymentCleanupService;
import com.example.payment.maintenance.PaymentRecoveryService;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
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

import com.example.payment.worker.config.WorkerConfig;
import com.example.payment.worker.config.WorkerTransactionConfig;

import static org.awaitility.Awaitility.await;

public class PaymentTestInstance {

    public static void main(String[] args) throws Exception {
        String mode = args[0];
        boolean shutdown = mode.equals("shutdown") || mode.equals("shutdownInterrupted");
        boolean receiveWebhooks = mode.equals("webhookReceiver");
        boolean recoverWebhooks = mode.equals("webhookRecovery");
        boolean worker = switch (mode) {
            case "worker", "hold", "effectWorker", "shutdown", "shutdownInterrupted" -> true;
            default -> false;
        };

        SpringApplicationBuilder application = new SpringApplicationBuilder(
            shutdown ? PaymentApplication.class : Config.class
        ).profiles("test");
        if (shutdown) {
            application.sources(Shutdown.class).initializers(context ->
                context.getBeanFactory().registerSingleton("shutdownTestTypeExcludeFilter", new TypeExcludeFilter() {
                    @Override
                    public boolean match(MetadataReader reader, MetadataReaderFactory factory) {
                        return reader.getAnnotationMetadata().isAnnotated(TestComponent.class.getName())
                            || StripeStub.replaces(reader.getClassMetadata().getClassName());
                    }
                })
            );
        }

        if (receiveWebhooks) {
            application.sources(WebhookReceiver.class);
        } else if (recoverWebhooks) {
            application.sources(WebhookRecovery.class);
        }

        List<String> arguments = new ArrayList<>(List.of(
            "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
            "--spring.datasource.username=" + System.getenv("TEST_DB_USERNAME"),
            "--spring.datasource.password=" + System.getenv("TEST_DB_PASSWORD"),
            "--spring.kafka.bootstrap-servers=" + System.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"),
            "--server.port=0",
            "--stripe.secret-key=sk_test_recovery_fixture",
            "--stripe.webhook-secret=whsec_recovery_fixture",
            "--spring.datasource.hikari.maximum-pool-size=3",
            "--spring.datasource.hikari.minimum-idle=0",
            "--spring.datasource.hikari.register-mbeans=false",
            "--spring.jpa.hibernate.ddl-auto=validate",
            "--spring.main.banner-mode=off",
            "--spring.main.log-startup-info=false",
            "--logging.level.root=WARN",
            "--logging.level.com.example.payment.worker.WorkerLifecycle=" + (shutdown ? "INFO" : "WARN"),
            "--logging.level.org.hibernate.SQL=OFF",
            "--test.mode=" + mode,
            "--test.worker=" + worker,
            "--worker.pool.size=1",
            "--worker.poll-delay-ms=100",
            "--worker.recovery-delay-ms=100",
            "--worker.cleanup.cron=-"
        ));
        if (receiveWebhooks) {
            arguments.add("--server.address=127.0.0.1");
            arguments.add("--worker.lease-seconds=5");
        }
        if (mode.equals("beforeCommit") || receiveWebhooks) {
            arguments.add("--worker.claim-timeout-seconds=60");
        }
        if (!shutdown || mode.equals("shutdownInterrupted")) {
            arguments.add("--worker.shutdown.grace-period-seconds=1");
            arguments.add("--worker.shutdown.interrupt-wait-seconds=" + (shutdown ? 10 : 1));
        }

        try (ConfigurableApplicationContext context = application
            .web(shutdown || receiveWebhooks ? WebApplicationType.SERVLET : WebApplicationType.NONE)
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
            if (receiveWebhooks) {
                int port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
                signal("WEBHOOK_PORT", Integer.toString(port));
                awaitContinue();
                context.getBean(StripeWebhookProcessor.class).processNextEvent();
                return;
            }
            if (worker || recoverWebhooks) {
                new CountDownLatch(1).await();
                return;
            }

            PaymentAttempt attempt = context.getBean(PaymentClaimService.class).claimNextPayment().orElseThrow();
            PaymentUpdateService updates = context.getBean(PaymentUpdateService.class);
            switch (mode) {
                case "beforeCommit" -> throw new IllegalStateException("Expected a crash before commit");
                case "afterCommit" -> {
                    signal("AFTER_COMMIT", attempt);
                    awaitContinue();
                }
                case "duringWork", "afterEffect" -> context.getBean(PaymentRunner.class).executePayment(attempt);
                case "oldWorker" -> {
                    if (!updates.renewLease(attempt.id(), attempt.executionId())) {
                        throw new IllegalStateException("Failed to renew the old attempt lease");
                    }
                    signal("PAUSED", attempt);
                    awaitContinue();
                    boolean renewed = updates.renewLease(attempt.id(), attempt.executionId());
                    boolean completed = updates.complete(attempt.id(), attempt.executionId(), "Old result");
                    boolean failed = updates.fail(attempt.id(), attempt.executionId(), "Old error").isPresent();
                    signal("OLD_RESULT", renewed + ":" + completed + ":" + failed);

                    new CountDownLatch(1).await();
                }
                default -> throw new IllegalArgumentException("Unknown mode: " + mode);
            }
        }
    }

    private static void signal(String event, PaymentAttempt attempt) {
        signal(event, attempt.id() + ":" + attempt.executionId());
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
    @EntityScan(basePackageClasses = Payment.class)
    @EnableJpaRepositories(basePackageClasses = {PaymentRepository.class, PaymentMetrics.class})
    @Import({PaymentClaimService.class, PaymentUpdateService.class, PaymentRecoveryService.class, PaymentMetrics.class, Worker.class, StripeStub.class})
    static class Config {
        @Bean
        TransactionTemplate workerTransactionTemplate(
            PlatformTransactionManager transactionManager,
            WorkerProperties workerProperties,
            @Value("${test.mode}") String mode
        ) {
            TransactionTemplate settings = new WorkerTransactionConfig()
                .workerTransactionTemplate(transactionManager, workerProperties);
            return new TransactionTemplate(transactionManager, settings) {
                @Override
                public <T> T execute(TransactionCallback<T> action) {
                    return super.execute(status -> {
                        T result = action.doInTransaction(status);
                        if (mode.equals("beforeCommit")
                            && result instanceof Optional<?> value
                            && value.orElse(null) instanceof PaymentAttempt attempt) {

                            signal("BEFORE_COMMIT", attempt);
                            awaitContinue();
                        }
                        return result;
                    });
                }
            };
        }

        @Bean
        @Primary
        PaymentHandler testPaymentHandler(JdbcTemplate jdbcTemplate, @Value("${test.mode}") String mode) {
            return attempt -> {
                if (mode.equals("duringWork")) {
                    signal("WORKING", attempt);
                    awaitContinue();
                }
                Thread.sleep(1000);
                if (mode.equals("afterEffect") || mode.equals("effectWorker")) {
                    jdbcTemplate.update("INSERT INTO test_calls (payment_id) VALUES (?)", attempt.id());
                    jdbcTemplate.update("""
                        INSERT INTO test_effects (payment_id) VALUES (?)
                        ON CONFLICT (payment_id) DO NOTHING
                        """, attempt.id());
                    if (mode.equals("afterEffect")) {
                        signal("EFFECT_DONE", attempt);
                        awaitContinue();
                    }
                }
            };
        }

        @Bean
        PaymentRunner paymentRunner(
            PaymentUpdateService updates,
            PaymentMetrics paymentMetrics,
            PaymentHandler handler,
            @Value("${test.mode}") String mode
        ) {
            return new PaymentRunner(updates, paymentMetrics, handler) {
                @Override
                public void executePayment(PaymentAttempt attempt) {
                    signal("CLAIMED", attempt);
                    if (mode.equals("hold")) {
                        if (!updates.renewLease(attempt.id(), attempt.executionId())) {
                            throw new IllegalStateException("Failed to renew the new attempt lease");
                        }
                        signal("HOLD", attempt);
                        awaitContinue();
                    }
                    super.executePayment(attempt);
                    signal("DONE", attempt);
                }
            };
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @Import({SecurityConfig.class, StripeWebhookController.class, StripeWebhookProcessor.class})
    static class WebhookReceiver {
        @Bean
        StripeWebhookEventRepository webhookEvents(JdbcTemplate jdbc) {
            return new StripeWebhookEventRepository(jdbc) {
                @Override
                public void finishEvent(PendingEvent event, PaymentStatus status) {
                    super.finishEvent(event, status);
                    String eventId = jdbc.queryForObject("""
                        SELECT event_id FROM stripe_webhook_events
                        WHERE event_id = ? AND processed_at IS NOT NULL AND payload IS NULL
                        """, String.class, event.eventId());
                    signal("BEFORE_WEBHOOK_COMMIT", eventId);
                    awaitContinue();
                }
            };
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableScheduling
    @Import({StripeWebhookEventRepository.class, StripeWebhookProcessor.class})
    static class WebhookRecovery {
    }

    @TestConfiguration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "test.worker", havingValue = "true")
    @EnableScheduling
    @Import({WorkerConfig.class, PaymentPoller.class, MaintenanceScheduler.class, WorkerLifecycle.class,
        PaymentCleanupService.class})
    static class Worker {

        @Bean
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {

            return new KafkaListenerEndpointRegistry();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    @Import({PaymentPoller.class, StripeStub.class})
    static class Shutdown {
        @Bean
        @Primary
        PaymentHandler shutdownTestPaymentHandler(@Value("${test.mode}") String mode) {
            AtomicBoolean started = new AtomicBoolean();
            return attempt -> {
                if (started.compareAndSet(false, true)) {
                    signal("WORKING", attempt);
                    if (mode.equals("shutdown")) {
                        awaitContinue();
                    }
                }
                Thread.sleep(mode.equals("shutdownInterrupted") ? 60_000 : 1000);
            };
        }

        @Bean
        @Primary
        PaymentRunner shutdownPaymentRunner(
            PaymentUpdateService updates,
            PaymentMetrics metrics,
            PaymentHandler handler
        ) {
            return new PaymentRunner(updates, metrics, handler) {
                @Override
                public void executePayment(PaymentAttempt attempt) {
                    super.executePayment(attempt);
                    signal("DONE", attempt);
                }
            };
        }

        @Bean
        DisposableBean shutdownState(
            ThreadPoolExecutor paymentExecutor,
            KafkaListenerEndpointRegistry registry
        ) {
            return () -> signal("STOPPED", paymentExecutor.isTerminated() + ":"
                + registry.getListenerContainers().stream().noneMatch(container -> container.isRunning()));
        }
    }

}
