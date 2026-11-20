package com.example.taskapplication.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(PropertiesConfiguration.class);

    @Test
    void usesDefaultsWhenNoSettingsAreProvided() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            WorkerProperties properties = context.getBean(WorkerProperties.class);
            assertThat(properties.leaseSeconds()).isEqualTo(30);
            assertThat(properties.retryBackoffSeconds()).isEqualTo(30);
            assertThat(properties.claimTimeoutSeconds()).isEqualTo(5);
            assertThat(properties.pool().size()).isEqualTo(4);
            assertThat(properties.shutdown().gracePeriodSeconds()).isEqualTo(75);
            assertThat(properties.shutdown().interruptWaitSeconds()).isEqualTo(10);
            assertThat(properties.cleanup().retentionDays()).isEqualTo(30);
            assertThat(properties.cleanup().failedRetentionDays()).isEqualTo(90);
            assertThat(properties.cleanup().batchSize()).isEqualTo(500);
            assertThat(properties.leaseNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(30));
        });
    }

    @Test
    void bindsCustomSettingsAndConvertsLeaseToNanos() {
        contextRunner.withPropertyValues(
                "worker.lease-seconds=47", "worker.retry-backoff-seconds=7",
                "worker.claim-timeout-seconds=9", "worker.pool.size=8",
                "worker.shutdown.grace-period-seconds=60", "worker.shutdown.interrupt-wait-seconds=15",
                "worker.cleanup.retention-days=14", "worker.cleanup.failed-retention-days=60",
                "worker.cleanup.batch-size=100")
            .run(context -> {
                assertThat(context).hasNotFailed();
                WorkerProperties properties = context.getBean(WorkerProperties.class);
                assertThat(properties.leaseSeconds()).isEqualTo(47);
                assertThat(properties.retryBackoffSeconds()).isEqualTo(7);
                assertThat(properties.claimTimeoutSeconds()).isEqualTo(9);
                assertThat(properties.pool().size()).isEqualTo(8);
                assertThat(properties.shutdown().gracePeriodSeconds()).isEqualTo(60);
                assertThat(properties.shutdown().interruptWaitSeconds()).isEqualTo(15);
                assertThat(properties.cleanup().retentionDays()).isEqualTo(14);
                assertThat(properties.cleanup().failedRetentionDays()).isEqualTo(60);
                assertThat(properties.cleanup().batchSize()).isEqualTo(100);
                assertThat(properties.leaseNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(47));
            });
    }

    @Test
    void allowsZeroBackoffForImmediateRetry() {
        contextRunner.withPropertyValues("worker.retry-backoff-seconds=0").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(WorkerProperties.class).retryBackoffSeconds()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsNonPositiveLease(int leaseSeconds) {
        contextRunner.withPropertyValues("worker.lease-seconds=" + leaseSeconds).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @Test
    void rejectsNegativeBackoff() {
        contextRunner.withPropertyValues("worker.retry-backoff-seconds=-1").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "worker.pool.size=0",
        "worker.pool.size=-1",
        "worker.claim-timeout-seconds=0",
        "worker.claim-timeout-seconds=-1",
        "worker.shutdown.grace-period-seconds=0",
        "worker.shutdown.grace-period-seconds=-1",
        "worker.shutdown.interrupt-wait-seconds=0",
        "worker.shutdown.interrupt-wait-seconds=-1"
    })
    void rejectsInvalidWorkerSettings(String property) {
        contextRunner.withPropertyValues(property).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(WorkerProperties.class)
    static class PropertiesConfiguration {
    }
}
