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

class WorkerTimingPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(PropertiesConfiguration.class);

    @Test
    void usesThirtySecondDefaultsWhenNoSettingsAreProvided() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            WorkerTimingProperties properties = context.getBean(WorkerTimingProperties.class);
            assertThat(properties.leaseSeconds()).isEqualTo(30);
            assertThat(properties.retryBackoffSeconds()).isEqualTo(30);
            assertThat(properties.leaseNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(30));
        });
    }

    @Test
    void bindsCustomLeaseAndBackoffAndConvertsLeaseToNanos() {
        contextRunner.withPropertyValues("worker.lease-seconds=47", "worker.retry-backoff-seconds=7")
            .run(context -> {
                assertThat(context).hasNotFailed();
                WorkerTimingProperties properties = context.getBean(WorkerTimingProperties.class);
                assertThat(properties.leaseSeconds()).isEqualTo(47);
                assertThat(properties.retryBackoffSeconds()).isEqualTo(7);
                assertThat(properties.leaseNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(47));
            });
    }

    @Test
    void allowsZeroBackoffForImmediateRetry() {
        contextRunner.withPropertyValues("worker.retry-backoff-seconds=0").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(WorkerTimingProperties.class).retryBackoffSeconds()).isZero();
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

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(WorkerTimingProperties.class)
    static class PropertiesConfiguration {
    }
}
