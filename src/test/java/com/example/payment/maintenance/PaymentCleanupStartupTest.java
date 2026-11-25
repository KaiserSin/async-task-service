package com.example.payment.maintenance;

import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PaymentCleanupStartupTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withBean(PaymentRepository.class, () -> mock(PaymentRepository.class))
        .withBean("cleanupTransactionTemplate", TransactionTemplate.class,
            () -> new TransactionTemplate(mock(PlatformTransactionManager.class)))
        .withUserConfiguration(PropertiesConfiguration.class, PaymentCleanupService.class);

    @Test
    void startsWithDefaultCleanupSettings() {
        contextRunner.run(context -> assertThat(context).hasNotFailed().hasSingleBean(PaymentCleanupService.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "worker.cleanup.retention-days=0",
        "worker.cleanup.retention-days=-1",
        "worker.cleanup.failed-retention-days=0",
        "worker.cleanup.failed-retention-days=-1",
        "worker.cleanup.batch-size=0",
        "worker.cleanup.batch-size=-1"
    })
    void rejectsInvalidCleanupSettingsAtStartup(String property) {
        contextRunner.withPropertyValues(property).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                .hasRootCauseInstanceOf(BindValidationException.class)
                .hasStackTraceContaining("cleanup.");
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(WorkerProperties.class)
    static class PropertiesConfiguration {
    }
}
