package com.example.payment.maintenance;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.worker.WorkerLifecycle;
import org.junit.jupiter.api.Test;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@Testcontainers
class PaymentCleanupLockTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void onlyOneInstanceCleansUpAndAnotherCanRunAfterTheMinimumLeaseExpires() throws Exception {
        DataSource dataSource = new DriverManagerDataSource(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        JdbcTemplate jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("""
            CREATE TABLE shedlock (
                name VARCHAR(64) PRIMARY KEY,
                lock_until TIMESTAMP NOT NULL,
                locked_at TIMESTAMP NOT NULL,
                locked_by VARCHAR(255) NOT NULL
            )
            """);
        PaymentCleanupService firstCleanup = mock(PaymentCleanupService.class);
        PaymentCleanupService secondCleanup = mock(PaymentCleanupService.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            return 1;
        }).when(firstCleanup).archiveExpiredPayments();
        ExecutorService worker = Executors.newSingleThreadExecutor();

        try (var firstContext = instance(dataSource, firstCleanup);
             var secondContext = instance(dataSource, secondCleanup)) {
            MaintenanceScheduler first = firstContext.getBean(MaintenanceScheduler.class);
            MaintenanceScheduler second = secondContext.getBean(MaintenanceScheduler.class);
            assertThat(AopUtils.isAopProxy(first)).isTrue();
            assertThat(AopUtils.isAopProxy(second)).isTrue();

            var firstRun = worker.submit(first::runCleanup);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(jdbc.queryForObject("""
                    SELECT COUNT(*) FROM shedlock
                    WHERE name = 'payment-cleanup'
                      AND lock_until > timezone('UTC', CURRENT_TIMESTAMP)
                    """, Integer.class)).isEqualTo(1);

                second.runCleanup();

                verify(secondCleanup, never()).archiveExpiredPayments();
            } finally {
                release.countDown();
                firstRun.get(5, TimeUnit.SECONDS);
            }
            verify(firstCleanup).archiveExpiredPayments();

            second.runCleanup();
            verify(secondCleanup, never()).archiveExpiredPayments();

            assertThat(jdbc.update("""
                UPDATE shedlock
                SET lock_until = timezone('UTC', CURRENT_TIMESTAMP) - INTERVAL '1 second'
                WHERE name = 'payment-cleanup'
                """)).isEqualTo(1);

            second.runCleanup();

            verify(secondCleanup).archiveExpiredPayments();
        } finally {
            release.countDown();
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private AnnotationConfigApplicationContext instance(DataSource dataSource, PaymentCleanupService cleanup) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(DataSource.class, () -> dataSource);
        context.registerBean(PaymentCleanupService.class, () -> cleanup);
        context.registerBean(MaintenanceScheduler.class, () -> new MaintenanceScheduler(
            mock(WorkerLifecycle.class), mock(PaymentRecoveryService.class), cleanup, mock(PaymentMetrics.class)));
        context.register(ShedLockConfig.class);
        try {
            context.refresh();
            return context;
        } catch (RuntimeException | Error failure) {
            context.close();
            throw failure;
        }
    }
}
