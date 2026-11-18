package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.config.ShedLockConfig;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.service.execution.WorkerLifecycle;
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
class TaskCleanupLockTest {

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
        TaskCleanupService firstCleanup = mock(TaskCleanupService.class);
        TaskCleanupService secondCleanup = mock(TaskCleanupService.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            return 1;
        }).when(firstCleanup).deleteExpiredTasks();
        ExecutorService worker = Executors.newSingleThreadExecutor();

        try (var firstContext = instance(dataSource, firstCleanup);
             var secondContext = instance(dataSource, secondCleanup)) {
            MaintenanceScheduler first = firstContext.getBean(MaintenanceScheduler.class);
            MaintenanceScheduler second = secondContext.getBean(MaintenanceScheduler.class);
            assertThat(AopUtils.isAopProxy(first)).isTrue();
            assertThat(AopUtils.isAopProxy(second)).isTrue();

            var firstRun = worker.submit(first::cleanupFinishedTasks);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(jdbc.queryForObject("""
                    SELECT COUNT(*) FROM shedlock
                    WHERE name = 'task-cleanup'
                      AND lock_until > timezone('UTC', CURRENT_TIMESTAMP)
                    """, Integer.class)).isEqualTo(1);

                second.cleanupFinishedTasks();

                verify(secondCleanup, never()).deleteExpiredTasks();
            } finally {
                release.countDown();
                firstRun.get(5, TimeUnit.SECONDS);
            }
            verify(firstCleanup).deleteExpiredTasks();

            // The minimum lease prevents sequential executions from the same cron tick.
            second.cleanupFinishedTasks();
            verify(secondCleanup, never()).deleteExpiredTasks();

            // Advance lease expiry without deleting the provider's cached lock row.
            assertThat(jdbc.update("""
                UPDATE shedlock
                SET lock_until = timezone('UTC', CURRENT_TIMESTAMP) - INTERVAL '1 second'
                WHERE name = 'task-cleanup'
                """)).isEqualTo(1);

            second.cleanupFinishedTasks();

            verify(secondCleanup).deleteExpiredTasks();
        } finally {
            release.countDown();
            worker.shutdownNow();
            assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private AnnotationConfigApplicationContext instance(DataSource dataSource, TaskCleanupService cleanup) {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(DataSource.class, () -> dataSource);
        context.registerBean(TaskCleanupService.class, () -> cleanup);
        context.registerBean(MaintenanceScheduler.class, () -> new MaintenanceScheduler(
            mock(WorkerLifecycle.class), mock(TaskRecoveryService.class), cleanup, mock(TaskMetrics.class)));
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
