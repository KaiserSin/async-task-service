package com.example.taskapplication.service;

import com.example.taskapplication.config.WorkerConfig;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.service.execution.TaskExecutionService;
import com.example.taskapplication.service.execution.TaskStatusService;
import com.example.taskapplication.service.execution.WorkerLifecycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.CannotCreateTransactionException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskPollerTest {

    @Mock
    private TaskExecutionService executionService;

    @Mock
    private TaskStatusService statusService;

    private final List<CountDownLatch> releases = new ArrayList<>();
    private ThreadPoolExecutor executor;
    private WorkerLifecycle lifecycle;
    private TaskPoller poller;

    @BeforeEach
    void setUp() {
        executor = new WorkerConfig().taskExecutor(4);
        lifecycle = new WorkerLifecycle(executor, mock(KafkaListenerEndpointRegistry.class));
        poller = new TaskPoller(lifecycle, executionService, statusService);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        ReflectionTestUtils.setField(lifecycle, "stopping", true);
        releases.forEach(CountDownLatch::countDown);
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void emptyQueueMakesOneClaimPerPollRegardlessOfPoolSize() {
        when(executionService.claimNextTask()).thenReturn(Optional.empty());

        poller.processTask();
        awaitCompletedWorkers(1);
        verify(executionService).claimNextTask();
        assertThat(executor.getTaskCount()).isEqualTo(1);
        verifyNoInteractions(statusService);

        poller.processTask();
        awaitCompletedWorkers(2);
        verify(executionService, times(2)).claimNextTask();
        assertThat(executor.getTaskCount()).isEqualTo(2);
        verifyNoInteractions(statusService);
    }

    @Test
    void failedClaimDoesNotStartAnotherWorker() {
        when(executionService.claimNextTask())
            .thenThrow(new CannotCreateTransactionException("Database unavailable"));

        poller.processTask();

        awaitCompletedWorkers(1);
        verify(executionService).claimNextTask();
        assertThat(executor.getTaskCount()).isEqualTo(1);
        verifyNoInteractions(statusService);
    }

    @Test
    void onePollFillsThePoolOnlyAfterSuccessfulClaimsAndNeverClaimsBeyondCapacity() throws Exception {
        Queue<TaskRun> ready = new ConcurrentLinkedQueue<>();
        for (long id = 1; id <= 5; id++) {
            ready.add(run(id));
        }
        when(executionService.claimNextTask()).thenAnswer(invocation -> Optional.ofNullable(ready.poll()));
        CountDownLatch started = new CountDownLatch(4);
        CountDownLatch release = releaseLatch();
        doAnswer(invocation -> {
            started.countDown();
            hold(release);
            return null;
        }).when(statusService).executeTask(any(TaskRun.class));

        poller.processTask();

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.getActiveCount()).isEqualTo(4);
        verify(executionService, times(4)).claimNextTask();
        assertThat(ready).hasSize(1);

        poller.processTask();

        verify(executionService, times(4)).claimNextTask();
        assertThat(ready).hasSize(1);
        assertThat(executor.getTaskCount()).isEqualTo(4);
    }

    @Test
    void laterPollUsesFreeSlotsWhileAnEarlierTaskIsStillRunning() throws Exception {
        TaskRun first = run(1);
        TaskRun second = run(2);
        Queue<TaskRun> ready = new ConcurrentLinkedQueue<>();
        ready.add(first);
        when(executionService.claimNextTask()).thenAnswer(invocation -> Optional.ofNullable(ready.poll()));
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch release = releaseLatch();
        doAnswer(invocation -> {
            TaskRun claimed = invocation.getArgument(0);
            if (claimed.id().equals(first.id())) {
                firstStarted.countDown();
            } else {
                secondStarted.countDown();
            }
            hold(release);
            return null;
        }).when(statusService).executeTask(any(TaskRun.class));

        poller.processTask();
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitCompletedWorkers(1);
        verify(executionService, times(2)).claimNextTask();

        ready.add(second);
        poller.processTask();

        assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitCompletedWorkers(2);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertThat(executor.getActiveCount()).isEqualTo(2));
        verify(statusService).executeTask(first);
        verify(statusService).executeTask(second);
        verify(executionService, times(4)).claimNextTask();
    }

    @Test
    void stoppingDoesNotStartAClaim() {
        ReflectionTestUtils.setField(lifecycle, "stopping", true);

        poller.processTask();

        assertThat(executor.getTaskCount()).isZero();
        verifyNoInteractions(executionService, statusService);
    }

    @Test
    void closedExecutorDoesNotStartAClaim() {
        executor.shutdown();

        poller.processTask();

        assertThat(executor.getTaskCount()).isZero();
        verifyNoInteractions(executionService, statusService);
    }

    @Test
    void shutdownDuringClaimDoesNotStartAnotherWorkerButFinishesTheAcceptedTask() throws Exception {
        TaskRun claimed = run(1);
        CountDownLatch claimStarted = new CountDownLatch(1);
        CountDownLatch releaseClaim = releaseLatch();
        when(executionService.claimNextTask()).thenAnswer(invocation -> {
            claimStarted.countDown();
            hold(releaseClaim);
            return Optional.of(claimed);
        });

        poller.processTask();
        assertThat(claimStarted.await(5, TimeUnit.SECONDS)).isTrue();
        ReflectionTestUtils.setField(lifecycle, "stopping", true);
        releaseClaim.countDown();

        awaitCompletedWorkers(1);
        verify(executionService).claimNextTask();
        verify(statusService).executeTask(claimed);
        assertThat(executor.getTaskCount()).isEqualTo(1);
    }

    private CountDownLatch releaseLatch() {
        CountDownLatch release = new CountDownLatch(1);
        releases.add(release);
        return release;
    }

    private void hold(CountDownLatch release) throws InterruptedException {
        assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private void awaitCompletedWorkers(long count) {
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertThat(executor.getCompletedTaskCount()).isEqualTo(count));
    }

    private TaskRun run(long id) {
        return new TaskRun(id, UUID.randomUUID(), UUID.randomUUID(), 1000,
            System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
    }
}
