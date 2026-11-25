package com.example.payment.worker;

import com.example.payment.worker.attempt.PaymentClaimService;
import com.example.payment.worker.attempt.PaymentAttempt;
import com.example.payment.worker.attempt.PaymentRunner;
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
import com.example.payment.worker.config.WorkerConfig;

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
class PaymentPollerTest {

    @Mock
    private PaymentClaimService paymentClaimService;

    @Mock
    private PaymentRunner paymentRunner;

    private final List<CountDownLatch> releases = new ArrayList<>();
    private ThreadPoolExecutor executor;
    private WorkerLifecycle lifecycle;
    private PaymentPoller poller;

    @BeforeEach
    void setUp() {
        WorkerProperties properties = new WorkerProperties(30, 30, 5,
            new WorkerProperties.Pool(4), new WorkerProperties.Shutdown(75, 10),
            new WorkerProperties.Cleanup(30, 90, 500));
        executor = new WorkerConfig().paymentExecutor(properties);
        lifecycle = new WorkerLifecycle(executor, mock(KafkaListenerEndpointRegistry.class), properties);
        lifecycle.start();
        poller = new PaymentPoller(lifecycle, paymentClaimService, paymentRunner);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        ReflectionTestUtils.setField(lifecycle, "running", false);
        releases.forEach(CountDownLatch::countDown);
        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void emptyQueueMakesOneClaimPerPollRegardlessOfPoolSize() {
        when(paymentClaimService.claimNextPayment()).thenReturn(Optional.empty());

        poller.startWorker();
        awaitCompletedWorkers(1);
        verify(paymentClaimService).claimNextPayment();
        assertThat(executor.getTaskCount()).isEqualTo(1);
        verifyNoInteractions(paymentRunner);

        poller.startWorker();
        awaitCompletedWorkers(2);
        verify(paymentClaimService, times(2)).claimNextPayment();
        assertThat(executor.getTaskCount()).isEqualTo(2);
        verifyNoInteractions(paymentRunner);
    }

    @Test
    void failedClaimDoesNotStartAnotherWorker() {
        when(paymentClaimService.claimNextPayment())
            .thenThrow(new CannotCreateTransactionException("Database unavailable"));

        poller.startWorker();

        awaitCompletedWorkers(1);
        verify(paymentClaimService).claimNextPayment();
        assertThat(executor.getTaskCount()).isEqualTo(1);
        verifyNoInteractions(paymentRunner);
    }

    @Test
    void onePollFillsThePoolOnlyAfterSuccessfulClaimsAndNeverClaimsBeyondCapacity() throws Exception {
        Queue<PaymentAttempt> ready = new ConcurrentLinkedQueue<>();
        for (long id = 1; id <= 5; id++) {
            ready.add(newAttempt(id));
        }
        when(paymentClaimService.claimNextPayment()).thenAnswer(invocation -> Optional.ofNullable(ready.poll()));
        CountDownLatch started = new CountDownLatch(4);
        CountDownLatch release = releaseLatch();
        doAnswer(invocation -> {
            started.countDown();
            hold(release);
            return null;
        }).when(paymentRunner).executePayment(any(PaymentAttempt.class));

        poller.startWorker();

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(executor.getActiveCount()).isEqualTo(4);
        verify(paymentClaimService, times(4)).claimNextPayment();
        assertThat(ready).hasSize(1);

        poller.startWorker();

        verify(paymentClaimService, times(4)).claimNextPayment();
        assertThat(ready).hasSize(1);
        assertThat(executor.getTaskCount()).isEqualTo(4);
    }

    @Test
    void laterPollUsesFreeSlotsWhileAnEarlierPaymentIsStillRunning() throws Exception {
        PaymentAttempt first = newAttempt(1);
        PaymentAttempt second = newAttempt(2);
        Queue<PaymentAttempt> ready = new ConcurrentLinkedQueue<>();
        ready.add(first);
        when(paymentClaimService.claimNextPayment()).thenAnswer(invocation -> Optional.ofNullable(ready.poll()));
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch release = releaseLatch();
        doAnswer(invocation -> {
            PaymentAttempt claimed = invocation.getArgument(0);
            if (claimed.id().equals(first.id())) {
                firstStarted.countDown();
            } else {
                secondStarted.countDown();
            }
            hold(release);
            return null;
        }).when(paymentRunner).executePayment(any(PaymentAttempt.class));

        poller.startWorker();
        assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitCompletedWorkers(1);
        verify(paymentClaimService, times(2)).claimNextPayment();

        ready.add(second);
        poller.startWorker();

        assertThat(secondStarted.await(5, TimeUnit.SECONDS)).isTrue();
        awaitCompletedWorkers(2);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertThat(executor.getActiveCount()).isEqualTo(2));
        verify(paymentRunner).executePayment(first);
        verify(paymentRunner).executePayment(second);
        verify(paymentClaimService, times(4)).claimNextPayment();
    }

    @Test
    void stoppingDoesNotStartAClaim() {
        lifecycle.stop();

        poller.startWorker();

        assertThat(executor.getTaskCount()).isZero();
        verifyNoInteractions(paymentClaimService, paymentRunner);
    }

    @Test
    void closedExecutorDoesNotStartAClaim() {
        executor.shutdown();

        poller.startWorker();

        assertThat(executor.getTaskCount()).isZero();
        verifyNoInteractions(paymentClaimService, paymentRunner);
    }

    @Test
    void shutdownDuringClaimDoesNotStartAnotherWorkerButFinishesTheAcceptedPayment() throws Exception {
        PaymentAttempt claimed = newAttempt(1);
        CountDownLatch claimStarted = new CountDownLatch(1);
        CountDownLatch releaseClaim = releaseLatch();
        when(paymentClaimService.claimNextPayment()).thenAnswer(invocation -> {
            claimStarted.countDown();
            hold(releaseClaim);
            return Optional.of(claimed);
        });

        poller.startWorker();
        assertThat(claimStarted.await(5, TimeUnit.SECONDS)).isTrue();
        ReflectionTestUtils.setField(lifecycle, "running", false);
        releaseClaim.countDown();

        awaitCompletedWorkers(1);
        verify(paymentClaimService).claimNextPayment();
        verify(paymentRunner).executePayment(claimed);
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

    private PaymentAttempt newAttempt(long id) {
        return new PaymentAttempt(id, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), 1000L, "EUR", "pm_test", "test-key",
            System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
    }
}
