package com.example.payment.worker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class WorkerLifecycleTest {

    @Mock
    private ThreadPoolExecutor executor;

    @Mock
    private KafkaListenerEndpointRegistry listeners;

    private final WorkerProperties properties = new WorkerProperties(30, 30, 5,
        new WorkerProperties.Pool(4), new WorkerProperties.Shutdown(1, 1),
        new WorkerProperties.Cleanup(30, 90, 500));

    private WorkerLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        lifecycle = new WorkerLifecycle(executor, listeners, properties);
        lifecycle.start();
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void gracefulStopRejectsNewWorkers() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of());
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
        Runnable newWorker = mock(Runnable.class);

        lifecycle.stop();
        lifecycle.startWorker(newWorker);

        assertThat(lifecycle.isStopping()).isTrue();
        verify(executor).shutdown();
        verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(executor, never()).shutdownNow();
        verify(executor, never()).execute(any(Runnable.class));
        verify(listeners, never()).stop(any(Runnable.class));
        verifyNoInteractions(newWorker);
    }

    @Test
    void stopsAdmissionAndKafkaBeforeWaitingForWorkers() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of(mock(MessageListenerContainer.class)));
        doAnswer(invocation -> {
            assertThat(lifecycle.isStopping()).isTrue();
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(listeners).stop(any(Runnable.class));
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);

        Runnable callback = mock(Runnable.class);
        lifecycle.stop(callback);

        var order = inOrder(executor, listeners, callback);
        order.verify(executor).shutdown();
        order.verify(listeners).stop(any(Runnable.class));
        order.verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        order.verify(callback).run();
        verify(executor, never()).shutdownNow();
    }

    @Test
    void interruptsWorkersOnlyAfterGracePeriodExpires() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of());
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(false, true);

        lifecycle.stop();

        var order = inOrder(executor);
        order.verify(executor).shutdown();
        order.verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        order.verify(executor).shutdownNow();
        order.verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        assertThat(lifecycle.isStopping()).isTrue();
    }

    @Test
    void restoresInterruptFlagAndInterruptsWorkersWhenShutdownWaitIsInterrupted() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of());
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS)))
            .thenThrow(new InterruptedException("Interrupted shutdown"));

        lifecycle.stop();

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(lifecycle.isStopping()).isTrue();
        verify(executor).shutdownNow();
        verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void interruptsWorkersIfListenerShutdownFails() {
        when(listeners.getListenerContainers()).thenThrow(new IllegalStateException("Registry unavailable"));

        assertThatCode(lifecycle::stop).doesNotThrowAnyException();

        assertThat(lifecycle.isStopping()).isTrue();
        verify(executor).shutdown();
        verify(executor).shutdownNow();
    }

    @Test
    void fullPoolSkipsSubmissionAndWorkerFailureFreesSlot() {
        int poolSize = properties.pool().size();
        Runnable worker = () -> {
            throw new IllegalStateException("Worker failed");
        };

        for (int i = 0; i < poolSize + 1; i++) {
            lifecycle.startWorker(worker);
        }

        var submissions = ArgumentCaptor.forClass(Runnable.class);
        verify(executor, times(poolSize)).execute(submissions.capture());
        assertThatThrownBy(submissions.getValue()::run).isInstanceOf(IllegalStateException.class);

        lifecycle.startWorker(mock(Runnable.class));

        verify(executor, times(poolSize + 1)).execute(any(Runnable.class));
    }

    @Test
    void shutdownDuringSubmissionDoesNotRunWorkerOnTheCallingThread() {
        Runnable worker = mock(Runnable.class);
        when(executor.isShutdown()).thenReturn(false, true);
        doThrow(new RejectedExecutionException("Executor shut down")).when(executor).execute(any(Runnable.class));

        assertThatCode(() -> lifecycle.startWorker(worker)).doesNotThrowAnyException();

        verify(executor).execute(any(Runnable.class));
        verifyNoInteractions(worker);
    }

    @Test
    void springStartsWorkerAndStopsPoolOnlyOnce() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of());
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
        WorkerLifecycle registered;
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean(ThreadPoolExecutor.class, () -> executor,
                definition -> definition.setDestroyMethodName(""));
            context.registerBean(KafkaListenerEndpointRegistry.class, () -> listeners);
            context.registerBean(WorkerProperties.class, () -> properties);
            context.register(WorkerLifecycle.class);
            context.refresh();
            registered = context.getBean(WorkerLifecycle.class);

            assertThat(registered.isRunning()).isTrue();
            context.stop();

            assertThat(registered.isStopping()).isTrue();
            verify(executor).shutdown();
        }

        assertThat(registered.isStopping()).isTrue();
        verify(executor, times(1)).shutdown();
        verify(executor, times(1)).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(executor, never()).shutdownNow();
    }
}
