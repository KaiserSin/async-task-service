package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
    }

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    @Test
    void gracefulShutdownIsIdempotentAndRejectsNewWorkers() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of());
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(true);
        Runnable newWorker = mock(Runnable.class);

        lifecycle.shutdown();
        lifecycle.shutdown();
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

        lifecycle.shutdown();

        var order = inOrder(executor, listeners);
        order.verify(executor).shutdown();
        order.verify(listeners).stop(any(Runnable.class));
        order.verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(executor, never()).shutdownNow();
    }

    @Test
    void interruptsWorkersOnlyAfterGracePeriodExpires() throws Exception {
        when(listeners.getListenerContainers()).thenReturn(List.of());
        when(executor.awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS))).thenReturn(false, true);

        lifecycle.shutdown();

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

        lifecycle.shutdown();

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(lifecycle.isStopping()).isTrue();
        verify(executor).shutdownNow();
        verify(executor).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    @Test
    void interruptsWorkersIfListenerShutdownFails() {
        when(listeners.getListenerContainers()).thenThrow(new IllegalStateException("Registry unavailable"));

        assertThatCode(lifecycle::shutdown).doesNotThrowAnyException();

        assertThat(lifecycle.isStopping()).isTrue();
        verify(executor).shutdown();
        verify(executor).shutdownNow();
    }

    @Test
    void rejectedWorkerIsNotRunByTheCallingThread() {
        Runnable worker = mock(Runnable.class);
        doThrow(new RejectedExecutionException("Pool full")).when(executor).execute(worker);

        assertThatCode(() -> lifecycle.startWorker(worker)).doesNotThrowAnyException();

        verify(executor).execute(worker);
        verifyNoInteractions(worker);
    }

    @Test
    void contextClosedEventAndBeanDestructionStopThePoolOnlyOnce() throws Exception {
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

            context.publishEvent(new ContextClosedEvent(context));

            assertThat(registered.isStopping()).isTrue();
            verify(executor).shutdown();
        }

        assertThat(registered.isStopping()).isTrue();
        verify(executor, times(1)).shutdown();
        verify(executor, times(1)).awaitTermination(anyLong(), eq(TimeUnit.NANOSECONDS));
        verify(executor, never()).shutdownNow();
    }
}
