package com.example.payment.registration.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaListenerHealthCheckTest {

    @Mock
    private KafkaListenerEndpointRegistry listenerRegistry;

    private SimpleMeterRegistry meterRegistry;
    private KafkaListenerHealthCheck health;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        health = new KafkaListenerHealthCheck(listenerRegistry, meterRegistry);
    }

    @AfterEach
    void tearDown() {
        meterRegistry.close();
    }

    @Test
    void noRegisteredListenersMeansNotReady() {
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of());

        assertState(Status.DOWN, 0);
        assertThat(health.health().getDetails()).containsEntry("containers", 0);
    }

    @Test
    void runningIdleListenerIsHealthyWithoutAssignedPartitions() {
        var listener = mock(MessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(listener));
        when(listener.isRunning()).thenReturn(true);

        assertState(Status.UP, 1);
    }

    @Test
    void stoppedListenerChangesHealthAndGaugeImmediately() {
        var listener = mock(MessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(listener));
        when(listener.isRunning()).thenReturn(true);
        assertState(Status.UP, 1);

        when(listener.isRunning()).thenReturn(false);

        assertState(Status.DOWN, 0);
    }

    @Test
    void requestedPauseIsNotReadyAndResumeRestoresHealthAndGauge() {
        var listener = mock(MessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(listener));
        when(listener.isRunning()).thenReturn(true);
        when(listener.isPauseRequested()).thenReturn(true);
        assertState(Status.DOWN, 0);

        when(listener.isPauseRequested()).thenReturn(false);

        assertState(Status.UP, 1);
    }

    @Test
    void actualPauseIsNotReadyEvenAfterResumeWasRequested() {
        var listener = mock(MessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(listener));
        when(listener.isRunning()).thenReturn(true);
        when(listener.isContainerPaused()).thenReturn(true);
        assertState(Status.DOWN, 0);

        when(listener.isContainerPaused()).thenReturn(false);

        assertState(Status.UP, 1);
    }

    @Test
    void everyRegisteredListenerMustBeReady() {
        var running = mock(MessageListenerContainer.class);
        var stopped = mock(MessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(running, stopped));
        when(running.isRunning()).thenReturn(true);

        assertState(Status.DOWN, 0);
        assertThat(health.health().getDetails()).containsEntry("containers", 2);
    }

    @Test
    void concurrentParentWithNoChildrenIsNotReady() {
        var parent = mock(ConcurrentMessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(parent));
        when(parent.isRunning()).thenReturn(true);
        when(parent.getContainers()).thenReturn(List.of());

        assertState(Status.DOWN, 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void stoppedChildIsDetectedWhileConcurrentParentAndAnotherChildAreRunning() {
        ConcurrentMessageListenerContainer<String, String> parent = mock(ConcurrentMessageListenerContainer.class);
        KafkaMessageListenerContainer<String, String> first = mock(KafkaMessageListenerContainer.class);
        KafkaMessageListenerContainer<String, String> second = mock(KafkaMessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(parent));
        when(parent.isRunning()).thenReturn(true);
        when(parent.getContainers()).thenReturn(List.of(first, second));
        when(first.isRunning()).thenReturn(true);
        when(second.isRunning()).thenReturn(true);
        assertState(Status.UP, 1);

        when(second.isRunning()).thenReturn(false);

        assertState(Status.DOWN, 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pausedChildIsDetectedWhileConcurrentParentIsRunning() {
        ConcurrentMessageListenerContainer<String, String> parent = mock(ConcurrentMessageListenerContainer.class);
        KafkaMessageListenerContainer<String, String> child = mock(KafkaMessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(parent));
        when(parent.isRunning()).thenReturn(true);
        when(parent.getContainers()).thenReturn(List.of(child));
        when(child.isRunning()).thenReturn(true);
        when(child.isPauseRequested()).thenReturn(true);

        assertState(Status.DOWN, 0);
    }

    private void assertState(Status status, double gauge) {
        assertThat(health.health().getStatus()).isEqualTo(status);
        assertThat(meterRegistry.get("payment.kafka.listener.ready").gauge().value()).isEqualTo(gauge);
    }
}
