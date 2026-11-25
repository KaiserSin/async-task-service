package com.example.payment.registration.kafka;

import com.example.payment.database.DatabaseFailureClassifier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DatabaseAfterRollbackProcessorTest {

    @Mock
    private BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer;
    @Mock
    private Consumer<String, String> consumer;
    @Mock
    private KafkaOperations<String, Object> kafkaTemplate;
    @Mock
    private TaskScheduler scheduler;

    private DatabaseAfterRollbackProcessor<String, String> processor;

    @BeforeEach
    void setUp() {
        processor = new DatabaseAfterRollbackProcessor<>(recoverer, new FixedBackOff(1000L, 2L),
            kafkaTemplate, new DatabaseFailureClassifier(), scheduler, 10_000L);
    }

    @Test
    void recoversDataErrorImmediatelyAndCommitsOnlyRecoveredOffset() {
        var failed = new ConsumerRecord<>("payments", 0, 2L, "key", "invalid");
        var next = new ConsumerRecord<>("payments", 0, 3L, "next", "valid");
        var failure = dataFailure();
        var group = new ConsumerGroupMetadata("test-group");
        when(kafkaTemplate.isTransactional()).thenReturn(true);
        when(consumer.groupMetadata()).thenReturn(group);

        processor.process(List.of(failed, next), consumer, null, failure, true, ContainerProperties.EOSMode.V2);

        var order = inOrder(recoverer, kafkaTemplate);
        order.verify(recoverer).accept(failed, failure);
        order.verify(kafkaTemplate).sendOffsetsToTransaction(
            Map.of(new TopicPartition("payments", 0), new OffsetAndMetadata(3L)), group);
        verify(consumer).seek(new TopicPartition("payments", 0), 3L);
        verifyNoInteractions(scheduler);
    }

    @Test
    void keepsFailedOffsetWhenDltPublishingFails() {
        var failed = new ConsumerRecord<>("payments", 0, 2L, "key", "invalid");
        var failure = dataFailure();
        doThrow(new IllegalStateException("DLT unavailable")).when(recoverer).accept(failed, failure);

        processor.process(List.of(failed), consumer, null, failure, true, ContainerProperties.EOSMode.V2);

        verify(recoverer).accept(failed, failure);
        verify(consumer).seek(new TopicPartition("payments", 0), 2L);
        verifyNoInteractions(kafkaTemplate, scheduler);
    }

    private DataIntegrityViolationException dataFailure() {
        return new DataIntegrityViolationException("Invalid name", new SQLException("NUL", "22021"));
    }

    @Test
    void pausesReadOnlyDatabaseFailureAndAutomaticallyResumesWithoutRecoveringTheRecord() {
        var failed = new ConsumerRecord<>("payments", 0, 2L, "key", "valid");
        var failure = new DataAccessResourceFailureException("Failover database is read-only",
            new SQLException("Cannot execute INSERT in a read-only transaction", "25006"));
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.isPauseRequested()).thenReturn(false, true);
        Instant before = Instant.now();

        processor.process(List.of(failed), consumer, container, failure, true, ContainerProperties.EOSMode.V2);

        Instant after = Instant.now();
        ArgumentCaptor<Runnable> resume = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Instant> resumeAt = ArgumentCaptor.forClass(Instant.class);
        verify(scheduler).schedule(resume.capture(), resumeAt.capture());
        assertThat(resumeAt.getValue()).isBetween(before.plusSeconds(10), after.plusSeconds(10));
        verify(consumer).seek(new TopicPartition("payments", 0), 2L);
        verify(container).pause();
        verify(container, never()).resume();
        verify(container, never()).stop();
        verifyNoInteractions(recoverer, kafkaTemplate);

        resume.getValue().run();

        verify(container).resume();
        verify(container, never()).stopAbnormally(any(Runnable.class));
    }
}
