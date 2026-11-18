package com.example.taskapplication.config;

import com.example.taskapplication.service.database.DatabaseFailureClassifier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
        var failed = new ConsumerRecord<>("tasks", 0, 2L, "key", "invalid");
        var next = new ConsumerRecord<>("tasks", 0, 3L, "next", "valid");
        var failure = dataFailure();
        var group = new ConsumerGroupMetadata("test-group");
        when(kafkaTemplate.isTransactional()).thenReturn(true);
        when(consumer.groupMetadata()).thenReturn(group);

        processor.process(List.of(failed, next), consumer, null, failure, true, ContainerProperties.EOSMode.V2);

        var order = inOrder(recoverer, kafkaTemplate);
        order.verify(recoverer).accept(failed, failure);
        order.verify(kafkaTemplate).sendOffsetsToTransaction(
            Map.of(new TopicPartition("tasks", 0), new OffsetAndMetadata(3L)), group);
        verify(consumer).seek(new TopicPartition("tasks", 0), 3L);
        verifyNoInteractions(scheduler);
    }

    @Test
    void keepsFailedOffsetWhenDltPublishingFails() {
        var failed = new ConsumerRecord<>("tasks", 0, 2L, "key", "invalid");
        var failure = dataFailure();
        doThrow(new IllegalStateException("DLT unavailable")).when(recoverer).accept(failed, failure);

        processor.process(List.of(failed), consumer, null, failure, true, ContainerProperties.EOSMode.V2);

        verify(recoverer).accept(failed, failure);
        verify(consumer).seek(new TopicPartition("tasks", 0), 2L);
        verifyNoInteractions(kafkaTemplate, scheduler);
    }

    private DataIntegrityViolationException dataFailure() {
        return new DataIntegrityViolationException("Invalid name", new SQLException("NUL", "22021"));
    }
}
