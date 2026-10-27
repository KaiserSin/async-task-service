package com.example.taskapplication.config;

import com.example.taskapplication.service.database.DatabaseFailureClassifier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonContainerStoppingErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekUtils;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.backoff.BackOff;

import java.time.Duration;
import java.util.List;
import java.util.function.BiConsumer;

@Slf4j
public class DatabaseAfterRollbackProcessor<K, V> extends DefaultAfterRollbackProcessor<K, V> {

    private final DatabaseFailureClassifier databaseFailureClassifier;
    private final ListenerContainerPauseService pauseService;
    private final Duration retryDelay;
    private final CommonContainerStoppingErrorHandler stoppingHandler =
        new CommonContainerStoppingErrorHandler();

    public DatabaseAfterRollbackProcessor(
        BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
        BackOff backOff,
        KafkaOperations<?, ?> kafkaTemplate,
        DatabaseFailureClassifier databaseFailureClassifier,
        TaskScheduler taskScheduler,
        long retryDelayMs
    ) {
        super(recoverer, backOff, kafkaTemplate, true);
        if (retryDelayMs < 1) {
            throw new IllegalArgumentException("kafka.listener.database-retry-delay-ms must be greater than 0");
        }
        this.databaseFailureClassifier = databaseFailureClassifier;
        this.pauseService = new ListenerContainerPauseService(null, taskScheduler);
        this.retryDelay = Duration.ofMillis(retryDelayMs);
    }

    @Override
    public void process(
        List<ConsumerRecord<K, V>> records,
        Consumer<K, V> consumer,
        MessageListenerContainer container,
        Exception exception,
        boolean recoverable,
        ContainerProperties.EOSMode eosMode
    ) {
        var action = databaseFailureClassifier.classify(exception);
        if (action == DatabaseFailureClassifier.Action.NOT_DATABASE) {
            super.process(records, consumer, container, exception, recoverable, eosMode);
            return;
        }
        if (container == null) {
            throw new IllegalStateException("No Kafka container found to handle the database error", exception);
        }

        SeekUtils.doSeeksToBegin(List.copyOf(records), consumer, logger);
        if (action == DatabaseFailureClassifier.Action.STOP) {
            log.error("Database error needs attention; Kafka listener stopped", exception);
            stoppingHandler.handleOtherException(exception, consumer, container, false);
        } else {
            log.warn("Temporary database error; pausing Kafka listener for {} ms", retryDelay.toMillis(), exception);
            pauseService.pause(container, retryDelay);
        }
    }
}
