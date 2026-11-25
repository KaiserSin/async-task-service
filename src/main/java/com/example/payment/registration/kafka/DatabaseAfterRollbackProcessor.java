package com.example.payment.registration.kafka;

import com.example.payment.database.DatabaseFailureClassifier;
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
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.List;
import java.util.function.BiConsumer;
import com.example.payment.registration.PaymentCommand;

@Slf4j
class DatabaseAfterRollbackProcessor<K, V> extends DefaultAfterRollbackProcessor<K, V> {

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
        TaskScheduler paymentScheduler,
        long retryDelayMs
    ) {
        super(recoverer, backOff, kafkaTemplate, true);
        this.databaseFailureClassifier = databaseFailureClassifier;
        this.pauseService = new ListenerContainerPauseService(null, paymentScheduler);
        this.retryDelay = Duration.ofMillis(retryDelayMs);
        setBackOffFunction((record, exception) ->
            databaseFailureClassifier.classify(exception) == DatabaseFailureClassifier.Action.RECOVER
                ? new FixedBackOff(0L, 0L)
                : backOff
        );
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
        var failure = databaseFailureClassifier.inspect(exception);
        var action = failure.action();
        if (action == DatabaseFailureClassifier.Action.NOT_DATABASE
            || action == DatabaseFailureClassifier.Action.RECOVER) {
            super.process(records, consumer, container, exception, recoverable, eosMode);
            return;
        }
        if (container == null) {
            throw new IllegalStateException("No Kafka container found to handle the database error", exception);
        }

        SeekUtils.doSeeksToBegin(List.copyOf(records), consumer, logger);
        var operationId = !records.isEmpty() && records.getFirst().value() instanceof PaymentCommand dto
            ? dto.paymentOperationId() : null;
        if (action == DatabaseFailureClassifier.Action.STOP) {
            log.error("Database error {}", failure.logDetails(operationId));
            stoppingHandler.handleOtherException(exception, consumer, container, false);
        } else {
            log.warn("Database error {}", failure.logDetails(operationId));
            pauseService.pause(container, retryDelay);
        }
    }
}
