package com.example.payment.registration.kafka;

import com.example.payment.database.DatabaseFailureClassifier;
import com.fasterxml.jackson.databind.DeserializationFeature;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import com.example.payment.registration.InvalidPaymentMessageException;
import com.example.payment.registration.PaymentCommand;

@Configuration
@EnableKafka
@Slf4j
@EnableConfigurationProperties({KafkaProperties.class, KafkaListenerProperties.class})
public class KafkaConfig {

    @Bean
    public ConsumerFactory<String, PaymentCommand> consumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        var mapper = JacksonUtils.enhancedObjectMapper().disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
        var json = new JsonDeserializer<>(PaymentCommand.class, mapper, false);

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new ErrorHandlingDeserializer<>(json)
        );
    }

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);

        Map<Class<?>, Serializer<?>> serializers = new HashMap<>();
        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(PaymentCommand.class, new JsonSerializer<>());

        var factory = new DefaultKafkaProducerFactory<String, Object>(
            props,
            new StringSerializer(),
            new DelegatingByTypeSerializer(serializers)
        );

        factory.setTransactionIdPrefix("payment-" + UUID.randomUUID() + "-");
        factory.setPhysicalCloseTimeout(1);
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(
        ProducerFactory<String, Object> dltProducerFactory
    ) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DefaultAfterRollbackProcessor<String, PaymentCommand> kafkaAfterRollbackProcessor(
        KafkaTemplate<String, Object> dltKafkaTemplate,
        DatabaseFailureClassifier databaseFailureClassifier,
        TaskScheduler paymentScheduler,
        KafkaListenerProperties listenerProperties
    ) {
        DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, exception) -> {
                    var failure = databaseFailureClassifier.inspect(exception);
                    PaymentCommand dto = record.value() instanceof PaymentCommand payment ? payment : null;
                    if (failure.action() == DatabaseFailureClassifier.Action.NOT_DATABASE) {
                        log.warn("Rejected payment message eventId={} topic={} partition={} offset={}",
                            dto == null ? null : dto.eventId(), record.topic(), record.partition(), record.offset());
                    } else {
                        log.warn("Database error {}", failure.logDetails(dto == null ? null : dto.paymentOperationId()));
                    }
                    return new TopicPartition(record.topic() + ".DLT", -1);
                }
            );
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));
        recoverer.setTimeoutBuffer(1000L);

        var processor = new DatabaseAfterRollbackProcessor<String, PaymentCommand>(
            recoverer,
            new FixedBackOff(1000L, 2L),
            dltKafkaTemplate,
            databaseFailureClassifier,
            paymentScheduler,
            listenerProperties.databaseRetryDelayMs()
        );
        processor.addNotRetryableExceptions(
            ConstraintViolationException.class,
            InvalidPaymentMessageException.class
        );
        return processor;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentCommand>
    kafkaListenerContainerFactory(
        ConsumerFactory<String, PaymentCommand> consumerFactory,
        ProducerFactory<String, Object> dltProducerFactory,
        DefaultAfterRollbackProcessor<String, PaymentCommand> kafkaAfterRollbackProcessor,
        KafkaListenerProperties listenerProperties
    ) {
        var factory =
            new ConcurrentKafkaListenerContainerFactory<String, PaymentCommand>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(listenerProperties.concurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setPauseImmediate(true);
        factory.getContainerProperties().setStopImmediate(true);

        factory.getContainerProperties().setKafkaAwareTransactionManager(
            new KafkaTransactionManager<>(dltProducerFactory)
        );
        factory.setAfterRollbackProcessor(kafkaAfterRollbackProcessor);
        return factory;
    }
}
