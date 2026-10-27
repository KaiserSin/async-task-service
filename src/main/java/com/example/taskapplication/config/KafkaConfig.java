package com.example.taskapplication.config;

import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.exception.InvalidTaskMessageException;
import com.example.taskapplication.service.database.DatabaseFailureClassifier;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
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

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:task-group}")
    private String groupId;

    @Value("${kafka.listener.concurrency:1}")
    private int concurrency;

    @Value("${kafka.listener.database-retry-delay-ms:10000}")
    private long databaseRetryDelayMs;

    @Bean
    public ConsumerFactory<String, TaskRequestDTO> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        JsonDeserializer<TaskRequestDTO> json =
            new JsonDeserializer<>(TaskRequestDTO.class, false);

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new ErrorHandlingDeserializer<>(json)
        );
    }

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000L);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);

        Map<Class<?>, Serializer<?>> serializers = new HashMap<>();
        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(TaskRequestDTO.class, new JsonSerializer<>());

        var factory = new DefaultKafkaProducerFactory<String, Object>(
            props,
            new StringSerializer(),
            new DelegatingByTypeSerializer(serializers)
        );

        factory.setTransactionIdPrefix("task-" + UUID.randomUUID() + "-");
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
    public DefaultAfterRollbackProcessor<String, TaskRequestDTO> kafkaAfterRollbackProcessor(
        KafkaTemplate<String, Object> dltKafkaTemplate,
        DatabaseFailureClassifier databaseFailureClassifier,
        TaskScheduler taskScheduler
    ) {
        DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, exception) -> new TopicPartition(
                    record.topic() + ".DLT", record.partition()
                )
            );
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(10));
        recoverer.setTimeoutBuffer(1000L);

        var processor = new DatabaseAfterRollbackProcessor<String, TaskRequestDTO>(
            recoverer,
            new FixedBackOff(1000L, 2L),
            dltKafkaTemplate,
            databaseFailureClassifier,
            taskScheduler,
            databaseRetryDelayMs
        );
        processor.addNotRetryableExceptions(
            ConstraintViolationException.class,
            InvalidTaskMessageException.class
        );
        return processor;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TaskRequestDTO>
    kafkaListenerContainerFactory(
        ConsumerFactory<String, TaskRequestDTO> consumerFactory,
        ProducerFactory<String, Object> dltProducerFactory,
        DefaultAfterRollbackProcessor<String, TaskRequestDTO> kafkaAfterRollbackProcessor
    ) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("Kafka concurrency must be greater than 0");
        }

        var factory =
            new ConcurrentKafkaListenerContainerFactory<String, TaskRequestDTO>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
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
