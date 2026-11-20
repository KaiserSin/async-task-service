package com.example.taskapplication.config;

import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.service.database.DatabaseFailureClassifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConfigTest {

    private static final String TEST_JAAS =
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test-only\";";

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withUserConfiguration(KafkaConfig.class, Dependencies.class)
        .withPropertyValues(
            "spring.kafka.bootstrap-servers=broker-one:9093,broker-two:9093",
            "spring.kafka.consumer.group-id=config-test-group"
        );

    @Test
    void passesSharedTlsAndSaslSettingsToBothFactories() {
        contextRunner.withPropertyValues(
            "spring.kafka.security.protocol=SASL_SSL",
            "spring.kafka.properties[sasl.mechanism]=PLAIN",
            "spring.kafka.properties[sasl.jaas.config]=" + TEST_JAAS,
            "spring.kafka.ssl.protocol=TLSv1.3",
            "spring.kafka.ssl.trust-store-location=file:/tmp/task-shared-truststore.jks",
            "spring.kafka.ssl.trust-store-password=test-only",
            "spring.kafka.properties[ssl.endpoint.identification.algorithm]=https"
        ).run(context -> {
            assertThat(context).hasNotFailed();
            var consumer = context.getBean("consumerFactory", DefaultKafkaConsumerFactory.class);
            var producer = context.getBean("dltProducerFactory", DefaultKafkaProducerFactory.class);
            for (var properties : List.of(consumer.getConfigurationProperties(), producer.getConfigurationProperties())) {
                assertThat(properties)
                    .containsEntry("bootstrap.servers", List.of("broker-one:9093", "broker-two:9093"))
                    .containsEntry("security.protocol", "SASL_SSL")
                    .containsEntry(SaslConfigs.SASL_MECHANISM, "PLAIN")
                    .containsEntry(SaslConfigs.SASL_JAAS_CONFIG, TEST_JAAS)
                    .containsEntry(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3")
                    .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/task-shared-truststore.jks")
                    .containsEntry(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "test-only")
                    .containsEntry(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");
            }
            assertThat(consumer.getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "config-test-group");
        });
    }

    @Test
    void keepsConsumerAndProducerSecurityOverridesSeparate() {
        contextRunner.withPropertyValues(
            "spring.kafka.security.protocol=SASL_SSL",
            "spring.kafka.properties[sasl.mechanism]=PLAIN",
            "spring.kafka.consumer.security.protocol=SSL",
            "spring.kafka.consumer.ssl.protocol=TLSv1.2",
            "spring.kafka.consumer.ssl.trust-store-location=file:/tmp/task-consumer-truststore.jks",
            "spring.kafka.consumer.properties[sasl.mechanism]=SCRAM-SHA-256",
            "spring.kafka.producer.ssl.protocol=TLSv1.3",
            "spring.kafka.producer.ssl.trust-store-location=file:/tmp/task-producer-truststore.jks",
            "spring.kafka.producer.properties[sasl.mechanism]=SCRAM-SHA-512",
            "spring.kafka.producer.properties[sasl.jaas.config]=" + TEST_JAAS
        ).run(context -> {
            assertThat(context).hasNotFailed();
            var consumer = context.getBean("consumerFactory", DefaultKafkaConsumerFactory.class);
            var producer = context.getBean("dltProducerFactory", DefaultKafkaProducerFactory.class);
            assertThat(consumer.getConfigurationProperties())
                .containsEntry("security.protocol", "SSL")
                .containsEntry(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/task-consumer-truststore.jks")
                .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256")
                .doesNotContainKey(SaslConfigs.SASL_JAAS_CONFIG);
            assertThat(producer.getConfigurationProperties())
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.3")
                .containsEntry(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/task-producer-truststore.jks")
                .containsEntry(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
                .containsEntry(SaslConfigs.SASL_JAAS_CONFIG, TEST_JAAS);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void preservesTransactionalDeliveryAndDltSerialization() {
        contextRunner.withPropertyValues(
            "spring.kafka.consumer.enable-auto-commit=true",
            "spring.kafka.consumer.isolation-level=read_uncommitted",
            "spring.kafka.producer.acks=0",
            "spring.kafka.producer.properties[enable.idempotence]=false"
        ).run(context -> {
            assertThat(context).hasNotFailed();
            var consumer = (DefaultKafkaConsumerFactory<String, TaskRequestDTO>)
                context.getBean("consumerFactory", DefaultKafkaConsumerFactory.class);
            var producer = (DefaultKafkaProducerFactory<String, Object>)
                context.getBean("dltProducerFactory", DefaultKafkaProducerFactory.class);
            assertThat(consumer.getConfigurationProperties())
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            assertThat(producer.getConfigurationProperties())
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all")
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            assertThat(producer.transactionCapable()).isTrue();
            assertThat(producer.getTransactionIdPrefix()).startsWith("task-");

            var task = new TaskRequestDTO(UUID.randomUUID(), "alice", "configured Kafka", 1000L);
            var json = producer.getValueSerializer().serialize("tasks.DLT", task);
            var mapper = new ObjectMapper();
            assertThat(mapper.readTree(json)).isEqualTo(mapper.readTree("""
                {
                  "eventId": "%s",
                  "ownerUsername": "alice",
                  "name": "configured Kafka",
                  "duration": 1000
                }
                """.formatted(task.eventId())));
            assertThat(consumer.getValueDeserializer().deserialize("tasks", json)).isEqualTo(task);

            var invalidJson = new byte[] {0, 1, 2};
            assertThat(producer.getValueSerializer().serialize("tasks.DLT", invalidJson)).isEqualTo(invalidJson);
            var headers = new RecordHeaders();
            assertThat(consumer.getValueDeserializer().deserialize("tasks", headers, invalidJson)).isNull();
            assertThat(headers.lastHeader(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isNotNull();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "kafka.listener.concurrency=0",
        "kafka.listener.database-retry-delay-ms=0"
    })
    void rejectsInvalidListenerSettingsAtStartup(String property) {
        contextRunner.withPropertyValues(property).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class Dependencies {
        @Bean
        ThreadPoolTaskScheduler taskScheduler() {
            return new ThreadPoolTaskScheduler();
        }

        @Bean
        DatabaseFailureClassifier databaseFailureClassifier() {
            return new DatabaseFailureClassifier();
        }
    }
}
