package com.example.payment.registration.kafka;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("kafka.listener")
record KafkaListenerProperties(
    @DefaultValue("1") @Min(1) int concurrency,
    @DefaultValue("10000") @Min(1) long databaseRetryDelayMs
) {
}
