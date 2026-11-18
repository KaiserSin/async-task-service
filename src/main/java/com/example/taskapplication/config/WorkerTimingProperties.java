package com.example.taskapplication.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.util.concurrent.TimeUnit;

@Validated
@ConfigurationProperties("worker")
public record WorkerTimingProperties(
    @DefaultValue("30") @Min(1) int leaseSeconds,
    @DefaultValue("30") @Min(0) int retryBackoffSeconds
) {
    public long leaseNanos() {
        return TimeUnit.SECONDS.toNanos(leaseSeconds);
    }
}
