package com.example.payment.worker;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.util.concurrent.TimeUnit;

@Validated
@ConfigurationProperties("worker")
public record WorkerProperties(
    @DefaultValue("30") @Min(1) int leaseSeconds,
    @DefaultValue("30") @Min(0) int retryBackoffSeconds,
    @DefaultValue("5") @Min(1) int claimTimeoutSeconds,
    @DefaultValue @Valid Pool pool,
    @DefaultValue @Valid Shutdown shutdown,
    @DefaultValue @Valid Cleanup cleanup
) {
    public long leaseNanos() {
        return TimeUnit.SECONDS.toNanos(leaseSeconds);
    }

    public record Pool(@DefaultValue("4") @Min(1) int size) {
    }

    public record Shutdown(
        @DefaultValue("75") @Min(1) long gracePeriodSeconds,
        @DefaultValue("10") @Min(1) long interruptWaitSeconds
    ) {
    }

    public record Cleanup(
        @DefaultValue("30") @Min(1) long retentionDays,
        @DefaultValue("90") @Min(1) long failedRetentionDays,
        @DefaultValue("500") @Min(1) int batchSize
    ) {
    }
}
