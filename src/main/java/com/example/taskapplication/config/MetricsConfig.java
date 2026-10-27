package com.example.taskapplication.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class MetricsConfig {

    @Bean
    public MeterBinder workerPoolMetrics(ThreadPoolExecutor taskExecutor) {
        return registry -> {
            Gauge.builder("task.worker.active", taskExecutor,
                    ThreadPoolExecutor::getActiveCount)
                .description("Busy workers")
                .register(registry);

            Gauge.builder("task.worker.capacity", taskExecutor,
                    ThreadPoolExecutor::getMaximumPoolSize)
                .description("Worker limit")
                .register(registry);
        };
    }
}
