package com.example.payment.worker.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

@Component
class WorkerPoolMetrics implements MeterBinder {

    private final ThreadPoolExecutor paymentExecutor;

    public WorkerPoolMetrics(ThreadPoolExecutor paymentExecutor) {
        this.paymentExecutor = paymentExecutor;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("payment.worker.active", paymentExecutor, ThreadPoolExecutor::getActiveCount)
            .description("Busy workers")
            .register(registry);

        Gauge.builder("payment.worker.capacity", paymentExecutor, ThreadPoolExecutor::getMaximumPoolSize)
            .description("Worker limit")
            .register(registry);
    }
}
