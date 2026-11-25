package com.example.payment.worker.config;

import org.springframework.boot.task.ThreadPoolTaskSchedulerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.example.payment.database.DatabaseLogFilter;
import com.example.payment.worker.WorkerProperties;

@Configuration
public class WorkerConfig {

    @Bean
    ThreadPoolTaskSchedulerCustomizer backgroundTaskLogging() {
        return scheduler -> scheduler.setTaskDecorator(DatabaseLogFilter::backgroundTask);
    }

    @Bean(destroyMethod = "")
    public ThreadPoolExecutor paymentExecutor(WorkerProperties workerProperties) {
        int poolSize = workerProperties.pool().size();
        return new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(poolSize),
            new CustomizableThreadFactory("payment-worker-"),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
