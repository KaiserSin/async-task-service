package com.example.taskapplication.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class WorkerConfig {

    @Bean(destroyMethod = "")
    public ThreadPoolExecutor taskExecutor(WorkerProperties workerProperties) {
        int poolSize = workerProperties.pool().size();
        return new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            // A worker may release its permit before the pool thread is ready for another handoff.
            new ArrayBlockingQueue<>(poolSize),
            new CustomizableThreadFactory("task-worker-"),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
