package com.example.taskapplication.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.SynchronousQueue;
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
            new SynchronousQueue<>(),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
