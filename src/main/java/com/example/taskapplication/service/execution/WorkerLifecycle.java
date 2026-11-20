package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class WorkerLifecycle {

    private final ThreadPoolExecutor taskExecutor;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final WorkerProperties workerProperties;

    private volatile boolean stopping;

    public boolean isStopping() {
        return stopping;
    }

    public void startWorker(Runnable worker) {
        if (stopping || taskExecutor.isShutdown()) {
            return;
        }
        try {
            taskExecutor.execute(worker);
        } catch (RejectedExecutionException ignored) {
            // No executor slot: leave unclaimed tasks in the database.
        }
    }

    @EventListener(ContextClosedEvent.class)
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @PreDestroy
    public synchronized void shutdown() {
        if (stopping) {
            return;
        }
        stopping = true;

        long startedAt = System.nanoTime();
        var shutdown = workerProperties.shutdown();
        long graceDeadline = startedAt + TimeUnit.SECONDS.toNanos(shutdown.gracePeriodSeconds());
        long shutdownDeadline = graceDeadline + TimeUnit.SECONDS.toNanos(shutdown.interruptWaitSeconds());
        CountDownLatch kafkaStopped = new CountDownLatch(1);
        taskExecutor.shutdown();

        try {
            if (kafkaListenerEndpointRegistry.getListenerContainers().isEmpty()) {
                kafkaStopped.countDown();
            } else {
                kafkaListenerEndpointRegistry.stop(kafkaStopped::countDown);
            }

            log.info("Waiting for workers and Kafka listeners to stop");
            if (!taskExecutor.awaitTermination(remainingNanos(graceDeadline), TimeUnit.NANOSECONDS)) {
                taskExecutor.shutdownNow();
                if (!taskExecutor.awaitTermination(remainingNanos(shutdownDeadline), TimeUnit.NANOSECONDS)) {
                    log.warn("Some workers are still running; tasks will be recovered after their leases expire");
                }
            }
            if (!kafkaStopped.await(remainingNanos(shutdownDeadline), TimeUnit.NANOSECONDS)) {
                log.warn("Kafka listeners did not stop in time; unconfirmed messages will be sent again");
            }
        } catch (InterruptedException e) {
            taskExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn("App shutdown was interrupted", e);
        } catch (RuntimeException e) {
            taskExecutor.shutdownNow();
            log.error("App shutdown failed", e);
        }
    }

    private long remainingNanos(long deadline) {
        return Math.max(0L, deadline - System.nanoTime());
    }
}
