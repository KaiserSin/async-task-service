package com.example.payment.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class WorkerLifecycle implements SmartLifecycle {

    private final ThreadPoolExecutor paymentExecutor;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final WorkerProperties workerProperties;
    private final Semaphore workerSlots;

    private volatile boolean running;

    public WorkerLifecycle(
        ThreadPoolExecutor paymentExecutor,
        KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
        WorkerProperties workerProperties
    ) {
        this.paymentExecutor = paymentExecutor;
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
        this.workerProperties = workerProperties;
        this.workerSlots = new Semaphore(workerProperties.pool().size());
    }

    @Override
    public void start() {
        running = !paymentExecutor.isShutdown();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public boolean isStopping() {
        return !isRunning();
    }

    public void startWorker(Runnable worker) {
        if (isStopping() || paymentExecutor.isShutdown() || !workerSlots.tryAcquire()) {
            return;
        }
        try {
            paymentExecutor.execute(() -> {
                try {
                    worker.run();
                } finally {
                    workerSlots.release();
                }
            });
        } catch (RejectedExecutionException e) {
            workerSlots.release();
            if (!paymentExecutor.isShutdown()) {
                log.error("Worker submission failed", e);
            }
        }
    }

    @Override
    public void stop() {
        running = false;

        long startedAt = System.nanoTime();
        var shutdown = workerProperties.shutdown();
        long graceDeadline = startedAt + TimeUnit.SECONDS.toNanos(shutdown.gracePeriodSeconds());
        long shutdownDeadline = graceDeadline + TimeUnit.SECONDS.toNanos(shutdown.interruptWaitSeconds());
        CountDownLatch kafkaStopped = new CountDownLatch(1);
        paymentExecutor.shutdown();

        try {
            if (kafkaListenerEndpointRegistry.getListenerContainers().isEmpty()) {
                kafkaStopped.countDown();
            } else {
                kafkaListenerEndpointRegistry.stop(kafkaStopped::countDown);
            }

            log.info("Waiting for workers and Kafka listeners to stop");
            if (!paymentExecutor.awaitTermination(remainingNanos(graceDeadline), TimeUnit.NANOSECONDS)) {
                paymentExecutor.shutdownNow();
                if (!paymentExecutor.awaitTermination(remainingNanos(shutdownDeadline), TimeUnit.NANOSECONDS)) {
                    log.warn("Some workers are still running; payments will be recovered after their leases expire");
                }
            }
            if (!kafkaStopped.await(remainingNanos(shutdownDeadline), TimeUnit.NANOSECONDS)) {
                log.warn("Kafka listeners did not stop in time; unconfirmed messages will be sent again");
            }
        } catch (InterruptedException e) {
            paymentExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            log.warn("App shutdown was interrupted", e);
        } catch (RuntimeException e) {
            paymentExecutor.shutdownNow();
            log.error("App shutdown failed", e);
        }
    }

    private long remainingNanos(long deadline) {
        return Math.max(0L, deadline - System.nanoTime());
    }
}
