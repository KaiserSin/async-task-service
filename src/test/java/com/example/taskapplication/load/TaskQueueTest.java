package com.example.taskapplication.load;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Tag("load")
@EnabledIfSystemProperty(named = "load.tests", matches = "true")
class TaskQueueTest extends TaskLoadSupport {

    @Test
    @DisplayName("The queue grows under overload and drains after publishing stops")
    void drainsQueue() throws Exception {
        startInstances(3);
        CompletableFuture<Void> sending = client.sendAtRate(60, 12, 5000);
        AtomicReference<TaskLoadResult> first = new AtomicReference<>();

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).until(() -> {
            sending.getNow(null);
            TaskLoadResult result = client.sample();
            if (result.sent() >= 20 && result.waiting() > 0) {
                first.set(result);
                return true;
            }
            return false;
        });
        sending.get(30, TimeUnit.SECONDS);

        AtomicReference<TaskLoadResult> peak = new AtomicReference<>();
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).until(() -> {
            TaskLoadResult result = client.sample();
            peak.set(result);
            return result.registered() == 60 && result.waiting() > first.get().waiting();
        });
        TaskLoadResult result = client.awaitCompleted(60, Duration.ofSeconds(90));

        assertFinished(60, true);
        System.out.println("Queue: " + first.get().waiting() + " -> " + peak.get().waiting()
            + " -> " + result.waiting() + ", " + result);
    }

    @Test
    @DisplayName("Three instances handle 8 short tasks per second")
    void keepsUp() throws Exception {
        checkRate(80, 6.0);
    }

    @Test
    @EnabledIfSystemProperty(named = "load.performance", matches = "true")
    @DisplayName("Three instances handle 8 tasks per second for ten minutes")
    void keepsUpForTenMinutes() throws Exception {
        checkRate(4800, 7.5);
    }

    private void checkRate(int expected, double minimumThroughput) throws Exception {
        startInstances(3);
        List<Double> restTimes = new ArrayList<>();
        AtomicInteger requestNumber = new AtomicInteger();
        AtomicReference<Long> taskId = new AtomicReference<>();
        AtomicReference<TaskLoadResult> latest = new AtomicReference<>();
        CompletableFuture<Void> sending = client.sendAtRate(expected, 8, 1000);

        await().atMost(Duration.ofSeconds(expected / 8 + 30))
            .pollInterval(Duration.ofMillis(250)).until(() -> {
                sending.getNow(null);
                TaskLoadResult result = client.sample();
                latest.set(result);

                assertThat(result.waiting()).isLessThanOrEqualTo(24);
                if (taskId.get() == null) {
                    taskId.set(jdbc.queryForObject("SELECT MIN(id) FROM tasks", Long.class));
                }
                if (taskId.get() != null) {
                    long started = System.nanoTime();
                    HttpResponse<String> response = response(
                        requestNumber.getAndIncrement() % instances.size(), "/api/tasks/" + taskId.get());
                    restTimes.add((System.nanoTime() - started) / 1_000_000.0);

                    assertThat(response.statusCode()).isEqualTo(200);
                }
                assertThat(result.failed()).isZero();
                return result.completed() == expected;
            });
        sending.get(30, TimeUnit.SECONDS);

        TaskLoadResult result = latest.get();
        double restP95 = TaskLoadResult.p95(restTimes);
        assertThat(result.sent()).isEqualTo(expected);
        assertThat(result.confirmed()).isEqualTo(expected);
        assertThat(result.registered()).isEqualTo(expected);
        assertThat(result.registrationP95Ms()).isLessThanOrEqualTo(2000);
        assertThat(result.waitP95Ms()).isLessThanOrEqualTo(3000);
        assertThat(result.maxSendDelayMs()).isLessThanOrEqualTo(1000);
        assertThat(result.throughput()).isGreaterThanOrEqualTo(minimumThroughput);
        assertThat(restTimes).isNotEmpty();
        assertThat(restP95).isLessThanOrEqualTo(500);
        assertFinished(expected, true);
        System.out.println("Steady load: " + result + ", REST p95=" + restP95 + " ms");
    }
}
