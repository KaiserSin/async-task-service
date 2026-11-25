package com.example.payment.load;

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
class PaymentQueueTest extends PaymentLoadSupport {

    @Test
    @DisplayName("Payments published in a burst are all registered")
    void registersBurst() throws Exception {
        startInstances(3);
        CompletableFuture<Void> sending = client.sendAtRate(60, 12, 5000);
        sending.get(30, TimeUnit.SECONDS);
        client.awaitRegistered(60, Duration.ofSeconds(90));
        assertRegistered(60);
    }

    @Test
    @DisplayName("Three instances handle 8 payments per second")
    void keepsUp() throws Exception {
        checkRate(80, 6.0);
    }

    @Test
    @EnabledIfSystemProperty(named = "load.performance", matches = "true")
    @DisplayName("Three instances handle 8 payments per second for ten minutes")
    void keepsUpForTenMinutes() throws Exception {
        checkRate(4800, 7.5);
    }

    private void checkRate(int expected, double minimumThroughput) throws Exception {
        startInstances(3);
        List<Double> restTimes = new ArrayList<>();
        AtomicInteger requestNumber = new AtomicInteger();
        AtomicReference<Long> paymentId = new AtomicReference<>();
        AtomicReference<PaymentLoadResult> latest = new AtomicReference<>();
        CompletableFuture<Void> sending = client.sendAtRate(expected, 8, 1000);

        await().atMost(Duration.ofSeconds(expected / 8 + 30))
            .pollInterval(Duration.ofMillis(250)).until(() -> {
                sending.getNow(null);
                PaymentLoadResult result = client.sample();
                latest.set(result);

                assertThat(result.sent() - result.registered()).isLessThanOrEqualTo(24);
                if (paymentId.get() == null) {
                    paymentId.set(jdbc.queryForObject("SELECT MIN(id) FROM payments", Long.class));
                }
                if (paymentId.get() != null) {
                    long started = System.nanoTime();
                    HttpResponse<String> response = response(
                        requestNumber.getAndIncrement() % instances.size(), "/api/payments/" + paymentId.get());
                    restTimes.add((System.nanoTime() - started) / 1_000_000.0);

                    assertThat(response.statusCode()).isEqualTo(200);
                }
                return result.registered() == expected;
            });
        sending.get(30, TimeUnit.SECONDS);

        PaymentLoadResult result = latest.get();
        double restP95 = PaymentLoadResult.p95(restTimes);
        assertThat(result.sent()).isEqualTo(expected);
        assertThat(result.confirmed()).isEqualTo(expected);
        assertThat(result.registered()).isEqualTo(expected);
        assertThat(result.registrationP95Ms()).isLessThanOrEqualTo(2000);
        assertThat(result.maxSendDelayMs()).isLessThanOrEqualTo(1000);
        assertThat(result.throughput()).isGreaterThanOrEqualTo(minimumThroughput);
        assertThat(restTimes).isNotEmpty();
        assertThat(restP95).isLessThanOrEqualTo(500);
        assertRegistered(expected);
        System.out.println("Steady load: " + result + ", REST p95=" + restP95 + " ms");
    }
}
