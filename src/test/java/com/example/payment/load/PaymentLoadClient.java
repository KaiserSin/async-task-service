package com.example.payment.load;

import com.example.payment.registration.PaymentCommand;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class PaymentLoadClient implements AutoCloseable {
    private final KafkaProducer<String, PaymentCommand> producer;
    private final String topic;
    private final NamedParameterJdbcTemplate jdbc;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Set<CompletableFuture<Void>> pending = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Long> sentAt = new ConcurrentHashMap<>();
    private final Map<UUID, Double> registeredAt = new HashMap<>();
    private final AtomicInteger confirmed = new AtomicInteger();
    private final AtomicLong firstSend = new AtomicLong();
    private final AtomicLong maxSendDelay = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    PaymentLoadClient(String bootstrapServers, String topic, JdbcTemplate jdbc) {
        this.topic = topic;
        this.jdbc = new NamedParameterJdbcTemplate(jdbc);
        producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000
        ), new StringSerializer(), new JsonSerializer<>());
        try {

            producer.partitionsFor(topic);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    CompletableFuture<Void> send(int count, long amount) {
        return send(Collections.nCopies(count, amount));
    }

    CompletableFuture<Void> send(List<Long> amounts) {
        return publish(List.copyOf(amounts), 0);
    }

    CompletableFuture<Void> sendAtRate(int count, int perSecond, long amount) {
        if (perSecond < 1) {
            throw new IllegalArgumentException("Publishing rate must be greater than 0");
        }
        return publish(Collections.nCopies(count, amount), perSecond);
    }

    private CompletableFuture<Void> publish(List<Long> amounts, int rate) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
        }
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        long start = System.nanoTime();
        for (int i = 0; i < amounts.size(); i++) {
            long amount = amounts.get(i);
            CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
            acknowledgements.add(acknowledgement);
            pending.add(acknowledgement);
            acknowledgement.whenComplete((ignored, error) -> pending.remove(acknowledgement));
            long deadline = rate == 0 ? 0 : start + TimeUnit.SECONDS.toNanos(1) * i / rate;
            try {
                if (rate == 0) {
                    dispatch(amount, deadline, acknowledgement);
                } else {
                    scheduler.schedule(() -> dispatch(amount, deadline, acknowledgement),
                        Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                }
            } catch (RuntimeException e) {
                acknowledgement.completeExceptionally(e);
            }
        }
        return CompletableFuture.allOf(acknowledgements.toArray(CompletableFuture[]::new));
    }

    private void dispatch(long amount, long deadline, CompletableFuture<Void> acknowledgement) {
        try {
            if (closed.get()) {
                throw new IllegalStateException("Client is closed");
            }
            UUID eventId = UUID.randomUUID();
            PaymentCommand dto = new PaymentCommand(
                eventId, "alice", UUID.randomUUID(), UUID.randomUUID(), amount, "usd", "pm_card_visa"
            );
            long now = System.nanoTime();
            firstSend.compareAndSet(0, now);
            sentAt.put(eventId, now);
            if (deadline != 0) {
                maxSendDelay.accumulateAndGet(Math.max(0, now - deadline), Math::max);
            }
            producer.send(new ProducerRecord<>(topic, eventId.toString(), dto), (record, error) -> {
                if (error != null) {
                    acknowledgement.completeExceptionally(error);
                } else {
                    confirmed.incrementAndGet();
                    acknowledgement.complete(null);
                }
            });
        } catch (RuntimeException e) {
            acknowledgement.completeExceptionally(e);
        }
    }

    int sent() {
        return sentAt.size();
    }

    int confirmed() {
        return confirmed.get();
    }

    synchronized PaymentLoadResult sample() {
        List<UUID> ids = new ArrayList<>(sentAt.keySet());
        if (!ids.isEmpty()) {
            jdbc.query("SELECT event_id FROM payments WHERE event_id IN (:ids)", Map.of("ids", ids), row -> {
                UUID id = row.getObject("event_id", UUID.class);
                registeredAt.putIfAbsent(id, (System.nanoTime() - sentAt.get(id)) / 1_000_000.0);
            });
        }
        double seconds = firstSend.get() == 0 ? 0 : (System.nanoTime() - firstSend.get()) / 1e9;
        return new PaymentLoadResult(sent(), confirmed(), registeredAt.size(),
            seconds == 0 ? 0 : registeredAt.size() / seconds,
            PaymentLoadResult.p95(registeredAt.values()), maxSendDelay.get() / 1_000_000.0);
    }

    PaymentLoadResult awaitRegistered(int expected, Duration timeout) {
        AtomicReference<PaymentLoadResult> latest = new AtomicReference<>();
        await().atMost(timeout).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            PaymentLoadResult result = sample();
            latest.set(result);
            assertThat(result.sent()).isEqualTo(expected);
            assertThat(result.confirmed()).isEqualTo(expected);
            assertThat(result.registered()).isEqualTo(expected);
        });
        return latest.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            scheduler.shutdownNow();
            pending.forEach(future -> future.completeExceptionally(new IllegalStateException("Client is closed")));
            producer.close(Duration.ofSeconds(5));
        }
    }
}
