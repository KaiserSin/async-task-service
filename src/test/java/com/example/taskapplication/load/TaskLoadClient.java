package com.example.taskapplication.load;

import com.example.taskapplication.dto.TaskRequestDTO;
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

class TaskLoadClient implements AutoCloseable {
    private final KafkaProducer<String, TaskRequestDTO> producer;
    private final String topic;
    private final NamedParameterJdbcTemplate jdbc;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Set<CompletableFuture<Void>> pending = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Long> sentAt = new ConcurrentHashMap<>();
    private final Map<UUID, Double> registeredAt = new HashMap<>();
    private final Map<UUID, Double> completedAt = new HashMap<>();
    private final Map<UUID, Double> queueWait = new HashMap<>();
    private final AtomicInteger confirmed = new AtomicInteger();
    private final AtomicLong firstSend = new AtomicLong();
    private final AtomicLong maxSendDelay = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    TaskLoadClient(String bootstrapServers, String topic, JdbcTemplate jdbc) {
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

    CompletableFuture<Void> send(int count, long duration) {
        return send(Collections.nCopies(count, duration));
    }

    CompletableFuture<Void> send(List<Long> durations) {
        return publish(List.copyOf(durations), 0);
    }

    CompletableFuture<Void> sendAtRate(int count, int perSecond, long duration) {
        if (perSecond < 1) {
            throw new IllegalArgumentException("Publishing rate must be greater than 0");
        }
        return publish(Collections.nCopies(count, duration), perSecond);
    }

    private CompletableFuture<Void> publish(List<Long> durations, int rate) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
        }
        List<CompletableFuture<Void>> acknowledgements = new ArrayList<>();
        long start = System.nanoTime();
        for (int i = 0; i < durations.size(); i++) {
            long duration = durations.get(i);
            CompletableFuture<Void> acknowledgement = new CompletableFuture<>();
            acknowledgements.add(acknowledgement);
            pending.add(acknowledgement);
            acknowledgement.whenComplete((ignored, error) -> pending.remove(acknowledgement));
            long deadline = rate == 0 ? 0 : start + TimeUnit.SECONDS.toNanos(1) * i / rate;
            try {
                if (rate == 0) {
                    dispatch(duration, deadline, acknowledgement);
                } else {
                    scheduler.schedule(() -> dispatch(duration, deadline, acknowledgement),
                        Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                }
            } catch (RuntimeException e) {
                acknowledgement.completeExceptionally(e);
            }
        }
        return CompletableFuture.allOf(acknowledgements.toArray(CompletableFuture[]::new));
    }

    private void dispatch(long duration, long deadline, CompletableFuture<Void> acknowledgement) {
        try {
            if (closed.get()) {
                throw new IllegalStateException("Client is closed");
            }
            UUID eventId = UUID.randomUUID();
            TaskRequestDTO dto = new TaskRequestDTO();
            dto.setEventId(eventId);
            dto.setName("Load task " + eventId);
            dto.setDuration(duration);
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

    synchronized TaskLoadResult sample() {
        int[] counts = new int[4];
        List<UUID> ids = new ArrayList<>(sentAt.keySet());
        if (!ids.isEmpty()) {
            jdbc.query("""
                SELECT event_id, status,
                    EXTRACT(EPOCH FROM (started_at - created_at)) * 1000 AS wait_ms
                FROM tasks WHERE event_id IN (:ids)
                """, Map.of("ids", ids), row -> {
                UUID id = row.getObject("event_id", UUID.class);
                double observedMs = (System.nanoTime() - sentAt.get(id)) / 1_000_000.0;

                registeredAt.putIfAbsent(id, observedMs);
                if (row.getObject("wait_ms") != null) {
                    queueWait.putIfAbsent(id, row.getDouble("wait_ms"));
                }
                switch (row.getString("status")) {
                    case "NEW" -> counts[0]++;
                    case "IN_PROGRESS" -> counts[1]++;
                    case "COMPLETED" -> {
                        counts[2]++;
                        completedAt.putIfAbsent(id, observedMs);
                    }
                    case "FAILED" -> counts[3]++;
                    default -> throw new IllegalStateException("Unknown task status");
                }
            });
        }
        double seconds = firstSend.get() == 0 ? 0 : (System.nanoTime() - firstSend.get()) / 1e9;
        return new TaskLoadResult(sent(), confirmed(), counts[0] + counts[1] + counts[2] + counts[3],
            counts[0], counts[1], counts[2], counts[3], seconds == 0 ? 0 : counts[2] / seconds,
            TaskLoadResult.p95(registeredAt.values()), TaskLoadResult.p95(queueWait.values()),
            TaskLoadResult.p95(completedAt.values()), maxSendDelay.get() / 1_000_000.0);
    }

    TaskLoadResult awaitCompleted(int expected, Duration timeout) {
        AtomicReference<TaskLoadResult> latest = new AtomicReference<>();
        await().atMost(timeout).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            TaskLoadResult result = sample();
            latest.set(result);
            assertThat(result.sent()).isEqualTo(expected);
            assertThat(result.confirmed()).isEqualTo(expected);
            assertThat(result.registered()).isEqualTo(expected);
            assertThat(result.completed()).isEqualTo(expected);
            assertThat(result.failed()).isZero();
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
