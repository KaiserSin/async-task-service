package com.example.payment.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.jpa.repository.Query;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
class PaymentMetricsRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private PaymentMetricsRepository metricsRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Queue metrics use the partial NEW index and read counts and ready age")
    void readsQueueMetrics() throws Exception {
        String metricsSql = PaymentMetricsRepository.class.getMethod("readQueueStats")
            .getAnnotation(Query.class).value();
        var registry = new SimpleMeterRegistry();
        try {
            PaymentMetrics metrics = new PaymentMetrics(registry, metricsRepository);
            metrics.updateQueueMetrics();
            assertThat(registry.get("payment.queue.new").gauge().value()).isZero();
            assertThat(registry.get("payment.queue.oldest.ready.age").timeGauge()
                .value(TimeUnit.SECONDS)).isZero();

            jdbcTemplate.update("""
                INSERT INTO payments (
                    event_id, owner_username, payment_operation_id, order_id, amount, currency, payment_method_id, idempotency_key, created_at, next_retry_at, attempts
                ) VALUES
                    (?, 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, CURRENT_TIMESTAMP - INTERVAL '30 seconds', NULL, 0),
                    (?, 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, CURRENT_TIMESTAMP - INTERVAL '1 hour',
                        CURRENT_TIMESTAMP + INTERVAL '1 minute', 1),
                    (?, 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, CURRENT_TIMESTAMP - INTERVAL '1 hour', NULL, 3),
                    (?, 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, CURRENT_TIMESTAMP - INTERVAL '20 seconds',
                        CURRENT_TIMESTAMP - INTERVAL '1 second', 1)
                """, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
            jdbcTemplate.update("""
                INSERT INTO payments (event_id, owner_username, payment_operation_id, order_id, amount, currency, payment_method_id, idempotency_key, execution_status, finished_at)
                SELECT gen_random_uuid(), 'alice', gen_random_uuid(), gen_random_uuid(), 1000, 'eur', 'pm_test', gen_random_uuid()::text, 'COMPLETED', CURRENT_TIMESTAMP
                FROM generate_series(1, 10000)
                """);
            jdbcTemplate.execute("ANALYZE payments");

            metrics.updateQueueMetrics();

            assertThat(registry.get("payment.queue.new").gauge().value()).isEqualTo(4);
            assertThat(registry.get("payment.queue.oldest.ready.age").timeGauge()
                .value(TimeUnit.SECONDS)).isEqualTo(30);
            String plan = String.join("\n", jdbcTemplate.queryForList(
                "EXPLAIN (ANALYZE, BUFFERS) " + metricsSql, String.class));
            assertThat(plan).contains("idx_payments_new_id").doesNotContain("Seq Scan on payments");
        } finally {
            registry.close();
        }
    }

    @Test
    void readsMetricsAndWarningsWhenThereAreNoPayments() {
        assertThat(metricsRepository.readFailedUnknownStats())
            .isEqualTo(new PaymentMetricsRepository.FailedUnknownStats(0, 0));
        assertThat(metricsRepository.readLatencyP95())
            .isEqualTo(new PaymentMetricsRepository.LatencyP95(Double.NaN, Double.NaN, Double.NaN));
        assertThat(metricsRepository.findLongProcessingPaymentIds()).isEmpty();
        assertThat(metricsRepository.findOldestOverdueCheck()).isEmpty();
    }
}
