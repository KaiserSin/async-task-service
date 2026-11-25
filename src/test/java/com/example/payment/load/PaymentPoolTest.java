package com.example.payment.load;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("load")
@EnabledIfSystemProperty(named = "load.tests", matches = "true")
class PaymentPoolTest extends PaymentLoadSupport {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    @DisplayName("Multiple instances register payments and leave them awaiting submission")
    void registersPayments(int instanceCount) throws Exception {
        startInstances(instanceCount);
        int capacity = instanceCount * 4;

        client.send(capacity, 30_000).get(30, TimeUnit.SECONDS);

        client.send(capacity, 1000).get(30, TimeUnit.SECONDS);
        client.awaitRegistered(capacity * 2, Duration.ofSeconds(60));
        assertRegistered(capacity * 2);
        assertThat(activeWorkers()).isZero();
    }

    @Test
    @DisplayName("Payments with different amounts are registered across instances")
    void registersMixedAmounts() throws Exception {
        startInstances(2);
        List<Long> amounts = new ArrayList<>();
        amounts.add(60_000L);
        amounts.addAll(Collections.nCopies(15, 1000L));
        client.send(amounts).get(30, TimeUnit.SECONDS);

        client.awaitRegistered(16, Duration.ofSeconds(30));
        assertRegistered(16);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE amount = 60000", Integer.class))
            .isEqualTo(1);
    }
}
