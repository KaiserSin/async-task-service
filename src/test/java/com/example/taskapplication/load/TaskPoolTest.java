package com.example.taskapplication.load;

import com.example.taskapplication.model.TaskStatus;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Tag("load")
@EnabledIfSystemProperty(named = "load.tests", matches = "true")
class TaskPoolTest extends TaskLoadSupport {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    @DisplayName("A full pool leaves new tasks in the database without creating extra workers")
    void fillsPool(int instanceCount) throws Exception {
        startInstances(instanceCount);
        int capacity = instanceCount * 4;

        client.send(capacity, 30_000).get(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(count(TaskStatus.IN_PROGRESS)).isEqualTo(capacity);
            assertThat(activeWorkers()).isEqualTo(capacity);
        });

        client.send(capacity, 1000).get(30, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(client.sample().registered()).isEqualTo(capacity * 2);
            assertThat(count(TaskStatus.NEW)).isEqualTo(capacity);
            assertThat(count(TaskStatus.IN_PROGRESS)).isEqualTo(capacity);
            instances.forEach(instance -> assertThat(instance.getBean(
                "taskExecutor", ThreadPoolExecutor.class).getActiveCount()).isLessThanOrEqualTo(4));
        });

        TaskLoadResult result = client.awaitCompleted(capacity * 2, Duration.ofSeconds(60));
        assertFinished(capacity * 2, true);
        System.out.println("Instances: " + instanceCount + ", " + result);
    }

    @Test
    @DisplayName("Short tasks finish alongside a minute-long task while its lease is renewed")
    void mixesDurations() throws Exception {
        startInstances(2);
        List<Long> durations = new ArrayList<>();
        durations.add(60_000L);
        durations.addAll(Collections.nCopies(15, 1000L));
        client.send(durations).get(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(count(TaskStatus.COMPLETED)).isEqualTo(15);
            assertThat(count(TaskStatus.IN_PROGRESS)).isEqualTo(1);
            assertThat(jdbc.queryForObject("""
                SELECT COUNT(*) FROM tasks
                WHERE duration = 60000 AND status = 'IN_PROGRESS'
                  AND progress > 0 AND attempts = 1
                  AND lease_until > CURRENT_TIMESTAMP
                """, Integer.class)).isEqualTo(1);
        });

        TaskLoadResult result = client.awaitCompleted(16, Duration.ofSeconds(90));
        assertFinished(16, true);
        System.out.println("Mixed durations: " + result);
    }
}
