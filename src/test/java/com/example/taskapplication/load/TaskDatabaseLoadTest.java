package com.example.taskapplication.load;

import com.example.taskapplication.model.TaskStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Tag("load")
@EnabledIfSystemProperty(named = "load.tests", matches = "true")
class TaskDatabaseLoadTest extends TaskLoadSupport {

    @Test
    @DisplayName("Occupied connections produce a bounded 503 response, then messages are processed")
    void fillsConnections() throws Exception {
        startInstances(2);
        client.send(4, 1000).get(30, TimeUnit.SECONDS);
        client.awaitCompleted(4, Duration.ofSeconds(30));
        Long id = jdbc.queryForObject("SELECT MIN(id) FROM tasks", Long.class);

        List<Connection> connections = new ArrayList<>();
        try {

            for (var instance : instances) {
                DataSource dataSource = instance.getBean(DataSource.class);
                for (int i = 0; i < 10; i++) {
                    connections.add(dataSource.getConnection());
                }
            }
            long started = System.nanoTime();
            assertThat(response(0, "/api/tasks/" + id).statusCode()).isEqualTo(503);
            assertThat(System.nanoTime() - started).isLessThan(TimeUnit.SECONDS.toNanos(15));

            client.send(8, 1000).get(30, TimeUnit.SECONDS);
            assertThat(client.confirmed()).isEqualTo(12);

            assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isEqualTo(4);
        } finally {
            closeConnections(connections);
        }

        System.out.println("After connections are released: "
            + client.awaitCompleted(12, Duration.ofSeconds(90)));
        assertFinished(12, true);
    }

    @Test
    @DisplayName("Write delays pause Kafka without sending tasks to DLT")
    void waitsForDatabase() throws Exception {
        startInstances(2);
        try (Connection connection = DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            connection.setAutoCommit(false);
            try {

                try (var statement = connection.createStatement()) {
                    statement.execute("LOCK TABLE tasks IN SHARE MODE");
                }
                client.send(12, 1000).get(30, TimeUnit.SECONDS);
                await().atMost(Duration.ofSeconds(10)).until(() -> jdbc.queryForObject("""
                    SELECT COUNT(*) FROM pg_stat_activity
                    WHERE datname = current_database() AND wait_event_type = 'Lock'
                      AND query LIKE '%tasks%'
                    """, Integer.class) > 0);
                await().atMost(Duration.ofSeconds(20)).until(this::listenerPaused);
                assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM tasks", Integer.class)).isZero();
            } finally {
                connection.rollback();
            }
        }

        System.out.println("After the database lock is released: "
            + client.awaitCompleted(12, Duration.ofSeconds(90)));
        assertFinished(12, true);
    }

    @Test
    @DisplayName("The queue and unfinished tasks recover after a PostgreSQL outage")
    void recoversDatabase() throws Exception {
        startInstances(2);
        client.send(4, 5000).get(30, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(15)).until(() -> count(TaskStatus.IN_PROGRESS) == 4);
        Long id = jdbc.queryForObject("SELECT MIN(id) FROM tasks", Long.class);

        try {
            pauseDatabase();
            client.send(8, 1000).get(30, TimeUnit.SECONDS);
            assertThat(client.confirmed()).isEqualTo(12);
            long started = System.nanoTime();
            assertThat(response(0, "/api/tasks/" + id).statusCode()).isEqualTo(503);
            assertThat(System.nanoTime() - started).isLessThan(TimeUnit.SECONDS.toNanos(15));
        } finally {
            resumeDatabase();
        }

        System.out.println("After PostgreSQL recovery: "
            + client.awaitCompleted(12, Duration.ofSeconds(150)));
        assertFinished(12, false);
    }

    private boolean listenerPaused() {
        return instances.stream()
            .flatMap(instance -> instance.getBean(KafkaListenerEndpointRegistry.class)
                .getListenerContainers().stream())
            .anyMatch(container -> {
                if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrent) {
                    return concurrent.getContainers().stream()
                        .anyMatch(MessageListenerContainer::isPauseRequested);
                }
                return container.isPauseRequested();
            });
    }

    private void closeConnections(List<Connection> connections) throws SQLException {
        SQLException failure = null;
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
