package com.example.taskapplication.health;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Component("taskDatabase")
@RequiredArgsConstructor
public class DatabaseHealthCheck extends AbstractHealthIndicator {

    private final DataSource dataSource;

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        try (Connection connection = dataSource.getConnection()) {
            if (connection.isValid(2)) {
                builder.up().withDetail("database", "PostgreSQL");
            } else {
                builder.down().withDetail("error", "Database connection check failed");
            }
        } catch (SQLException e) {
            builder.down(e);
        }
    }
}
