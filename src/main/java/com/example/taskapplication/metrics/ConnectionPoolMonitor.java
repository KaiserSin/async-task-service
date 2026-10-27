package com.example.taskapplication.metrics;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
@Slf4j
@RequiredArgsConstructor
public class ConnectionPoolMonitor {

    private final DataSource dataSource;

    @Scheduled(fixedDelay = 60000)
    public void monitorConnectionPool() {
        if (!(dataSource instanceof HikariDataSource hikari)) {
            return;
        }

        HikariPoolMXBean pool = hikari.getHikariPoolMXBean();
        if (pool == null) {
            return;
        }

        log.info("Connection pool: active={}, idle={}, total={}, waiting={}",
            pool.getActiveConnections(),
            pool.getIdleConnections(),
            pool.getTotalConnections(),
            pool.getThreadsAwaitingConnection());
    }
}
