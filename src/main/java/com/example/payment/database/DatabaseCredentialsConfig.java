package com.example.payment.database;

import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration(proxyBeanMethods = false)
class DatabaseCredentialsConfig {

    public DatabaseCredentialsConfig(JdbcConnectionDetails connectionDetails) {
        if (!StringUtils.hasText(connectionDetails.getPassword())) {
            throw new IllegalStateException("Database password must be configured");
        }
    }
}
