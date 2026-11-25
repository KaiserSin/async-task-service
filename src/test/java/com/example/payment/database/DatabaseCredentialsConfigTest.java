package com.example.payment.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.util.Map;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseCredentialsConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withInitializer(context -> {
            var sources = context.getEnvironment().getPropertySources();
            sources.remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
            sources.remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
            new ConfigDataApplicationContextInitializer().initialize(context);
        })
        .withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class))
        .withUserConfiguration(DatabaseCredentialsConfig.class);

    @Test
    void missingDatabasePasswordStopsStartup() {
        contextRunner.run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage("Database password must be configured");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    void explicitlyBlankDatabasePasswordStopsStartupEvenWhenFallbackExists(String password) {
        contextRunner.withPropertyValues("POSTGRES_PASSWORD=fallback-test-only")
            .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                new MapPropertySource("database-password-test", Map.of("DB_PASSWORD", password))))
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure()).hasRootCauseMessage("Database password must be configured");
            });
    }

    @Test
    void acceptsDatabasePasswordFromExternalConfiguration() {
        contextRunner.withPropertyValues("DB_PASSWORD=database-test-only").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(DatabaseCredentialsConfig.class).hasSingleBean(DataSource.class);
            assertThat(context.getBean(JdbcConnectionDetails.class).getPassword()).isEqualTo("database-test-only");
        });
    }

    @Test
    void acceptsPostgresPasswordAsFallback() {
        contextRunner.withPropertyValues("POSTGRES_PASSWORD=postgres-test-only").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JdbcConnectionDetails.class).getPassword()).isEqualTo("postgres-test-only");
        });
    }

    @Test
    void databasePasswordTakesPrecedenceOverPostgresPassword() {
        contextRunner.withPropertyValues("DB_PASSWORD=database-test-only", "POSTGRES_PASSWORD=postgres-test-only")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(JdbcConnectionDetails.class).getPassword()).isEqualTo("database-test-only");
            });
    }

    @Test
    void acceptsDirectDatasourcePasswordOverride() {
        contextRunner.withPropertyValues("spring.datasource.password=direct-test-only").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(JdbcConnectionDetails.class).getPassword()).isEqualTo("direct-test-only");
        });
    }

    @Test
    void acceptsEffectiveConnectionDetailsInsteadOfEmptyConfiguration() {
        contextRunner.withPropertyValues("spring.datasource.password=")
            .withBean(JdbcConnectionDetails.class, () -> new JdbcConnectionDetails() {
                @Override
                public String getUsername() {
                    return "connection-test-user";
                }

                @Override
                public String getPassword() {
                    return "connection-test-only";
                }

                @Override
                public String getJdbcUrl() {
                    return "jdbc:postgresql://127.0.0.1:1/unused";
                }
            })
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(DatabaseCredentialsConfig.class).hasSingleBean(DataSource.class);
                assertThat(context.getBean(JdbcConnectionDetails.class).getPassword()).isEqualTo("connection-test-only");
            });
    }
}
