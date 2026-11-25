package com.example.payment.api.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class ApiSecurityPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withUserConfiguration(PropertiesConfiguration.class)
        .withPropertyValues("PAYMENT_API_PASSWORD=test-user-password", "PAYMENT_API_ADMIN_PASSWORD=test-admin-password");

    @ParameterizedTest
    @ValueSource(strings = {"PAYMENT_API_PASSWORD=", "PAYMENT_API_ADMIN_PASSWORD=", "PAYMENT_API_PASSWORD= "})
    void refusesToStartWithoutBothPasswords(String missingPassword) {
        contextRunner.withPropertyValues(missingPassword).run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(BindValidationException.class);
        });
    }

    @Test
    void bindsConfiguredUsersAndRolesFromApplicationYaml() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            var accounts = context.getBean(ApiSecurityProperties.class).users();
            assertThat(accounts).hasSize(2);
            assertThat(accounts.getFirst().roles()).containsExactly("USER");
            assertThat(accounts.getLast().roles()).containsExactly("ADMIN");
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ApiSecurityProperties.class)
    static class PropertiesConfiguration {
    }
}
