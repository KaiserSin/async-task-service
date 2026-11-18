package com.example.taskapplication.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.autoconfigure.web.server.ManagementPortType;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class ManagementConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withInitializer(new ConfigDataApplicationContextInitializer());

    @Test
    void managementUsesASeparatePortByDefault() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            var environment = context.getEnvironment();
            assertThat(environment.getProperty("management.server.port", Integer.class)).isEqualTo(9091);
            assertThat(ManagementPortType.get(environment)).isEqualTo(ManagementPortType.DIFFERENT);
            assertThat(environment.getProperty("management.server.address")).isNull();
        });
    }

    @Test
    void productionDisablesSwaggerAndBindsManagementToLoopback() {
        contextRunner.withPropertyValues("spring.profiles.active=prod").run(context -> {
            assertThat(context).hasNotFailed();
            var environment = context.getEnvironment();
            assertThat(environment.getProperty("springdoc.api-docs.enabled", Boolean.class)).isFalse();
            assertThat(environment.getProperty("springdoc.swagger-ui.enabled", Boolean.class)).isFalse();
            assertThat(environment.getProperty("management.server.address")).isEqualTo("127.0.0.1");
            assertThat(ManagementPortType.get(environment)).isEqualTo(ManagementPortType.DIFFERENT);
        });
    }

    @Test
    void mockMvcProfileKeepsManagementOnTheSamePortWithoutAnAddressOverride() {
        contextRunner.withPropertyValues("spring.profiles.active=test").run(context -> {
            assertThat(context).hasNotFailed();
            var environment = context.getEnvironment();
            assertThat(environment.getProperty("management.server.port", Integer.class)).isNull();
            assertThat(environment.getProperty("management.server.address")).isNull();
            assertThat(ManagementPortType.get(environment)).isEqualTo(ManagementPortType.SAME);
        });
    }

    @Test
    void realHttpTestsRequestSeparateRandomPortsEvenWhenBothAreZero() {
        contextRunner.withPropertyValues("spring.profiles.active=test,prod",
            "server.port=0", "management.server.port=0").run(context -> {
                assertThat(context).hasNotFailed();
                var environment = context.getEnvironment();
                assertThat(ManagementPortType.get(environment)).isEqualTo(ManagementPortType.DIFFERENT);
                assertThat(environment.getProperty("springdoc.api-docs.enabled", Boolean.class)).isFalse();
                assertThat(environment.getProperty("springdoc.swagger-ui.enabled", Boolean.class)).isFalse();
            });
    }
}
