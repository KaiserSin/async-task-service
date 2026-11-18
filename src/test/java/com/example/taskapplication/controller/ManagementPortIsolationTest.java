package com.example.taskapplication.controller;

import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = TaskApiTest.Config.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "management.server.port=0",
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.datasource.hikari.register-mbeans=false"
    })
@ActiveProfiles({"test", "prod"})
@AutoConfigureObservability
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ManagementPortIsolationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    private int apiPort;
    @LocalManagementPort
    private int managementPort;
    @Autowired
    private Environment environment;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private ObjectMapper mapper;

    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void actuatorUsesItsOwnPortAndIsAbsentFromTheApiPortEvenForAdmin() throws Exception {
        assertThat(apiPort).isPositive().isNotEqualTo(managementPort);
        assertThat(managementPort).isPositive();
        for (String path : List.of("/actuator", "/actuator/health", "/actuator/health/readiness",
            "/actuator/health/liveness", "/actuator/info", "/actuator/metrics", "/actuator/prometheus")) {
            assertStatus(apiPort, path, "admin", 404);
        }
        assertStatus(managementPort, "/api/csrf", "admin", 404);
    }

    @Test
    void managementKeepsHealthPublicAndMetricsRestrictedToAdmin() throws Exception {
        for (String path : List.of("/actuator/health", "/actuator/health/readiness", "/actuator/health/liveness")) {
            var response = assertStatus(managementPort, path, null, 200);
            var body = mapper.readTree(response.body());
            assertThat(body.path("status").asText()).isEqualTo("UP");
            assertThat(body.has("components")).isFalse();
            assertThat(body.has("details")).isFalse();
        }
        for (String path : List.of("/actuator/metrics", "/actuator/prometheus")) {
            assertStatus(managementPort, path, null, 401);
            assertStatus(managementPort, path, "alice", 403);
            assertStatus(managementPort, path, "admin", 200);
        }
    }

    @Test
    void productionDisablesSwaggerAndOpenApiOnBothPortsEvenForAdmin() throws Exception {
        assertThat(environment.getProperty("springdoc.api-docs.enabled", Boolean.class)).isFalse();
        assertThat(environment.getProperty("springdoc.swagger-ui.enabled", Boolean.class)).isFalse();
        for (int port : new int[]{apiPort, managementPort}) {
            for (String path : List.of("/v3/api-docs", "/v3/api-docs.yaml", "/swagger-ui.html",
                "/swagger-ui/index.html")) {
                assertStatus(port, path, "admin", 404);
            }
        }
    }

    @Test
    void taskApiStillAuthenticatesAndServesTheOwnerOnTheApplicationPort() throws Exception {
        var task = TaskApiTest.task(TaskStatus.NEW);
        task.setOwnerUsername("alice");
        task = taskRepository.saveAndFlush(task);
        String path = "/api/tasks/" + task.getId();

        assertStatus(apiPort, path, null, 401);
        var response = assertStatus(apiPort, path, "alice", 200);
        assertThat(mapper.readTree(response.body()).path("id").asLong()).isEqualTo(task.getId());
        assertStatus(apiPort, path, "bob", 404);
    }

    private HttpResponse<String> assertStatus(int port, String path, String username, int expected) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .timeout(Duration.ofSeconds(10)).GET();
        if (username != null) {
            String credentials = username + ":" + username + "-test-password";
            request.header("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
        }
        var response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("GET %s on port %s: %s", path, port, response.body())
            .isEqualTo(expected);
        return response;
    }
}
