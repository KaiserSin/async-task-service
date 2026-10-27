package com.example.taskapplication.controller;

import com.example.taskapplication.exception.GlobalExceptionHandler;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.service.query.TaskQueryService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TaskApiTest.Config.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false"
})
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskApiTest {

    private static final UUID EVENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final List<String> TASK_FIELDS = List.of(
        "id", "eventId", "name", "duration", "status", "progress", "result",
        "createdAt", "startedAt", "finishedAt"
    );

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TaskRepository taskRepository;

    @MockitoSpyBean
    private TaskQueryService taskQueryService;

    @BeforeEach
    void setUp() {
        taskRepository.deleteAllInBatch();
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"NEW", "COMPLETED"})
    @DisplayName("Returns public fields of new and completed tasks by ID")
    void findsTask(TaskStatus taskStatus) throws Exception {
        Task task = taskRepository.saveAndFlush(task(taskStatus));

        JsonNode body = getJson("/api/tasks/" + task.getId(), 200);

        assertTask(body, task);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"NEW", "COMPLETED"})
    @DisplayName("Returns the same DTO with the internal ID by eventId")
    void findsByEventId(TaskStatus taskStatus) throws Exception {
        Task task = taskRepository.saveAndFlush(task(taskStatus));

        JsonNode body = getJson("/api/tasks/by-event-id/" + task.getEventId(), 200);

        assertTask(body, task);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc", "9223372036854775808"})
    @DisplayName("Invalid ID returns 400 with a clear message")
    void rejectsId(String id) throws Exception {
        assertError(getJson("/api/tasks/" + id, 400), "INVALID_ARGUMENT",
            "Task ID must be a whole number greater than 0");
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-uuid", "123"})
    @DisplayName("Invalid eventId returns 400 with a UUID message")
    void rejectsEventId(String eventId) throws Exception {
        assertError(getJson("/api/tasks/by-event-id/" + eventId, 400), "INVALID_ARGUMENT",
            "eventId must be a valid UUID");
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "eventId"})
    @DisplayName("Missing task returns 404 for both identifiers")
    void missesTask(String lookup) throws Exception {
        String value = lookup.equals("id") ? "9223372036854775807" : EVENT_ID.toString();
        String path = lookup.equals("id") ? "/api/tasks/" : "/api/tasks/by-event-id/";
        String message = lookup.equals("id")
            ? "Task with ID " + value + " was not found"
            : "Task with eventId " + value + " was not found";

        assertError(getJson(path + value, 404), "TASK_NOT_FOUND", message);
    }

    @Test
    @DisplayName("Connection and transaction creation failures return 503")
    void handlesDatabaseError() throws Exception {

        doThrow(new CannotCreateTransactionException("Sensitive detail about the connection"))
            .when(taskQueryService).findById(1L);
        doThrow(new DataAccessResourceFailureException("Sensitive detail about SQL"))
            .when(taskQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/tasks/1", "/api/tasks/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
        }
    }

    @Test
    @DisplayName("Connection failure during rollback returns 503")
    void handlesRollbackError() throws Exception {
        doThrow(new JpaSystemException(new org.hibernate.TransactionException(
            "Unable to rollback against JDBC Connection", new SQLException("Connection is closed")
        ))).when(taskQueryService).findById(1L);
        doThrow(new TransactionSystemException("Sensitive detail about rollback",
            new SQLException("Sensitive detail about the connection", "08006")))
            .when(taskQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/tasks/1", "/api/tasks/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
        }
    }

    @Test
    @DisplayName("Unexpected failure returns 500 without internal details")
    void handlesUnexpectedError() throws Exception {
        doThrow(new IllegalStateException("Sensitive detail about execution"))
            .when(taskQueryService).findById(1L);
        doThrow(new JpaSystemException(new org.hibernate.TransactionException(
            "Sensitive detail about execution", new SQLException("Sensitive detail about SQL", "42601")
        ))).when(taskQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/tasks/1", "/api/tasks/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 500), "INTERNAL_ERROR", "Internal server error");
        }
    }

    @Test
    @DisplayName("Handler preserves the standard 405 status and Allow header")
    void keepsHttpStatus() throws Exception {
        MockHttpServletResponse response = mockMvc.perform(post("/api/tasks/1"))
            .andExpect(status().isMethodNotAllowed())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn().getResponse();

        assertThat(response.getHeader(HttpHeaders.ALLOW)).contains("GET");
        assertError(objectMapper.readTree(response.getContentAsByteArray()),
            "HTTP_405", "Invalid request");
    }

    static Task task(TaskStatus taskStatus) {
        Task task = new Task();
        task.setEventId(UUID.randomUUID());
        task.setName("Task");
        task.setDuration(2500L);
        task.setCreatedAt(Instant.parse("2026-01-01T10:00:00Z"));
        if (taskStatus == TaskStatus.COMPLETED) {
            task.setStatus(TaskStatus.COMPLETED);
            task.setProgress(100);
            task.setResult("Done");
            task.setExecutionId(UUID.randomUUID());
            task.setAttempts(1);
            task.setStartedAt(task.getCreatedAt().plusSeconds(1));
            task.setFinishedAt(task.getCreatedAt().plusSeconds(4));
        }
        return task;
    }

    private JsonNode getJson(String path, int expectedStatus) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(get(path).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().is(expectedStatus))
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn().getResponse();
        if ((expectedStatus == 200 || expectedStatus == 404) && path.startsWith("/api/tasks/")) {
            assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        }
        return objectMapper.readTree(response.getContentAsByteArray());
    }

    private void assertTask(JsonNode body, Task task) {
        assertThat(fields(body)).containsExactlyInAnyOrderElementsOf(TASK_FIELDS);
        assertThat(body.path("id").isIntegralNumber()).isTrue();
        assertThat(body.path("id").asLong()).isEqualTo(task.getId());
        assertThat(body.path("eventId").asText()).isEqualTo(task.getEventId().toString());
        assertThat(body.path("name").asText()).isEqualTo(task.getName());
        assertThat(body.path("duration").isIntegralNumber()).isTrue();
        assertThat(body.path("duration").asLong()).isEqualTo(task.getDuration());
        assertThat(body.path("status").asText()).isEqualTo(task.getStatus().name());
        assertThat(body.path("progress").isIntegralNumber()).isTrue();
        assertThat(body.path("progress").asInt()).isEqualTo(task.getProgress());
        assertThat(Instant.parse(body.path("createdAt").asText())).isEqualTo(task.getCreatedAt());
        if (task.getStatus() == TaskStatus.NEW) {
            assertThat(body.path("result").isNull()).isTrue();
            assertThat(body.path("startedAt").isNull()).isTrue();
            assertThat(body.path("finishedAt").isNull()).isTrue();
        } else {
            assertThat(body.path("result").asText()).isEqualTo(task.getResult());
            assertThat(Instant.parse(body.path("startedAt").asText())).isEqualTo(task.getStartedAt());
            assertThat(Instant.parse(body.path("finishedAt").asText())).isEqualTo(task.getFinishedAt());
        }
    }

    static void assertError(JsonNode body, String code, String message) {
        assertThat(fields(body)).containsExactlyInAnyOrder("code", "message");
        assertThat(body.path("code").asText()).isEqualTo(code);
        assertThat(body.path("message").asText()).isEqualTo(message);
        assertThat(body.toString()).doesNotContain("Sensitive detail", "org.springframework", "SQLException");
    }

    private static List<String> fields(JsonNode object) {
        List<String> fields = new ArrayList<>();
        object.fieldNames().forEachRemaining(fields::add);
        return fields;
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
    @EntityScan(basePackageClasses = Task.class)
    @EnableJpaRepositories(basePackageClasses = TaskRepository.class)
    @Import({TaskController.class, GlobalExceptionHandler.class, TaskQueryService.class})
    static class Config {
    }
}

@SpringBootTest(classes = TaskApiTest.Config.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false",
    "spring.datasource.hikari.connection-timeout=1000",
    "spring.datasource.hikari.validation-timeout=500",
    "spring.datasource.hikari.data-source-properties.connectTimeout=1",
    "spring.datasource.hikari.data-source-properties.socketTimeout=1"
})
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskDatabaseTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TaskRepository taskRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Actual PostgreSQL outage returns 503 within a bounded time")
    void losesDatabase() throws Exception {
        Task task = taskRepository.saveAndFlush(TaskApiTest.task(TaskStatus.NEW));
        String path = "/api/tasks/" + task.getId();
        mockMvc.perform(get(path)).andExpect(status().isOk());

        postgres.stop();

        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            MockHttpServletResponse response = mockMvc.perform(get(path))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andReturn().getResponse();
            TaskApiTest.assertError(objectMapper.readTree(response.getContentAsByteArray()),
                "SERVICE_UNAVAILABLE", "Service is not available right now");
        });
    }
}
