package com.example.taskapplication.controller;

import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.util.ReflectionTestUtils.setField;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TaskApiTest.Config.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false"
})
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskSecurityTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private TaskRepository taskRepository;
    @Autowired
    private ObjectMapper mapper;

    @BeforeEach
    void clearTasks() {
        taskRepository.deleteAllInBatch();
    }

    @Test
    @DisplayName("Both lookup routes reject anonymous and incorrect Basic credentials")
    void authenticatesBeforeDisclosingTasks() throws Exception {
        Task task = ownedTask(TaskStatus.NEW, "alice");
        for (String path : new String[]{byId(task), byEvent(task)}) {
            MockHttpServletResponse anonymous = mockMvc.perform(get(path))
                .andExpect(status().isUnauthorized()).andReturn().getResponse();
            assertThat(anonymous.getHeader(HttpHeaders.WWW_AUTHENTICATE)).contains("Basic");
            assertCode(anonymous, "UNAUTHORIZED");
            mockMvc.perform(get(path).with(httpBasic("alice", "wrong-password")))
                .andExpect(status().isUnauthorized());
            mockMvc.perform(get(path).with(httpBasic("missing", "alice-test-password")))
                .andExpect(status().isUnauthorized());
        }
    }

    @Test
    @DisplayName("A user reads their own task through either lookup route")
    void readsOwnTask() throws Exception {
        Task task = ownedTask(TaskStatus.NEW, "alice");
        for (String path : new String[]{byId(task), byEvent(task)}) {
            MockHttpServletResponse response = mockMvc.perform(get(path)
                    .with(httpBasic("alice", "alice-test-password")))
                .andExpect(status().isOk()).andReturn().getResponse();
            assertThat(mapper.readTree(response.getContentAsByteArray()).path("id").asLong())
                .isEqualTo(task.getId());
            assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        }
    }

    @Test
    @DisplayName("Another user's task is indistinguishable from a missing task")
    void hidesForeignTasks() throws Exception {
        Task task = ownedTask(TaskStatus.NEW, "alice");
        for (String path : new String[]{byId(task), byEvent(task)}) {
            MockHttpServletResponse response = mockMvc.perform(get(path)
                    .with(httpBasic("bob", "bob-test-password")))
                .andExpect(status().isNotFound()).andReturn().getResponse();
            assertCode(response, "TASK_NOT_FOUND");
            assertThat(response.getContentAsString()).doesNotContain("alice", "owner", "forbidden");
        }
    }

    @Test
    @DisplayName("Only an administrator reads legacy tasks without an assigned owner")
    void restrictsLegacyTasksToAdmin() throws Exception {
        Task legacy = ownedTask(TaskStatus.NEW, null);
        for (String path : new String[]{byId(legacy), byEvent(legacy)}) {
            mockMvc.perform(get(path).with(httpBasic("alice", "alice-test-password")))
                .andExpect(status().isNotFound());
            mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")))
                .andExpect(status().isOk());
        }
    }

    @Test
    @DisplayName("An administrator reads another user's task")
    void adminReadsAnyOwner() throws Exception {
        Task task = ownedTask(TaskStatus.NEW, "alice");
        mockMvc.perform(get(byId(task)).with(httpBasic("admin", "admin-test-password")))
            .andExpect(status().isOk());
    }

    @Test
    @DisplayName("A principal without USER or ADMIN cannot access task endpoints")
    void requiresAnApplicationRole() throws Exception {
        Task task = ownedTask(TaskStatus.NEW, "intruder");
        MockHttpServletResponse response = mockMvc.perform(get(byId(task))
                .with(user("intruder").roles("OTHER")))
            .andExpect(status().isForbidden()).andReturn().getResponse();
        assertCode(response, "FORBIDDEN");
    }

    @Test
    @DisplayName("An owner retries FAILED once and gets a fresh attempt budget")
    void retriesOwnFailedTask() throws Exception {
        Task task = ownedTask(TaskStatus.FAILED, "alice");

        MockHttpServletResponse response = mockMvc.perform(post(retry(task))
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isNoContent()).andReturn().getResponse();

        assertThat(response.getContentAsByteArray()).isEmpty();
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        Task queued = find(task);
        assertThat(queued.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(queued.getOwnerUsername()).isEqualTo("alice");
        assertThat(queued.getAttempts()).isZero();
        assertThat(queued.getProgress()).isZero();
        assertThat(queued.getExecutionId()).isNull();
        assertThat(queued.getLeaseUntil()).isNull();
        assertThat(queued.getNextRetryAt()).isNull();
        assertThat(queued.getStartedAt()).isNull();
        assertThat(queued.getFinishedAt()).isNull();
        assertThat(queued.getResult()).isNull();
        assertThat(queued.getVersion()).isEqualTo(task.getVersion() + 1);
        MockHttpServletResponse repeated = mockMvc.perform(post(retry(task))
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isConflict()).andReturn().getResponse();
        assertCode(repeated, "TASK_NOT_RETRYABLE");
        assertThat(find(task)).usingRecursiveComparison().isEqualTo(queued);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "alice")
    @DisplayName("An administrator retries both another user's and legacy FAILED tasks")
    void adminRetriesAnyOwner(String owner) throws Exception {
        Task task = ownedTask(TaskStatus.FAILED, owner);
        mockMvc.perform(post(retry(task)).with(httpBasic("admin", "admin-test-password")).with(csrf()))
            .andExpect(status().isNoContent());
        assertThat(find(task).getStatus()).isEqualTo(TaskStatus.NEW);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"NEW", "IN_PROGRESS", "COMPLETED"})
    @DisplayName("A manual retry cannot reset a task that is not FAILED")
    void rejectsOtherStates(TaskStatus state) throws Exception {
        Task task = ownedTask(state, "alice");
        MockHttpServletResponse response = mockMvc.perform(post(retry(task))
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isConflict()).andReturn().getResponse();
        assertCode(response, "TASK_NOT_RETRYABLE");
        assertThat(find(task)).usingRecursiveComparison().isEqualTo(task);
    }

    @Test
    @DisplayName("Anonymous, foreign-owner and missing-CSRF retries leave the task unchanged")
    void doesNotMutateUnauthorizedRetries() throws Exception {
        Task task = ownedTask(TaskStatus.FAILED, "alice");
        mockMvc.perform(post(retry(task)).with(csrf())).andExpect(status().isUnauthorized());
        mockMvc.perform(post(retry(task)).with(httpBasic("bob", "bob-test-password")).with(csrf()))
            .andExpect(status().isNotFound());
        mockMvc.perform(post(retry(task)).with(httpBasic("alice", "alice-test-password")))
            .andExpect(status().isForbidden());
        assertThat(find(task)).usingRecursiveComparison().isEqualTo(task);
    }

    @Test
    @DisplayName("The CSRF endpoint provides a token usable with the same session and Basic credentials")
    void retriesWithARealCsrfToken() throws Exception {
        Task task = ownedTask(TaskStatus.FAILED, "alice");
        var result = mockMvc.perform(get("/api/csrf").with(httpBasic("alice", "alice-test-password")))
            .andExpect(status().isOk()).andReturn();
        JsonNode token = mapper.readTree(result.getResponse().getContentAsByteArray());
        MockHttpSession session = (MockHttpSession) result.getRequest().getSession(false);
        assertThat(session).isNotNull();
        assertThat(token.path("token").asText()).isNotBlank();

        mockMvc.perform(get(byId(task)).session(session))
            .andExpect(status().isUnauthorized());
        mockMvc.perform(post(retry(task)).session(session)
                .header(token.path("headerName").asText(), token.path("token").asText()))
            .andExpect(status().isUnauthorized());
        assertThat(find(task).getStatus()).isEqualTo(TaskStatus.FAILED);

        mockMvc.perform(post(retry(task)).with(httpBasic("alice", "alice-test-password"))
                .session(session).header(token.path("headerName").asText(), token.path("token").asText()))
            .andExpect(status().isNoContent());
        assertThat(find(task).getStatus()).isEqualTo(TaskStatus.NEW);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc"})
    @DisplayName("The retry endpoint validates its task ID")
    void validatesRetryId(String id) throws Exception {
        mockMvc.perform(post("/api/tasks/" + id + "/retry")
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("A manual retry of a missing task returns 404")
    void rejectsMissingRetry() throws Exception {
        mockMvc.perform(post("/api/tasks/9223372036854775807/retry")
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Health is public; documentation and management require ADMIN; unspecified routes stay denied")
    void restrictsManagementAndDocumentation() throws Exception {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk());
        for (String path : new String[]{"/v3/api-docs", "/actuator/metrics"}) {
            mockMvc.perform(get(path)).andExpect(status().isUnauthorized());
            mockMvc.perform(get(path).with(httpBasic("alice", "alice-test-password")))
                .andExpect(status().isForbidden());
            mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")))
                .andExpect(status().isOk());
        }
        mockMvc.perform(get("/unspecified").with(httpBasic("admin", "admin-test-password")))
            .andExpect(status().isForbidden());
    }

    private Task ownedTask(TaskStatus status, String owner) {
        Task task = TaskApiTest.task(status == TaskStatus.NEW ? TaskStatus.NEW : TaskStatus.COMPLETED, owner);
        setField(task, "status", status);
        if (status == TaskStatus.FAILED) {
            setField(task, "attempts", 3);
            setField(task, "progress", 42);
            setField(task, "result", "Task failed");
        } else if (status == TaskStatus.IN_PROGRESS) {
            setField(task, "finishedAt", null);
            setField(task, "progress", 40);
        }
        return taskRepository.saveAndFlush(task);
    }

    private Task find(Task task) {
        return taskRepository.findById(task.getId()).orElseThrow();
    }

    private String byId(Task task) {
        return "/api/tasks/" + task.getId();
    }

    private String byEvent(Task task) {
        return "/api/tasks/by-event-id/" + task.getEventId();
    }

    private String retry(Task task) {
        return byId(task) + "/retry";
    }

    private void assertCode(MockHttpServletResponse response, String expected) throws Exception {
        assertThat(mapper.readTree(response.getContentAsByteArray()).path("code").asText()).isEqualTo(expected);
    }
}
