package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.exception.TaskNotFoundException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.service.security.TaskAccessService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskRetryServiceTest {

    private static final Long TASK_ID = 7L;
    private static final UUID EVENT_ID = UUID.fromString("ac06c557-1707-421b-acfe-28a7ab1d74f6");
    private static final UUID EXECUTION_ID = UUID.fromString("cc258051-0eba-4ede-9311-eb59eb55609a");
    private static final Instant CREATED_AT = Instant.parse("2026-10-24T10:00:00Z");
    private static final Instant STARTED_AT = Instant.parse("2026-10-24T10:01:00Z");
    private static final Instant FINISHED_AT = Instant.parse("2026-10-24T10:02:00Z");

    @Mock
    private TaskRepository taskRepository;

    private TaskRetryService taskRetryService;

    @BeforeEach
    void setUp() {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("user1", "unused",
                AuthorityUtils.createAuthorityList("ROLE_USER"))
        );
        taskRetryService = new TaskRetryService(taskRepository, new TaskAccessService());
    }

    @AfterEach
    void clearSecurityContext() {
        SecurityContextHolder.clearContext();
    }

    @Test
    @DisplayName("A manual retry changes FAILED to NEW and resets the attempt budget")
    void retryFailedTaskResetsExecutionState() {
        Task task = createTask(TaskStatus.FAILED);
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        boolean retried = taskRetryService.retryFailedTask(TASK_ID);

        assertThat(retried).isTrue();
        assertThat(task.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(task.getExecutionId()).isNull();
        assertThat(task.getLeaseUntil()).isNull();
        assertThat(task.getNextRetryAt()).isNull();
        assertThat(task.getAttempts()).isZero();
        assertThat(task.getStartedAt()).isNull();
        assertThat(task.getFinishedAt()).isNull();
        assertThat(task.getProgress()).isZero();
        assertThat(task.getResult()).isNull();

        assertThat(task.getId()).isEqualTo(TASK_ID);
        assertThat(task.getEventId()).isEqualTo(EVENT_ID);
        assertThat(task.getName()).isEqualTo("My task");
        assertThat(task.getDuration()).isEqualTo(2500L);
        assertThat(task.getCreatedAt()).isEqualTo(CREATED_AT);
        assertThat(task.getMaxAttempts()).isEqualTo(3);
        assertThat(task.getVersion()).isEqualTo(5L);
        assertThat(task.getOwnerUsername()).isEqualTo("user1");
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"NEW", "IN_PROGRESS", "COMPLETED"})
    @DisplayName("A manual retry does not change a task that is not FAILED")
    void retryFailedTaskLeavesOtherStatesUnchanged(TaskStatus status) {
        Task task = createTask(status);
        Task original = createTask(status);
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        boolean retried = taskRetryService.retryFailedTask(TASK_ID);

        assertThat(retried).isFalse();
        assertThat(task).usingRecursiveComparison().isEqualTo(original);
    }

    @Test
    @DisplayName("A manual retry reports a lookup error when the task is missing")
    void retryFailedTaskRejectsMissingTask() {
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> taskRetryService.retryFailedTask(TASK_ID))
            .isInstanceOf(TaskNotFoundException.class)
            .hasMessage("Task with ID 7 was not found");
    }

    @Test
    @DisplayName("A foreign task is hidden and its execution state is not reset")
    void rejectsForeignTaskWithoutChangingIt() {
        Task task = createTask(TaskStatus.FAILED);
        task.setOwnerUsername("another-user");
        Task original = createTask(TaskStatus.FAILED);
        original.setOwnerUsername("another-user");
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        assertThatThrownBy(() -> taskRetryService.retryFailedTask(TASK_ID))
            .isInstanceOf(TaskNotFoundException.class)
            .hasMessage("Task with ID 7 was not found");

        assertThat(task).usingRecursiveComparison().isEqualTo(original);
    }

    @Test
    @DisplayName("A foreign non-failed task is hidden before its status is checked")
    void hidesForeignTaskBeforeCheckingStatus() {
        Task task = createTask(TaskStatus.NEW);
        task.setOwnerUsername("another-user");
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        assertThatThrownBy(() -> taskRetryService.retryFailedTask(TASK_ID))
            .isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    @DisplayName("A user cannot retry a legacy task without an owner")
    void rejectsLegacyTaskForUser() {
        Task task = createTask(TaskStatus.FAILED);
        task.setOwnerUsername(null);
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        assertThatThrownBy(() -> taskRetryService.retryFailedTask(TASK_ID))
            .isInstanceOf(TaskNotFoundException.class);

        assertThat(task.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.getAttempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("An administrator can retry a legacy failed task without taking ownership")
    void adminRetriesLegacyTask() {
        SecurityContextHolder.getContext().setAuthentication(
            UsernamePasswordAuthenticationToken.authenticated("admin", "unused",
                AuthorityUtils.createAuthorityList("ROLE_ADMIN"))
        );
        Task task = createTask(TaskStatus.FAILED);
        task.setOwnerUsername(null);
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        assertThat(taskRetryService.retryFailedTask(TASK_ID)).isTrue();

        assertThat(task.getStatus()).isEqualTo(TaskStatus.NEW);
        assertThat(task.getAttempts()).isZero();
        assertThat(task.getOwnerUsername()).isNull();
    }

    @Test
    @DisplayName("Missing authentication cannot reset a failed task")
    void rejectsUnauthenticatedRetry() {
        SecurityContextHolder.clearContext();
        Task task = createTask(TaskStatus.FAILED);
        when(taskRepository.findByIdForUpdate(TASK_ID)).thenReturn(Optional.of(task));

        assertThatThrownBy(() -> taskRetryService.retryFailedTask(TASK_ID))
            .isInstanceOf(TaskNotFoundException.class);

        assertThat(task.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(task.getAttempts()).isEqualTo(3);
    }

    private Task createTask(TaskStatus status) {
        Task task = new Task();
        task.setId(TASK_ID);
        task.setEventId(EVENT_ID);
        task.setOwnerUsername("user1");
        task.setName("My task");
        task.setDuration(2500L);
        task.setStatus(status);
        task.setProgress(75);
        task.setResult("Previous result");
        task.setExecutionId(EXECUTION_ID);
        task.setLeaseUntil(FINISHED_AT.plusSeconds(30));
        task.setNextRetryAt(FINISHED_AT.plusSeconds(60));
        task.setAttempts(3);
        task.setMaxAttempts(3);
        task.setCreatedAt(CREATED_AT);
        task.setStartedAt(STARTED_AT);
        task.setFinishedAt(FINISHED_AT);
        task.setVersion(5L);
        return task;
    }
}
