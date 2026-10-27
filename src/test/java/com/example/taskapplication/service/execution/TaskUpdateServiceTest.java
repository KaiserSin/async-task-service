package com.example.taskapplication.service.execution;

import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskUpdateServiceTest {

    private static final Long TASK_ID = 7L;
    private static final UUID EXECUTION_ID = UUID.fromString("cc258051-0eba-4ede-9311-eb59eb55609a");

    @Mock
    private TaskRepository taskRepository;

    private TaskUpdateService taskUpdateService;

    @BeforeEach
    void setUp() {
        taskUpdateService = new TaskUpdateService(taskRepository);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 99})
    @DisplayName("Accepts both intermediate progress boundaries and passes the execution owner")
    void updateProgressAcceptsBoundaries(int progress) {
        when(taskRepository.updateProgress(
            TASK_ID, EXECUTION_ID, TaskStatus.IN_PROGRESS.name(), progress
        )).thenReturn(1);

        boolean updated = taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, progress);

        assertThat(updated).isTrue();
        verify(taskRepository).updateProgress(
            TASK_ID, EXECUTION_ID, TaskStatus.IN_PROGRESS.name(), progress
        );
    }

    @Test
    @DisplayName("Returns false when the progress update affects no rows")
    void updateProgressReturnsFalseWhenNothingWasUpdated() {
        when(taskRepository.updateProgress(
            TASK_ID, EXECUTION_ID, TaskStatus.IN_PROGRESS.name(), 40
        )).thenReturn(0);

        assertThat(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 100})
    @DisplayName("Rejects invalid intermediate progress before accessing the database")
    void updateProgressRejectsInvalidProgress(int progress) {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, progress))
            .withMessage("Progress must be between 0 and 99 while a task is running");

        verifyNoInteractions(taskRepository);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"NEW", "IN_PROGRESS"})
    @NullSource
    @DisplayName("Accepts only COMPLETED and FAILED as final statuses")
    void updateTaskStatusRejectsNonFinalStatus(TaskStatus status) {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> taskUpdateService.updateTaskStatus(
                TASK_ID, EXECUTION_ID, status, 40, "Result"
            ))
            .withMessage("Task status must be COMPLETED or FAILED");

        verifyNoInteractions(taskRepository);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 101})
    @DisplayName("Rejects invalid final progress before accessing the database")
    void updateTaskStatusRejectsInvalidProgress(int progress) {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> taskUpdateService.updateTaskStatus(
                TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, progress, "Result"
            ))
            .withMessage("Progress must be between 0 and 100");

        verifyNoInteractions(taskRepository);
    }

    @ParameterizedTest
    @CsvSource({
        "COMPLETED, 0, 100",
        "COMPLETED, 99, 100",
        "COMPLETED, 100, 100",
        "FAILED, 0, 0",
        "FAILED, 42, 42",
        "FAILED, 99, 99",
        "FAILED, 100, 99"
    })
    @DisplayName("Normalizes final progress and preserves identifiers and the result")
    void updateTaskStatusNormalizesProgress(TaskStatus status, int progress, int expectedProgress) {
        String result = "Execution result";
        when(taskRepository.finishExecution(
            eq(TASK_ID), eq(EXECUTION_ID), eq(TaskStatus.IN_PROGRESS.name()), eq(status.name()),
            eq(expectedProgress), eq(result), any(Instant.class)
        )).thenReturn(1);

        boolean updated = taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, status, progress, result
        );

        ArgumentCaptor<Instant> finishedAt = ArgumentCaptor.forClass(Instant.class);
        assertThat(updated).isTrue();
        verify(taskRepository).finishExecution(
            eq(TASK_ID), eq(EXECUTION_ID), eq(TaskStatus.IN_PROGRESS.name()), eq(status.name()),
            eq(expectedProgress), eq(result), finishedAt.capture()
        );
        assertThat(finishedAt.getValue()).isNotNull();
    }

    @ParameterizedTest
    @CsvSource({"0, false", "1, true"})
    @DisplayName("Returns the completion outcome based on the number of updated rows")
    void updateTaskStatusReturnsRepositoryOutcome(int updatedRows, boolean expected) {
        when(taskRepository.finishExecution(
            eq(TASK_ID), eq(EXECUTION_ID), eq(TaskStatus.IN_PROGRESS.name()),
            eq(TaskStatus.COMPLETED.name()), eq(100), eq("Done"), any(Instant.class)
        )).thenReturn(updatedRows);

        assertThat(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 80, "Done"
        )).isEqualTo(expected);
    }
}
