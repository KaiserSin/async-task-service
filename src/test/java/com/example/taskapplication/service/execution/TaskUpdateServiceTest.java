package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
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

    private static final WorkerProperties TIMING = new WorkerProperties(47, 7, 5,
        new WorkerProperties.Pool(4), new WorkerProperties.Shutdown(75, 10),
        new WorkerProperties.Cleanup(30, 90, 500));

    private static final Long TASK_ID = 7L;
    private static final UUID EXECUTION_ID = UUID.fromString("cc258051-0eba-4ede-9311-eb59eb55609a");

    @Mock
    private TaskRepository taskRepository;

    private TaskUpdateService taskUpdateService;

    @BeforeEach
    void setUp() {
        taskUpdateService = new TaskUpdateService(taskRepository, TIMING);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 99})
    @DisplayName("Accepts both intermediate progress boundaries and passes the execution owner")
    void updateProgressAcceptsBoundaries(int progress) {
        when(taskRepository.updateProgress(
            TASK_ID, EXECUTION_ID, progress, TIMING.leaseSeconds()
        )).thenReturn(1);

        boolean updated = taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, progress);

        assertThat(updated).isTrue();
        verify(taskRepository).updateProgress(
            TASK_ID, EXECUTION_ID, progress, TIMING.leaseSeconds()
        );
    }

    @Test
    @DisplayName("Returns false when the progress update affects no rows")
    void updateProgressReturnsFalseWhenNothingWasUpdated() {
        when(taskRepository.updateProgress(
            TASK_ID, EXECUTION_ID, 40, TIMING.leaseSeconds()
        )).thenReturn(0);

        assertThat(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"0, false", "1, true"})
    @DisplayName("Requeues only the interrupted execution owner and reports whether it was released")
    void requeuesInterruptedTask(int updatedRows, boolean expected) {
        when(taskRepository.requeueInterruptedExecution(TASK_ID, EXECUTION_ID)).thenReturn(updatedRows);

        assertThat(taskUpdateService.requeueInterruptedTask(TASK_ID, EXECUTION_ID)).isEqualTo(expected);

        verify(taskRepository).requeueInterruptedExecution(TASK_ID, EXECUTION_ID);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 101})
    @DisplayName("Rejects invalid failed-attempt progress before accessing the database")
    void failRejectsInvalidProgress(int progress) {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> taskUpdateService.fail(
                TASK_ID, EXECUTION_ID, progress, "Task failed"
            ));

        verifyNoInteractions(taskRepository);
    }

    @ParameterizedTest
    @CsvSource({
        "0, 0, NEW",
        "42, 42, NEW",
        "99, 99, FAILED",
        "100, 99, FAILED",
        "42, 42, "
    })
    @DisplayName("Returns the saved retry or terminal status, or no result for a stale attempt")
    void failPreservesOwnerAndNormalizesProgress(
        int progress, int expectedProgress, TaskStatus expectedStatus
    ) {
        when(taskRepository.failExecution(TASK_ID, EXECUTION_ID, expectedProgress, "Task failed",
            TIMING.retryBackoffSeconds()))
            .thenReturn(Optional.ofNullable(expectedStatus).map(TaskStatus::name));

        assertThat(taskUpdateService.fail(TASK_ID, EXECUTION_ID, progress, "Task failed"))
            .isEqualTo(Optional.ofNullable(expectedStatus));

        verify(taskRepository).failExecution(TASK_ID, EXECUTION_ID, expectedProgress, "Task failed",
            TIMING.retryBackoffSeconds());
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

    @Test
    @DisplayName("Completes the execution owner with its result and finish time")
    void completePreservesOwnerResultAndFinishTime() {
        String result = "Execution result";
        when(taskRepository.completeExecution(
            eq(TASK_ID), eq(EXECUTION_ID), eq(result), any(Instant.class)
        )).thenReturn(1);

        boolean updated = taskUpdateService.complete(TASK_ID, EXECUTION_ID, result);

        ArgumentCaptor<Instant> finishedAt = ArgumentCaptor.forClass(Instant.class);
        assertThat(updated).isTrue();
        verify(taskRepository).completeExecution(
            eq(TASK_ID), eq(EXECUTION_ID), eq(result), finishedAt.capture()
        );
        assertThat(finishedAt.getValue()).isNotNull();
    }

    @ParameterizedTest
    @CsvSource({"0, false", "1, true"})
    @DisplayName("Returns the completion outcome based on the number of updated rows")
    void completeReturnsRepositoryOutcome(int updatedRows, boolean expected) {
        when(taskRepository.completeExecution(
            eq(TASK_ID), eq(EXECUTION_ID), eq("Done"), any(Instant.class)
        )).thenReturn(updatedRows);

        assertThat(taskUpdateService.complete(TASK_ID, EXECUTION_ID, "Done")).isEqualTo(expected);
    }
}
