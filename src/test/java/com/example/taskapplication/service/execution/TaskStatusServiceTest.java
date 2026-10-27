package com.example.taskapplication.service.execution;

import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.metrics.TaskMetrics;
import com.example.taskapplication.model.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskStatusServiceTest {

    private static final Long TASK_ID = 1L;
    private static final UUID EXECUTION_ID = UUID.fromString("456b3456-1234-4321-8765-123456789abc");

    @Mock
    private TaskUpdateService taskUpdateService;

    @Mock
    private TaskMetrics taskMetrics;

    private TaskStatusService taskStatusService;

    @BeforeEach
    void setUp() {
        taskStatusService = spy(new TaskStatusService(taskUpdateService, taskMetrics));
    }

    @AfterEach
    void recordsAttemptTime() {
        verify(taskMetrics).recordExecution(longThat(nanos -> nanos >= 0));
    }

    @Test
    @DisplayName("A 2500 ms task saves progress at 40 and 80 before completing at 100")
    void executesStepsAndSavesProgressBeforeCompletion() throws InterruptedException {
        TaskRun run = taskRun(2500);
        doNothing().when(taskStatusService).performStep(anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(true);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 80)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        InOrder order = inOrder(taskStatusService, taskUpdateService);
        order.verify(taskStatusService).performStep(1000L);
        order.verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        order.verify(taskStatusService).performStep(1000L);
        order.verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 80);
        order.verify(taskStatusService).performStep(500L);
        order.verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Intermediate progress stays below 100 until the result is saved")
    void limitsIntermediateProgressTo99() throws InterruptedException {
        TaskRun run = taskRun(1001);
        doNothing().when(taskStatusService).performStep(anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 99)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        verify(taskStatusService).performStep(1000L);
        verify(taskStatusService).performStep(1L);
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 99);
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A short task saves only its final result")
    void completesShortTaskWithoutIntermediateProgress() throws InterruptedException {
        TaskRun run = taskRun(500);
        doNothing().when(taskStatusService).performStep(500L);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        verify(taskStatusService).performStep(500L);
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Losing ownership stops the attempt without another step or final update")
    void stopsWhenProgressUpdateIsRejected() throws InterruptedException {
        TaskRun run = taskRun(2500);
        doNothing().when(taskStatusService).performStep(anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(false);

        taskStatusService.executeTask(run);

        verify(taskStatusService, times(1)).performStep(anyLong());
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A database access error while saving progress does not set FAILED")
    void doesNotMarkFailedOnProgressDatabaseFailure() throws InterruptedException {
        TaskRun run = taskRun(2500);
        doNothing().when(taskStatusService).performStep(anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40))
            .thenThrow(new DataAccessResourceFailureException("Database is not available"));

        assertThatCode(() -> taskStatusService.executeTask(run)).doesNotThrowAnyException();

        verify(taskStatusService, times(1)).performStep(anyLong());
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A transaction creation error while saving progress does not set FAILED")
    void doesNotMarkFailedOnProgressTransactionFailure() throws InterruptedException {
        TaskRun run = taskRun(2500);
        doNothing().when(taskStatusService).performStep(anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40))
            .thenThrow(new CannotCreateTransactionException("Failed to start the transaction"));

        assertThatCode(() -> taskStatusService.executeTask(run)).doesNotThrowAnyException();

        verify(taskStatusService, times(1)).performStep(anyLong());
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A step error saves FAILED with the last reached progress")
    void marksFailedOnStepFailure() throws InterruptedException {
        TaskRun run = taskRun(2500);
        doNothing().doThrow(new IllegalStateException("Step failed"))
            .when(taskStatusService).performStep(anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.FAILED, 40, "Task failed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        verify(taskStatusService, times(2)).performStep(1000L);
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.FAILED, 40, "Task failed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("An error saving COMPLETED does not trigger a FAILED update")
    void doesNotMarkFailedWhenCompletionCannotBeSaved() throws InterruptedException {
        TaskRun run = taskRun(500);
        doNothing().when(taskStatusService).performStep(500L);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenThrow(new DataAccessResourceFailureException("Failed to save the result"));

        assertThatCode(() -> taskStatusService.executeTask(run)).doesNotThrowAnyException();

        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verify(taskMetrics).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A rejected final result is not saved again with a different status")
    void doesNotRetryRejectedCompletion() throws InterruptedException {
        TaskRun run = taskRun(500);
        doNothing().when(taskStatusService).performStep(500L);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(false);

        taskStatusService.executeTask(run);

        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Interrupting a real sleep saves FAILED and restores the interrupt flag")
    void restoresInterruptFlagAfterInterruptedSleep() throws InterruptedException {
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.FAILED, 0, "Task was interrupted"
        )).thenReturn(true);

        boolean interrupted = executeInInterruptedThread(taskRun(2500));

        assertThat(interrupted).isTrue();
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.FAILED, 0, "Task was interrupted"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Restores the interrupt flag even when saving FAILED fails")
    void restoresInterruptFlagWhenInterruptedResultCannotBeSaved() throws InterruptedException {
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.FAILED, 0, "Task was interrupted"
        )).thenThrow(new DataAccessResourceFailureException("Failed to save the result"));

        boolean interrupted = executeInInterruptedThread(taskRun(2500));

        assertThat(interrupted).isTrue();
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.FAILED, 0, "Task was interrupted"
        );
        verify(taskMetrics).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
    }

    private TaskRun taskRun(long duration) {
        return new TaskRun(TASK_ID, EXECUTION_ID, duration);
    }

    private boolean executeInInterruptedThread(TaskRun run) throws InterruptedException {
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                Thread.currentThread().interrupt();
                taskStatusService.executeTask(run);
                interruptRestored.set(Thread.currentThread().isInterrupted());
            } catch (Throwable ex) {
                failure.set(ex);
            }
        }, "task-status-interruption-test");
        worker.setDaemon(true);
        worker.start();

        try {
            worker.join(5000);
            assertThat(worker.isAlive()).as("The worker thread must finish within 5 seconds").isFalse();
            assertThat(failure.get()).as("The worker thread must not hide exceptions").isNull();
            return interruptRestored.get();
        } finally {
            if (worker.isAlive()) {
                worker.interrupt();
                worker.join(1000);
            }
        }
    }
}
