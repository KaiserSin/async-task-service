package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerTimingProperties;
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
import org.springframework.transaction.TransactionSystemException;

import java.util.UUID;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskStatusServiceTest {

    private static final WorkerTimingProperties TIMING = new WorkerTimingProperties(30, 30);

    private static final Long TASK_ID = 1L;
    private static final UUID EVENT_ID = UUID.fromString("12345678-1234-4321-8765-123456789abc");
    private static final UUID EXECUTION_ID = UUID.fromString("456b3456-1234-4321-8765-123456789abc");

    @Mock
    private TaskUpdateService taskUpdateService;

    @Mock
    private TaskMetrics taskMetrics;

    @Mock
    private TaskHandler taskHandler;

    private TaskStatusService taskStatusService;

    @BeforeEach
    void setUp() {
        taskStatusService = spy(new TaskStatusService(taskUpdateService, taskMetrics, taskHandler, TIMING));
    }

    @AfterEach
    void recordsAttemptTime() {
        verify(taskMetrics).recordExecution(longThat(nanos -> nanos >= 0));
    }

    @Test
    @DisplayName("A 2500 ms task saves progress at 40 and 80 before completing at 100")
    void executesStepsAndSavesProgressBeforeCompletion() throws InterruptedException {
        TaskRun run = taskRun(2500);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(true);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 80)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        InOrder order = inOrder(taskHandler, taskUpdateService, taskMetrics);
        order.verify(taskHandler).handleStep(same(run), eq(0L), eq(1000L));
        order.verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        order.verify(taskHandler).handleStep(same(run), eq(1000L), eq(1000L));
        order.verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 80);
        order.verify(taskHandler).handleStep(same(run), eq(2000L), eq(500L));
        order.verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        order.verify(taskMetrics).recordCompleted();
        verify(taskMetrics, never()).recordFailed(anyLong());
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Intermediate progress stays below 100 until the result is saved")
    void limitsIntermediateProgressTo99() throws InterruptedException {
        TaskRun run = taskRun(1001);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 99)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        verify(taskHandler).handleStep(same(run), eq(0L), eq(1000L));
        verify(taskHandler).handleStep(same(run), eq(1000L), eq(1L));
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
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(run);

        verify(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(500L));
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Losing ownership stops the attempt without another step or final update")
    void stopsWhenProgressUpdateIsRejected() throws InterruptedException {
        TaskRun run = taskRun(2500);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(false);

        taskStatusService.executeTask(run);

        verify(taskHandler, times(1)).handleStep(any(TaskRun.class), anyLong(), anyLong());
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A temporary heartbeat database error does not discard a live attempt")
    void continuesAfterProgressDatabaseFailure() throws InterruptedException {
        TaskRun run = taskRun(2500);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40))
            .thenThrow(new DataAccessResourceFailureException("Database is not available"));
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 80)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        assertThatCode(() -> taskStatusService.executeTask(run)).doesNotThrowAnyException();

        verify(taskHandler, times(3)).handleStep(any(TaskRun.class), anyLong(), anyLong());
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 80);
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A temporary heartbeat transaction error does not discard a live attempt")
    void continuesAfterProgressTransactionFailure() throws InterruptedException {
        TaskRun run = taskRun(2500);
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40))
            .thenThrow(new CannotCreateTransactionException("Failed to start the transaction"));
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 80)).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        assertThatCode(() -> taskStatusService.executeTask(run)).doesNotThrowAnyException();

        verify(taskHandler, times(3)).handleStep(any(TaskRun.class), anyLong(), anyLong());
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 80);
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Repeated failed heartbeats stop work when the confirmed lease expires")
    void stopsAtLeaseDeadlineAfterRepeatedHeartbeatFailures() throws InterruptedException {
        AtomicLong clock = useClock();
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(invocation.getArgument(2, Long.class)));
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt()))
            .thenThrow(new DataAccessResourceFailureException("Database is unavailable"));

        taskStatusService.executeTask(taskRun(60_000));

        verify(taskHandler, times(30)).handleStep(any(TaskRun.class), anyLong(), eq(1000L));
        verify(taskUpdateService, times(29)).updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt());
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A successful heartbeat renews the deadline before later heartbeats fail")
    void successfulHeartbeatExtendsTheConfirmedLease() throws InterruptedException {
        AtomicLong clock = useClock();
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(invocation.getArgument(2, Long.class)));
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt()))
            .thenAnswer(invocation -> {
                if (clock.get() == TimeUnit.SECONDS.toNanos(21)) {
                    return true;
                }
                throw new DataAccessResourceFailureException("Temporary heartbeat failure");
            });

        taskStatusService.executeTask(taskRun(60_000));

        verify(taskHandler, times(50)).handleStep(any(TaskRun.class), anyLong(), eq(1000L));
        verify(taskUpdateService, times(49)).updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt());
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A heartbeat renews the lease for the configured five seconds instead of the default")
    void renewsHeartbeatWithConfiguredLease() throws InterruptedException {
        WorkerTimingProperties timing = new WorkerTimingProperties(5, 7);
        taskStatusService = spy(new TaskStatusService(taskUpdateService, taskMetrics, taskHandler, timing));
        AtomicLong clock = useClock();
        TaskRun run = new TaskRun(TASK_ID, EVENT_ID, EXECUTION_ID, 60_000,
            clock.get() + timing.leaseNanos());
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(invocation.getArgument(2, Long.class)));
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt()))
            .thenAnswer(invocation -> {
                if (clock.get() == TimeUnit.SECONDS.toNanos(3)) {
                    return true;
                }
                throw new DataAccessResourceFailureException("Temporary heartbeat failure");
            });

        taskStatusService.executeTask(run);

        assertThat(clock.get()).isEqualTo(TimeUnit.SECONDS.toNanos(8));
        verify(taskHandler, times(7)).handleStep(same(run), anyLong(), eq(1000L));
        verify(taskUpdateService, times(6)).updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt());
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Successful heartbeats keep a minute-long task alive beyond its original lease")
    void completesLongTaskWithRenewedLease() throws InterruptedException {
        AtomicLong clock = useClock();
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(invocation.getArgument(2, Long.class)));
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt())).thenReturn(true);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(true);

        taskStatusService.executeTask(taskRun(60_000));

        verify(taskHandler, times(60)).handleStep(any(TaskRun.class), anyLong(), eq(1000L));
        verify(taskUpdateService, times(59)).updateProgress(eq(TASK_ID), eq(EXECUTION_ID), anyInt());
        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("Time spent in a failed heartbeat consumes the remaining lease")
    void stopsAfterSlowFailedHeartbeatConsumesLease() throws InterruptedException {
        AtomicLong clock = useClock();
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenAnswer(invocation -> {
            clock.addAndGet(TIMING.leaseNanos());
            throw new CannotCreateTransactionException("Heartbeat timed out");
        });

        taskStatusService.executeTask(taskRun(2500));

        verify(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(1000L));
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A successful heartbeat starts its lease budget before the database call")
    void slowSuccessfulHeartbeatDoesNotStartFreshLeaseAfterReturning() throws InterruptedException {
        AtomicLong clock = useClock();
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenAnswer(invocation -> {
            clock.addAndGet(TIMING.leaseNanos());
            return true;
        });

        taskStatusService.executeTask(taskRun(2500));

        verify(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(1000L));
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A run whose lease has already expired never starts work")
    void doesNotStartExpiredRun() throws InterruptedException {
        AtomicLong clock = useClock();

        taskStatusService.executeTask(new TaskRun(TASK_ID, EVENT_ID, EXECUTION_ID, 500, clock.get()));

        verify(taskHandler, never()).handleStep(any(TaskRun.class), anyLong(), anyLong());
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A task that outlives its lease during its last step does not save a result")
    void doesNotCompleteAfterLeaseExpiresDuringFinalStep() throws InterruptedException {
        AtomicLong clock = useClock();
        doAnswer(invocation -> {
            clock.addAndGet(TIMING.leaseNanos());
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(500L));

        taskStatusService.executeTask(taskRun(500));

        verify(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(500L));
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A step is limited to the remaining confirmed lease after heartbeat failure")
    void limitsWorkStepToRemainingLease() throws InterruptedException {
        AtomicLong clock = useClock();
        TaskRun run = new TaskRun(TASK_ID, EVENT_ID, EXECUTION_ID, 2500,
            clock.get() + TimeUnit.MILLISECONDS.toNanos(1200));
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(invocation.getArgument(2, Long.class)));
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40))
            .thenThrow(new DataAccessResourceFailureException("Temporary heartbeat failure"));

        taskStatusService.executeTask(run);

        verify(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(1000L));
        verify(taskHandler).handleStep(same(run), eq(1000L), eq(200L));
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    private AtomicLong useClock() {
        AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
        doAnswer(invocation -> clock.get()).when(taskStatusService).nanoTime();
        return clock;
    }

    @Test
    @DisplayName("A step error records the failed attempt with the last reached progress")
    void recordsAttemptFailureOnStepFailure() throws InterruptedException {
        TaskRun run = taskRun(2500);
        doNothing().doThrow(new IllegalStateException("Step failed"))
            .when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(true);
        when(taskUpdateService.failTaskAttempt(
            TASK_ID, EXECUTION_ID, 40, "Task failed"
        )).thenReturn(Optional.of(TaskStatus.NEW));

        taskStatusService.executeTask(run);

        verify(taskHandler, times(2)).handleStep(any(TaskRun.class), anyLong(), eq(1000L));
        verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        verify(taskUpdateService).failTaskAttempt(
            TASK_ID, EXECUTION_ID, 40, "Task failed"
        );
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A database error from the handler follows the normal failed-attempt retry policy")
    void recordsAttemptFailureOnHandlerDatabaseError() throws InterruptedException {
        TaskRun run = taskRun(500);
        doThrow(new DataAccessResourceFailureException("Handler database is unavailable"))
            .when(taskHandler).handleStep(same(run), eq(0L), eq(500L));
        when(taskUpdateService.failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed"))
            .thenReturn(Optional.of(TaskStatus.NEW));

        taskStatusService.executeTask(run);

        verify(taskHandler).handleStep(same(run), eq(0L), eq(500L));
        verify(taskUpdateService).failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed");
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskHandler, taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("An error saving the failed attempt does not trigger a final FAILED update")
    void doesNotMarkFailedWhenAttemptFailureCannotBeSaved() throws InterruptedException {
        doThrow(new IllegalStateException("Step failed"))
            .when(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(500L));
        when(taskUpdateService.failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed"))
            .thenThrow(new DataAccessResourceFailureException("Failed to save the attempt"));

        assertThatCode(() -> taskStatusService.executeTask(taskRun(500))).doesNotThrowAnyException();

        verify(taskUpdateService).failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed");
        verify(taskMetrics).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A rejected failed-attempt update is not saved again with a final status")
    void doesNotRetryRejectedAttemptFailure() throws InterruptedException {
        doThrow(new IllegalStateException("Step failed"))
            .when(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(500L));
        when(taskUpdateService.failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed"))
            .thenReturn(Optional.empty());

        taskStatusService.executeTask(taskRun(500));

        verify(taskUpdateService).failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed");
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("An error saving COMPLETED does not trigger a FAILED update")
    void doesNotMarkFailedWhenCompletionCannotBeSaved() throws InterruptedException {
        TaskRun run = taskRun(500);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenThrow(new DataAccessResourceFailureException("Failed to save the result"));

        assertThatCode(() -> taskStatusService.executeTask(run)).doesNotThrowAnyException();

        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verify(taskMetrics).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A rejected final result is not saved again with a different status")
    void doesNotRetryRejectedCompletion() throws InterruptedException {
        TaskRun run = taskRun(500);
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenReturn(false);

        taskStatusService.executeTask(run);

        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("Interrupting a real sleep requeues the task and restores the interrupt flag")
    void restoresInterruptFlagAfterInterruptedSleep() throws InterruptedException {
        when(taskUpdateService.requeueInterruptedTask(TASK_ID, EXECUTION_ID)).thenReturn(true);

        boolean interrupted = executeInInterruptedThread(taskRun(2500));

        assertThat(interrupted).isTrue();
        verify(taskUpdateService).requeueInterruptedTask(TASK_ID, EXECUTION_ID);
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("Restores the interrupt flag even when requeuing the interrupted task fails")
    void restoresInterruptFlagWhenInterruptedResultCannotBeSaved() throws InterruptedException {
        when(taskUpdateService.requeueInterruptedTask(TASK_ID, EXECUTION_ID))
            .thenThrow(new DataAccessResourceFailureException("Failed to requeue the task"));

        boolean interrupted = executeInInterruptedThread(taskRun(2500));

        assertThat(interrupted).isTrue();
        verify(taskUpdateService).requeueInterruptedTask(TASK_ID, EXECUTION_ID);
        verify(taskMetrics).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("An interruption after partial progress requeues without saving a final status")
    void requeuesInterruptedTaskAfterProgress() throws InterruptedException {
        doNothing().doThrow(new InterruptedException("Worker shutdown"))
            .when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        when(taskUpdateService.updateProgress(TASK_ID, EXECUTION_ID, 40)).thenReturn(true);
        when(taskUpdateService.requeueInterruptedTask(TASK_ID, EXECUTION_ID)).thenReturn(true);

        assertThat(executeInWorkerThread(taskRun(2500), false)).isTrue();

        var order = inOrder(taskUpdateService);
        order.verify(taskUpdateService).updateProgress(TASK_ID, EXECUTION_ID, 40);
        order.verify(taskUpdateService).requeueInterruptedTask(TASK_ID, EXECUTION_ID);
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("An interruption from a stale execution does not write another status")
    void ignoresRejectedRequeue() throws InterruptedException {
        when(taskUpdateService.requeueInterruptedTask(TASK_ID, EXECUTION_ID)).thenReturn(false);

        assertThat(executeInInterruptedThread(taskRun(2500))).isTrue();

        verify(taskUpdateService).requeueInterruptedTask(TASK_ID, EXECUTION_ID);
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
        assertNoTerminalResultRecorded();
    }

    @Test
    @DisplayName("A committed terminal attempt failure increments FAILED once")
    void countsTerminalFailureAfterSavingIt() throws InterruptedException {
        TaskRun run = taskRun(500);
        doThrow(new IllegalStateException("Step failed"))
            .when(taskHandler).handleStep(same(run), eq(0L), eq(500L));
        when(taskUpdateService.failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed"))
            .thenReturn(Optional.of(TaskStatus.FAILED));

        taskStatusService.executeTask(run);

        InOrder order = inOrder(taskUpdateService, taskMetrics);
        order.verify(taskUpdateService).failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed");
        order.verify(taskMetrics).recordFailed(1);
        verify(taskMetrics, never()).recordCompleted();
        verify(taskMetrics, never()).recordResultSaveError();
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A failed completion commit does not increment either terminal counter")
    void doesNotCountCompletionWhenCommitFails() {
        when(taskUpdateService.updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        )).thenThrow(new TransactionSystemException("Commit failed"));

        assertThatCode(() -> taskStatusService.executeTask(taskRun(500))).doesNotThrowAnyException();

        verify(taskUpdateService).updateTaskStatus(
            TASK_ID, EXECUTION_ID, TaskStatus.COMPLETED, 100, "Task completed"
        );
        verify(taskMetrics).recordResultSaveError();
        assertNoTerminalResultRecorded();
        verifyNoMoreInteractions(taskUpdateService);
    }

    @Test
    @DisplayName("A failed failed-attempt commit does not increment either terminal counter")
    void doesNotCountFailureWhenCommitFails() throws InterruptedException {
        doThrow(new IllegalStateException("Step failed"))
            .when(taskHandler).handleStep(any(TaskRun.class), eq(0L), eq(500L));
        when(taskUpdateService.failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed"))
            .thenThrow(new TransactionSystemException("Commit failed"));

        assertThatCode(() -> taskStatusService.executeTask(taskRun(500))).doesNotThrowAnyException();

        verify(taskUpdateService).failTaskAttempt(TASK_ID, EXECUTION_ID, 0, "Task failed");
        verify(taskMetrics).recordResultSaveError();
        assertNoTerminalResultRecorded();
        verifyNoMoreInteractions(taskUpdateService);
    }

    private void assertNoTerminalResultRecorded() {
        verify(taskMetrics, never()).recordCompleted();
        verify(taskMetrics, never()).recordFailed(anyLong());
    }

    private TaskRun taskRun(long duration) {
        return new TaskRun(TASK_ID, EVENT_ID, EXECUTION_ID, duration, taskStatusService.nanoTime() + TIMING.leaseNanos());
    }

    private boolean executeInInterruptedThread(TaskRun run) throws InterruptedException {
        doAnswer(invocation -> {
            new DemoTaskHandler().handleStep(invocation.getArgument(0), invocation.getArgument(1),
                invocation.getArgument(2));
            return null;
        }).when(taskHandler).handleStep(any(TaskRun.class), anyLong(), anyLong());
        return executeInWorkerThread(run, true);
    }

    private boolean executeInWorkerThread(TaskRun run, boolean interruptBeforeStart) throws InterruptedException {
        AtomicBoolean interruptRestored = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                if (interruptBeforeStart) {
                    Thread.currentThread().interrupt();
                }
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
