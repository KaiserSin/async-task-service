package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskClaimServiceTest {

    private static final WorkerProperties TIMING = new WorkerProperties(47, 7, 5,
        new WorkerProperties.Pool(4), new WorkerProperties.Shutdown(75, 10),
        new WorkerProperties.Cleanup(30, 90, 500));

    private static final long TASK_ID = 1L;
    private static final UUID EVENT_ID = UUID.fromString("12345678-1234-4321-8765-123456789abc");
    private static final long DURATION_MS = 2500L;

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private TaskRepository.ClaimedTask claimedTask;

    @Mock
    private PlatformTransactionManager transactionManager;

    @Mock
    private TransactionStatus transactionStatus;

    private TaskClaimService taskClaimService;
    private Runnable beforeTransaction = () -> {};

    @BeforeEach
    void setUp() {
        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
            .thenAnswer(invocation -> {
                beforeTransaction.run();
                return transactionStatus;
            });
        taskClaimService = new TaskClaimService(
            taskRepository, new TransactionTemplate(transactionManager), TIMING
        );
    }

    @Test
    @DisplayName("Does not create an execution attempt when no tasks are ready")
    void returnsEmptyWhenNoTaskIsAvailable() {
        when(taskRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()))).thenReturn(Optional.empty());

        Optional<TaskRun> result = taskClaimService.claimNextTask();

        assertThat(result).isEmpty();
        verify(taskRepository).claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()));
    }

    @Test
    @DisplayName("Claim returns a snapshot with the saved execution ID")
    void returnsTaskRunWithClaimedExecutionId() {
        prepareClaimedTask();
        when(taskRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds())))
            .thenReturn(Optional.of(claimedTask));

        TaskRun result = taskClaimService.claimNextTask().orElseThrow();

        ArgumentCaptor<UUID> executionId = ArgumentCaptor.forClass(UUID.class);
        verify(taskRepository).claimNextExecution(executionId.capture(), eq(TIMING.leaseSeconds()));
        assertThat(result.id()).isEqualTo(TASK_ID);
        assertThat(result.eventId()).isEqualTo(EVENT_ID);
        assertThat(result.duration()).isEqualTo(DURATION_MS);
        assertThat(result.executionId()).isNotNull().isEqualTo(executionId.getValue());
    }

    @Test
    @DisplayName("Retries retain the operation event ID while each attempt gets a different execution ID")
    void preservesEventIdAcrossExecutionAttempts() {
        prepareClaimedTask();
        when(taskRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds())))
            .thenReturn(Optional.of(claimedTask));

        TaskRun first = taskClaimService.claimNextTask().orElseThrow();
        TaskRun retry = taskClaimService.claimNextTask().orElseThrow();

        assertThat(first.eventId()).isEqualTo(EVENT_ID);
        assertThat(retry.eventId()).isEqualTo(first.eventId());
        assertThat(retry.executionId()).isNotEqualTo(first.executionId());
    }

    @Test
    @DisplayName("The confirmed lease budget includes transaction startup, claim and commit time")
    void leaseDeadlineStartsBeforeClaimTransaction() {
        prepareClaimedTask();
        long beforeClaim = TimeUnit.SECONDS.toNanos(100);
        AtomicLong clock = new AtomicLong(beforeClaim);
        taskClaimService = spy(taskClaimService);
        doAnswer(invocation -> clock.get()).when(taskClaimService).nanoTime();
        beforeTransaction = () -> clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
        when(taskRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()))).thenAnswer(invocation -> {
            clock.addAndGet(TimeUnit.SECONDS.toNanos(3));
            return Optional.of(claimedTask);
        });
        doAnswer(invocation -> {
            clock.addAndGet(TimeUnit.SECONDS.toNanos(4));
            return null;
        }).when(transactionManager).commit(transactionStatus);

        TaskRun result = taskClaimService.claimNextTask().orElseThrow();

        assertThat(result.leaseDeadlineNanos()).isEqualTo(beforeClaim + TIMING.leaseNanos());
        assertThat(result.leaseDeadlineNanos() - clock.get())
            .isEqualTo(TIMING.leaseNanos() - TimeUnit.SECONDS.toNanos(9));
    }

    @Test
    @DisplayName("Propagates database errors without automatically retrying the claim")
    void propagatesDatabaseFailureWithoutRetry() {
        var failure = new DataAccessResourceFailureException("Database is not available");
        when(taskRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()))).thenThrow(failure);

        assertThatThrownBy(() -> taskClaimService.claimNextTask()).isSameAs(failure);

        verify(taskRepository).claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds()));
    }

    @Test
    @DisplayName("Does not return a task snapshot to the worker when commit fails")
    void doesNotReturnTaskRunWhenCommitFails() {
        prepareClaimedTask();
        var failure = new TransactionSystemException("Failed to commit the transaction");
        when(taskRepository.claimNextExecution(any(UUID.class), eq(TIMING.leaseSeconds())))
            .thenReturn(Optional.of(claimedTask));
        doThrow(failure).when(transactionManager).commit(transactionStatus);

        assertThatThrownBy(() -> taskClaimService.claimNextTask()).isSameAs(failure);
    }

    private void prepareClaimedTask() {
        when(claimedTask.getId()).thenReturn(TASK_ID);
        when(claimedTask.getEventId()).thenReturn(EVENT_ID);
        when(claimedTask.getDuration()).thenReturn(DURATION_MS);
    }
}
