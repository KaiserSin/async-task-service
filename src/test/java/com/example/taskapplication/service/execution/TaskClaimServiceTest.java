package com.example.taskapplication.service.execution;

import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.model.Task;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskClaimServiceTest {

    @Mock
    private TaskRepository taskRepository;

    @Mock
    private PlatformTransactionManager transactionManager;

    @Mock
    private TransactionStatus transactionStatus;

    private TaskClaimService taskClaimService;

    @BeforeEach
    void setUp() {
        when(transactionManager.getTransaction(any(TransactionDefinition.class)))
            .thenReturn(transactionStatus);
        taskClaimService = new TaskClaimService(
            taskRepository, new TransactionTemplate(transactionManager)
        );
    }

    @Test
    @DisplayName("Does not create an execution attempt when no tasks are ready")
    void returnsEmptyWhenNoTaskIsAvailable() {
        when(taskRepository.findAndLockFirstNewTask()).thenReturn(Optional.empty());

        Optional<TaskRun> result = taskClaimService.claimNextTask();

        assertThat(result).isEmpty();
        verify(taskRepository, never()).startExecution(anyLong(), any(UUID.class));
    }

    @Test
    @DisplayName("Claim returns a snapshot with the saved execution ID")
    void returnsTaskRunWithClaimedExecutionId() {
        Task task = createTask();
        when(taskRepository.findAndLockFirstNewTask()).thenReturn(Optional.of(task));
        when(taskRepository.startExecution(eq(task.getId()), any(UUID.class))).thenReturn(1);

        TaskRun result = taskClaimService.claimNextTask().orElseThrow();

        ArgumentCaptor<UUID> executionId = ArgumentCaptor.forClass(UUID.class);
        verify(taskRepository).startExecution(eq(task.getId()), executionId.capture());
        assertThat(result.id()).isEqualTo(task.getId());
        assertThat(result.duration()).isEqualTo(task.getDuration().longValue());
        assertThat(result.executionId()).isNotNull().isEqualTo(executionId.getValue());
    }

    @Test
    @DisplayName("Does not return a task or retry when the claim updates no rows")
    void rejectsTaskWhenExecutionWasNotStarted() {
        Task task = createTask();
        when(taskRepository.findAndLockFirstNewTask()).thenReturn(Optional.of(task));
        when(taskRepository.startExecution(eq(task.getId()), any(UUID.class))).thenReturn(0);

        assertThatThrownBy(() -> taskClaimService.claimNextTask())
            .isInstanceOf(IllegalStateException.class);

        verify(taskRepository).findAndLockFirstNewTask();
        verify(taskRepository).startExecution(eq(task.getId()), any(UUID.class));
    }

    @Test
    @DisplayName("Propagates database errors without automatically retrying the claim")
    void propagatesDatabaseFailureWithoutRetry() {
        var failure = new DataAccessResourceFailureException("Database is not available");
        when(taskRepository.findAndLockFirstNewTask()).thenThrow(failure);

        assertThatThrownBy(() -> taskClaimService.claimNextTask()).isSameAs(failure);

        verify(taskRepository).findAndLockFirstNewTask();
        verify(taskRepository, never()).startExecution(anyLong(), any(UUID.class));
    }

    @Test
    @DisplayName("Does not return a task snapshot to the worker when commit fails")
    void doesNotReturnTaskRunWhenCommitFails() {
        Task task = createTask();
        var failure = new TransactionSystemException("Failed to commit the transaction");
        when(taskRepository.findAndLockFirstNewTask()).thenReturn(Optional.of(task));
        when(taskRepository.startExecution(eq(task.getId()), any(UUID.class))).thenReturn(1);
        doThrow(failure).when(transactionManager).commit(transactionStatus);

        assertThatThrownBy(() -> taskClaimService.claimNextTask()).isSameAs(failure);
    }

    private Task createTask() {
        Task task = new Task();
        task.setId(1L);
        task.setDuration(2500L);
        return task;
    }
}
