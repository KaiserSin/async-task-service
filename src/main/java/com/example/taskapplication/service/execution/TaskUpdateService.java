package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskUpdateService {

    private final TaskRepository taskRepository;
    private final WorkerProperties workerProperties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean updateProgress(Long id, UUID executionId, int progress) {
        if (progress < 0 || progress > 99) {
            throw new IllegalArgumentException("Progress must be between 0 and 99 while a task is running");
        }
        return taskRepository.updateProgress(
            id, executionId, progress, workerProperties.leaseSeconds()
        ) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean requeueInterruptedTask(Long id, UUID executionId) {
        return taskRepository.requeueInterruptedExecution(id, executionId) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<TaskStatus> fail(Long id, UUID executionId, int progress, String result) {
        if (progress < 0 || progress > 100) {
            throw new IllegalArgumentException("Progress must be between 0 and 100");
        }
        return taskRepository.failExecution(
            id, executionId, Math.min(progress, 99), result, workerProperties.retryBackoffSeconds()
        ).map(TaskStatus::valueOf);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean complete(Long id, UUID executionId, String result) {
        return taskRepository.completeExecution(id, executionId, result, Instant.now()) == 1;
    }
}
