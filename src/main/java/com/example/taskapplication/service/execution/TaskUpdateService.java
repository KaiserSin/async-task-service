package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerTimingProperties;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class TaskUpdateService {

    private final TaskRepository taskRepository;
    private final WorkerTimingProperties workerTiming;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean updateProgress(Long id, UUID executionId, int progress) {
        if (progress < 0 || progress > 99) {
            throw new IllegalArgumentException("Progress must be between 0 and 99 while a task is running");
        }
        return taskRepository.updateProgress(
            id, executionId, TaskStatus.IN_PROGRESS.name(), progress, workerTiming.leaseSeconds()
        ) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean requeueInterruptedTask(Long id, UUID executionId) {
        return taskRepository.requeueInterruptedExecution(id, executionId) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<TaskStatus> failTaskAttempt(Long id, UUID executionId, int progress, String result) {
        if (progress < 0 || progress > 100) {
            throw new IllegalArgumentException("Progress must be between 0 and 100");
        }
        return taskRepository.failExecution(
            id, executionId, Math.min(progress, 99), result, workerTiming.retryBackoffSeconds()
        ).map(TaskStatus::valueOf);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean updateTaskStatus(
        Long id,
        UUID executionId,
        TaskStatus newStatus,
        int progress,
        String result
    ) {
        if (newStatus != TaskStatus.COMPLETED && newStatus != TaskStatus.FAILED) {
            throw new IllegalArgumentException("Task status must be COMPLETED or FAILED");
        }
        if (progress < 0 || progress > 100) {
            throw new IllegalArgumentException("Progress must be between 0 and 100");
        }

        int finalProgress = newStatus == TaskStatus.COMPLETED ? 100 : Math.min(progress, 99);
        return taskRepository.finishExecution(
            id, executionId, TaskStatus.IN_PROGRESS.name(), newStatus.name(),
            finalProgress, result, Instant.now()
        ) == 1;
    }
}
