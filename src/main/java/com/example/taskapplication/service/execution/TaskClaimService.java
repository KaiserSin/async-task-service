package com.example.taskapplication.service.execution;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskClaimService {

    private final TaskRepository taskRepository;
    private final TransactionTemplate claimTransactionTemplate;
    private final WorkerProperties workerProperties;

    public Optional<TaskRun> claimNextTask() {
        long claimStartedAt = nanoTime();
        return claimTransactionTemplate.execute(transactionStatus -> {
            UUID executionId = UUID.randomUUID();
            return taskRepository.claimNextExecution(executionId, workerProperties.leaseSeconds())
                .map(task -> new TaskRun(
                    task.getId(), task.getEventId(), executionId, task.getDuration(),
                    claimStartedAt + workerProperties.leaseNanos()
                ));
        });
    }

    long nanoTime() {
        return System.nanoTime();
    }
}
