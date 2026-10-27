package com.example.taskapplication.service.execution;

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

    public Optional<TaskRun> claimNextTask() {
        return claimTransactionTemplate.execute(transactionStatus ->
            taskRepository.findAndLockFirstNewTask()
                .map(task -> {
                    UUID executionId = UUID.randomUUID();
                    int updated = taskRepository.startExecution(task.getId(), executionId);
                    if (updated != 1) {
                        throw new IllegalStateException("Failed to claim task " + task.getId());
                    }

                    return new TaskRun(
                        task.getId(), executionId, task.getDuration()
                    );
                })
        );
    }
}
