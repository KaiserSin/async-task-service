package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.exception.TaskNotFoundException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TaskRetryService {

    private final TaskRepository taskRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean retryFailedTask(Long id) {
        Task task = taskRepository.findByIdForUpdate(id)
            .orElseThrow(() -> new TaskNotFoundException(id));

        if (task.getStatus() != TaskStatus.FAILED) {
            return false;
        }

        task.setStatus(TaskStatus.NEW);
        task.setExecutionId(null);
        task.setLeaseUntil(null);
        task.setNextRetryAt(null);
        task.setAttempts(0);
        task.setStartedAt(null);
        task.setFinishedAt(null);
        task.setProgress(0);
        task.setResult(null);
        return true;
    }
}
