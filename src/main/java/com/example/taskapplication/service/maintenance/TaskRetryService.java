package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.exception.TaskNotFoundException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.service.security.TaskAccessService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class TaskRetryService {

    private final TaskRepository taskRepository;
    private final TaskAccessService taskAccessService;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean retryFailedTask(Long id) {
        Task task = taskRepository.findByIdForUpdate(id)
            .filter(taskAccessService::canAccess)
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
        log.info("Manual retry requested by {} for task {}",
            SecurityContextHolder.getContext().getAuthentication().getName(), id);
        return true;
    }
}
