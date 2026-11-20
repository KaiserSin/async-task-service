package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.config.WorkerProperties;
import com.example.taskapplication.repository.TaskRepository;
import com.example.taskapplication.repository.TaskRepository.RecoveryCounts;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TaskRecoveryService {

    private final TaskRepository taskRepository;
    private final WorkerProperties workerProperties;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public RecoveryCounts recoverExpiredTasks() {
        return taskRepository.recoverExpiredExecutions(workerProperties.retryBackoffSeconds());
    }
}
