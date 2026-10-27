package com.example.taskapplication.service.maintenance;

import com.example.taskapplication.model.TaskStatus;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
@RequiredArgsConstructor
public class TaskCleanupService {

    private final TaskRepository taskRepository;

    @Value("${worker.cleanup.retention-days:30}")
    private long retentionDays;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int deleteCompletedTasks() {
        if (retentionDays < 1) {
            throw new IllegalArgumentException(
                "worker.cleanup.retention-days must be greater than 0"
            );
        }

        Instant cutoff = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        return taskRepository.deleteFinishedBefore(TaskStatus.COMPLETED, cutoff);
    }
}
