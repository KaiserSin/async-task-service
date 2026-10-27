package com.example.taskapplication.service.execution;

import com.example.taskapplication.dto.TaskRun;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class TaskExecutionService {

    private final TaskClaimService taskClaimService;

    public Optional<TaskRun> claimNextTask() {
        return taskClaimService.claimNextTask();
    }
}
