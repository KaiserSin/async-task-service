package com.example.taskapplication.service;

import com.example.taskapplication.dto.TaskRun;
import com.example.taskapplication.service.execution.TaskClaimService;
import com.example.taskapplication.service.execution.TaskRunner;
import com.example.taskapplication.service.execution.WorkerLifecycle;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
@RequiredArgsConstructor
public class TaskPoller {

    private final WorkerLifecycle workerLifecycle;
    private final TaskClaimService taskClaimService;
    private final TaskRunner taskRunner;

    @Scheduled(fixedDelayString = "${worker.poll-delay-ms:1000}")
    public void processTask() {
        workerLifecycle.startWorker(this::runWorker);
    }

    private void runWorker() {
        try {
            while (!workerLifecycle.isStopping() && !Thread.currentThread().isInterrupted()) {
                Optional<TaskRun> run = taskClaimService.claimNextTask();
                if (run.isEmpty()) {
                    return;
                }
                processTask();
                taskRunner.executeTask(run.get());
            }
        } catch (RuntimeException e) {
            log.error("Worker loop failed", e);
        }
    }

}
