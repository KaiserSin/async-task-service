package com.example.taskapplication.service.execution;

import com.example.taskapplication.dto.TaskRun;
import org.springframework.stereotype.Component;

/** Demonstrates a work step until an application-specific handler is supplied. */
@Component
public class DemoTaskHandler implements TaskHandler {
    @Override
    public void handleStep(TaskRun run, long elapsedMillis, long stepMillis) throws InterruptedException {
        Thread.sleep(stepMillis);
    }
}
