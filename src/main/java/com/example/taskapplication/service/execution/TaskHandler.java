package com.example.taskapplication.service.execution;

import com.example.taskapplication.dto.TaskRun;

/**
 * Performs one bounded work step; may be invoked repeatedly within an attempt and after retries.
 * Implementations must respect the stepMillis budget, honor interruption and make repeated effects safe using stable keys.
 * Elapsed time and step size are execution hints, not durable resume checkpoints.
 */
@FunctionalInterface
public interface TaskHandler {
    void handleStep(TaskRun run, long elapsedMillis, long stepMillis) throws InterruptedException;
}
