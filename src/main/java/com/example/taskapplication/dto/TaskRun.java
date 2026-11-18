package com.example.taskapplication.dto;

import java.util.UUID;

// The monotonic lease deadline is local to the process that claimed this run.
public record TaskRun(Long id, UUID eventId, UUID executionId, long duration, long leaseDeadlineNanos) {
}
