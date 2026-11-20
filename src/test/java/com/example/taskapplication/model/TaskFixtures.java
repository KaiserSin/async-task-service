package com.example.taskapplication.model;

import java.time.Instant;
import java.util.UUID;

import static org.springframework.test.util.ReflectionTestUtils.setField;

public final class TaskFixtures {

    private TaskFixtures() {
    }

    public static Task task(UUID eventId, String ownerUsername, String name, long duration) {
        Task task = new Task();
        setField(task, "eventId", eventId);
        setField(task, "ownerUsername", ownerUsername);
        setField(task, "name", name);
        setField(task, "duration", duration);
        setField(task, "status", TaskStatus.NEW);
        setField(task, "progress", 0);
        setField(task, "attempts", 0);
        setField(task, "maxAttempts", 3);
        setField(task, "createdAt", Instant.parse("2026-01-01T10:00:00Z"));
        return task;
    }
}
