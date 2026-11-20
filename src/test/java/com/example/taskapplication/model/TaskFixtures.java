package com.example.taskapplication.model;

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
        return task;
    }
}
