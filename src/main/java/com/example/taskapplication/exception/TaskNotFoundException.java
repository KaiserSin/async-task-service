package com.example.taskapplication.exception;

import java.util.UUID;

public class TaskNotFoundException extends RuntimeException {

    public TaskNotFoundException(Long id) {
        super("Task with ID " + id + " was not found");
    }

    public TaskNotFoundException(UUID eventId) {
        super("Task with eventId " + eventId + " was not found");
    }
}
