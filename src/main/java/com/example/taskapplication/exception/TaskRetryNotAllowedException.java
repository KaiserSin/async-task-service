package com.example.taskapplication.exception;

public class TaskRetryNotAllowedException extends RuntimeException {

    public TaskRetryNotAllowedException(Long id) {
        super("Task with ID " + id + " can only be retried when its status is FAILED");
    }
}
