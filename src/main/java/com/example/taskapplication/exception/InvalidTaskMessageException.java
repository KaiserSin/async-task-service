package com.example.taskapplication.exception;

public class InvalidTaskMessageException extends RuntimeException {

    public InvalidTaskMessageException(String message) {
        super(message);
    }

    public InvalidTaskMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
