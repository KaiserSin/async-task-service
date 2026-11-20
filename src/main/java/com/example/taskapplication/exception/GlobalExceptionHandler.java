package com.example.taskapplication.exception;

import com.example.taskapplication.dto.ApiError;
import com.example.taskapplication.service.database.DatabaseFailureClassifier;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
@RequiredArgsConstructor
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private final DatabaseFailureClassifier databaseFailureClassifier;

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<ApiError> handleTaskNotFound(TaskNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .cacheControl(CacheControl.noStore())
            .body(new ApiError("TASK_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(TaskRetryNotAllowedException.class)
    public ResponseEntity<ApiError> handleTaskRetryNotAllowed(TaskRetryNotAllowedException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .cacheControl(CacheControl.noStore())
            .body(new ApiError("TASK_NOT_RETRYABLE", ex.getMessage()));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiError> handleConstraintViolation(ConstraintViolationException ex) {
        var violations = ex.getConstraintViolations();
        String message = CollectionUtils.isEmpty(violations)
            ? "Invalid request arguments"
            : violations.stream()
                .map(ConstraintViolation::getMessage)
                .sorted()
                .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest()
            .body(new ApiError("INVALID_ARGUMENT", message));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> handleArgumentTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String message = switch (ex.getName()) {
            case "id" -> "Task ID must be a whole number greater than 0";
            case "eventId" -> "eventId must be a valid UUID";
            default -> "Invalid value for " + ex.getName();
        };

        return ResponseEntity.badRequest()
            .body(new ApiError("INVALID_ARGUMENT", message));
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
        MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request
    ) {
        String message = ex.getBindingResult().getFieldErrors().stream()
            .map(error -> error.getField() + ": " + error.getDefaultMessage())
            .collect(Collectors.joining("; "));
        return super.handleExceptionInternal(ex, new ApiError("VALIDATION_ERROR", message), headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
        Exception ex, Object body, HttpHeaders headers, HttpStatusCode status, WebRequest request
    ) {
        if (status.is5xxServerError()) {
            log.error("Could not handle HTTP request", ex);
        }
        String message = status.is4xxClientError() ? "Invalid request" : "Internal server error";
        ApiError error = new ApiError("HTTP_" + status.value(), message);
        return super.handleExceptionInternal(ex, error, headers, status, request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex) {
        if (databaseFailureClassifier.classify(ex) == DatabaseFailureClassifier.Action.RETRY) {
            return handleDatabaseUnavailable(ex);
        }

        log.error("Unexpected request error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ApiError("INTERNAL_ERROR", "Internal server error"));
    }

    private ResponseEntity<ApiError> handleDatabaseUnavailable(Exception ex) {
        log.error("Database is not available for this request", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ApiError("SERVICE_UNAVAILABLE", "Service is not available right now"));
    }
}
