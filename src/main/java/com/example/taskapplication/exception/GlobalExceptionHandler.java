package com.example.taskapplication.exception;

import com.example.taskapplication.dto.ErrorResponse;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.sql.SQLException;
import java.util.UUID;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleTaskNotFound(TaskNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .cacheControl(CacheControl.noStore())
            .body(new ErrorResponse("TASK_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler({
        ConstraintViolationException.class,
        MethodArgumentTypeMismatchException.class
    })
    public ResponseEntity<ErrorResponse> handleInvalidId(Exception ex) {
        String message = ex instanceof MethodArgumentTypeMismatchException mismatch
            && mismatch.getRequiredType() == UUID.class
                ? "eventId must be a valid UUID"
                : "Task ID must be a whole number greater than 0";

        return ResponseEntity.badRequest()
            .body(new ErrorResponse("INVALID_ARGUMENT", message));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleInvalidBody(
        MethodArgumentNotValidException ex
    ) {
        String message = ex.getBindingResult().getFieldErrors().stream()
            .map(error -> error.getField() + ": " + error.getDefaultMessage())
            .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest()
            .body(new ErrorResponse("VALIDATION_ERROR", message));
    }

    @ExceptionHandler({
        DataAccessResourceFailureException.class,
        CannotCreateTransactionException.class
    })
    public ResponseEntity<ErrorResponse> handleDatabaseUnavailable(Exception ex) {
        log.error("Database is not available for this request", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
            .body(new ErrorResponse("SERVICE_UNAVAILABLE", "Service is not available right now"));
    }

    @ExceptionHandler({JpaSystemException.class, TransactionSystemException.class})
    public ResponseEntity<ErrorResponse> handleTransactionError(Exception ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof SQLException sql
                && sql.getSQLState() != null
                && sql.getSQLState().startsWith("08")) {
                return handleDatabaseUnavailable(ex);
            }
            if (cause instanceof org.hibernate.TransactionException transaction
                && transaction.getCause() instanceof SQLException sql
                && sql.getSQLState() == null
                && "Connection is closed".equalsIgnoreCase(sql.getMessage())) {
                return handleDatabaseUnavailable(ex);
            }
            cause = cause.getCause();
        }
        return handleUnexpected(ex);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {

        if (ex instanceof org.springframework.web.ErrorResponse httpError) {
            HttpStatusCode status = httpError.getStatusCode();
            String message = status.is4xxClientError()
                ? "Invalid request" : "Internal server error";
            if (status.is5xxServerError()) {
                log.error("Could not handle HTTP request", ex);
            }
            return ResponseEntity.status(status)
                .headers(httpError.getHeaders())
                .body(new ErrorResponse("HTTP_" + status.value(), message));
        }

        log.error("Unexpected request error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ErrorResponse("INTERNAL_ERROR", "Internal server error"));
    }
}
