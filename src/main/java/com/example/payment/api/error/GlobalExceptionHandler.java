package com.example.payment.api.error;

import com.example.payment.database.DatabaseFailureClassifier;
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

import java.util.UUID;
import java.util.stream.Collectors;
import com.example.payment.api.dto.ApiError;

@RestControllerAdvice
@Slf4j
@RequiredArgsConstructor
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private final DatabaseFailureClassifier databaseFailureClassifier;

    @ExceptionHandler(PaymentNotFoundException.class)
    public ResponseEntity<ApiError> handlePaymentNotFound(PaymentNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .cacheControl(CacheControl.noStore())
            .body(new ApiError("PAYMENT_NOT_FOUND", ex.getMessage()));
    }

    @ExceptionHandler(PaymentRetryNotAllowedException.class)
    public ResponseEntity<ApiError> handlePaymentRetryNotAllowed(PaymentRetryNotAllowedException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
            .cacheControl(CacheControl.noStore())
            .body(new ApiError("PAYMENT_NOT_RETRYABLE", ex.getMessage()));
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
            case "id" -> "Payment ID must be a whole number greater than 0";
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
            logRequestFailure(ex, request);
        }
        String message = status.is4xxClientError() ? "Invalid request" : "Internal server error";
        ApiError error = new ApiError("HTTP_" + status.value(), message);
        return super.handleExceptionInternal(ex, error, headers, status, request);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex, WebRequest request) {
        if (logRequestFailure(ex, request) == DatabaseFailureClassifier.Action.RETRY) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiError("SERVICE_UNAVAILABLE", "Service is not available right now"));
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
            .body(new ApiError("INTERNAL_ERROR", "Internal server error"));
    }

    private DatabaseFailureClassifier.Action logRequestFailure(Exception ex, WebRequest request) {
        var failure = databaseFailureClassifier.inspect(ex);
        if (failure.action() == DatabaseFailureClassifier.Action.NOT_DATABASE) {
            log.error("Could not handle HTTP request", ex);
        } else {
            Object id = request.getAttribute("paymentOperationId", WebRequest.SCOPE_REQUEST);
            log.error("Database error {}", failure.logDetails(id instanceof UUID uuid ? uuid : null));
        }
        return failure.action();
    }
}
