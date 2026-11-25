package com.example.payment.worker.attempt;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.stripe.StripeGateway;
import com.stripe.exception.AuthenticationException;
import com.stripe.exception.InvalidRequestException;
import com.stripe.exception.PermissionException;
import com.stripe.exception.RateLimitException;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentConfirmParams;
import com.stripe.param.PaymentIntentCreateParams;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Runs one payment attempt with Stripe. It creates a PaymentIntent, confirms an existing one
 * with a new card or checks what happened to an earlier attempt. Each request uses the time left
 * on the worker's lease to set its timeout.
 */
@Component
class StripePaymentHandler implements PaymentHandler {

    private final StripeGateway stripe;
    private final PaymentUpdateService updates;
    private final PaymentMetrics metrics;

    public StripePaymentHandler(StripeGateway stripe, PaymentUpdateService updates, PaymentMetrics metrics) {
        this.stripe = stripe;
        this.updates = updates;
        this.metrics = metrics;
    }

    @Override
    public void handle(PaymentAttempt attempt) {
        PaymentIntent intent;
        try {
            if (attempt.confirmationKey() != null) {
                var params = PaymentIntentConfirmParams.builder().setPaymentMethod(attempt.paymentMethodId()).build();
                var requestOptions = beginSubmission(attempt, attempt.confirmationKey());
                intent = stripe.confirm(attempt.paymentOperationId(), attempt.providerPaymentId(), params, requestOptions);
            } else {
                intent = attempt.createIntent() ? createIntent(attempt) : reconcileIntent(attempt);
            }
        } catch (StripeException e) {
            boolean submitted = attempt.createIntent() || attempt.confirmationKey() != null;
            intent = submitted && e.getStripeError() != null ? e.getStripeError().getPaymentIntent() : null;
            if (intent == null || (attempt.confirmationKey() != null && !attempt.providerPaymentId().equals(intent.getId()))) {
                if (e instanceof AuthenticationException || e instanceof PermissionException
                        || (submitted && isConfirmedRateLimit(e))
                        || (attempt.createIntent() && isCreationValidationFailure(e))) {
                    throw new StripeRequestRejectedException(e.getStatusCode(), e.getRequestId());
                }
                throw new IllegalStateException("Stripe request failed (HTTP " + e.getStatusCode()
                    + ", requestId=" + e.getRequestId() + ")");
            }
        }
        updates.saveStripeResult(attempt, intent.getId(), StripeGateway.paymentStatus(intent));
    }

    private boolean isCreationValidationFailure(StripeException e) {
        if (!(e instanceof InvalidRequestException) || !Integer.valueOf(400).equals(e.getStatusCode())) {
            return false;
        }
        return switch (String.valueOf(e.getCode())) {
            case "amount_too_small", "amount_too_large", "parameter_invalid_empty",
                "parameter_invalid_integer", "parameter_invalid_string_blank",
                "parameter_invalid_string_empty", "parameter_missing", "parameter_unknown",
                "parameters_exclusive" -> true;
            default -> false;
        };
    }

    private boolean isConfirmedRateLimit(StripeException e) {
        if (!(e instanceof RateLimitException)) {
            return false;
        }
        if ("lock_timeout".equals(e.getCode())) {
            return true;
        }
        var error = e.getStripeError();
        return error != null && error.getLastResponse() != null
            && error.getLastResponse().headers().firstValue("Stripe-Rate-Limited-Reason")
                .filter(reason -> !reason.isBlank()).isPresent();
    }

    private PaymentIntent createIntent(PaymentAttempt attempt) throws StripeException {
        var params = PaymentIntentCreateParams.builder()
            .setAmount(attempt.amount())
            .setCurrency(attempt.currency())
            .setPaymentMethod(attempt.paymentMethodId())
            .setConfirm(true)
            .setCaptureMethod(PaymentIntentCreateParams.CaptureMethod.AUTOMATIC)
            .addAllowedPaymentMethodType(PaymentIntentCreateParams.AllowedPaymentMethodType.CARD)
            .putMetadata("paymentOperationId", attempt.paymentOperationId().toString())
            .putMetadata("orderId", attempt.orderId().toString())
            .build();
        var requestOptions = beginSubmission(attempt, attempt.idempotencyKey());
        return stripe.create(attempt.paymentOperationId(), params, requestOptions);
    }

    private PaymentIntent reconcileIntent(PaymentAttempt attempt) throws StripeException {
        return stripe.findMatchingIntent(attempt.paymentOperationId(), attempt.orderId(), attempt.amount(), attempt.currency(),
            attempt.providerPaymentId(), () -> readRequestOptions(attempt)).orElseThrow(() ->
                new IllegalStateException("Could not identify a unique Stripe PaymentIntent for payment " + attempt.id()));
    }

    private RequestOptions beginSubmission(PaymentAttempt attempt, String idempotencyKey) {
        int timeout = requestTimeoutMillis(attempt);
        updates.markStripeSubmission(attempt);
        recordRetryRequest(attempt);
        return StripeGateway.requestOptions(idempotencyKey, timeout);
    }

    private RequestOptions readRequestOptions(PaymentAttempt attempt) {
        int timeout = requestTimeoutMillis(attempt);
        recordRetryRequest(attempt);
        return StripeGateway.requestOptions(null, timeout);
    }

    private void recordRetryRequest(PaymentAttempt attempt) {
        if (attempt.retry()) {
            metrics.recordWorkerStripeRetryRequest();
        }
    }

    private int requestTimeoutMillis(PaymentAttempt attempt) {
        long remainingMs = TimeUnit.NANOSECONDS.toMillis(attempt.leaseDeadlineNanos() - System.nanoTime());
        if (remainingMs < 2) {
            throw new IllegalStateException("Payment lease expired before Stripe request");
        }
        return (int) Math.min(10_000, remainingMs / 2);
    }
}
