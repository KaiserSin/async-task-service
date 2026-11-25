package com.example.payment.worker.attempt;

final class StripeRequestRejectedException extends RuntimeException {
    private final int statusCode;

    StripeRequestRejectedException(int statusCode, String requestId) {
        super("Stripe request rejected (HTTP " + statusCode + ", requestId=" + requestId + ")");
        this.statusCode = statusCode;
    }

    boolean allowsAutomaticRetry() {
        return statusCode == 429;
    }
}
