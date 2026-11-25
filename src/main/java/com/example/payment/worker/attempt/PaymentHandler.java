package com.example.payment.worker.attempt;


/**
 * Handles one payment attempt. A handler must stop when the worker is interrupted
 * or its lease expires. When it retries the same operation, it must reuse the idempotency key.
 */
@FunctionalInterface
public interface PaymentHandler {
    void handle(PaymentAttempt attempt) throws InterruptedException;
}
