package com.example.payment.api.error;

public class PaymentRetryNotAllowedException extends RuntimeException {

    public PaymentRetryNotAllowedException(Long id) {
        super("Payment with ID " + id + " can only be retried when its status is FAILED");
    }
}
