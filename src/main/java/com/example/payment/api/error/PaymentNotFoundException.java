package com.example.payment.api.error;

import java.util.UUID;

public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(Long id) {
        super("Payment with ID " + id + " was not found");
    }

    public PaymentNotFoundException(UUID eventId) {
        super("Payment with eventId " + eventId + " was not found");
    }
}
