package com.example.payment.registration;

public class InvalidPaymentMessageException extends RuntimeException {

    public InvalidPaymentMessageException(String message) {
        super(message);
    }

    public InvalidPaymentMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
