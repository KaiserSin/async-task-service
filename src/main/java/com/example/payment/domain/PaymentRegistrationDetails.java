package com.example.payment.domain;

import java.util.UUID;

public interface PaymentRegistrationDetails {
    UUID getPaymentOperationId();
    UUID getOrderId();
    String getOwnerUsername();
    Long getAmount();
    String getCurrency();
    String getPaymentMethodId();
}
