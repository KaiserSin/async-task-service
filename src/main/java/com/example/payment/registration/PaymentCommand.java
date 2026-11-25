package com.example.payment.registration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.util.UUID;

public record PaymentCommand(
    @NotNull(message = "Event ID is required")
    UUID eventId,

    @NotBlank(message = "Payment owner is required")
    @Size(max = 255, message = "Payment owner must be 255 characters or fewer")
    @Pattern(regexp = "[^\\p{Cc}]*", message = "Payment owner must not contain control characters")
    String ownerUsername,

    @NotNull(message = "Payment operation ID is required")
    UUID paymentOperationId,

    @NotNull(message = "Order ID is required")
    UUID orderId,

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be positive")
    @Max(value = 99_999_999, message = "Amount must contain at most eight digits")
    Long amount,

    @NotNull(message = "Currency is required")
    @Pattern(regexp = "[a-z]{3}", message = "Currency must be a lowercase three-letter ISO code")
    String currency,

    @NotNull(message = "Payment method ID is required")
    @Size(max = 255)
    @Pattern(regexp = "pm_[a-zA-Z0-9_]+", message = "Expected a Stripe PaymentMethod ID")
    String paymentMethodId
) {
}
