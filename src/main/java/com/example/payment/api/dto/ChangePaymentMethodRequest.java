package com.example.payment.api.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;

public record ChangePaymentMethodRequest(
    @NotNull @Size(max = 255) @Pattern(regexp = "pm_[a-zA-Z0-9_]+") String paymentMethodId,
    @NotNull @PositiveOrZero Long version
) {}
