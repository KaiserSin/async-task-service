package com.example.payment.api.dto;

import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

public record PaymentResponseDTO(
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    Long id,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    UUID eventId,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    UUID paymentOperationId,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    UUID orderId,
    @Schema(description = "Amount in the currency's smallest unit", requiredMode = Schema.RequiredMode.REQUIRED)
    long amount,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    String currency,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    String paymentMethodId,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    ExecutionStatus executionStatus,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    PaymentStatus paymentStatus,
    @Schema(nullable = true, types = {"string", "null"}, requiredMode = Schema.RequiredMode.REQUIRED)
    String providerPaymentId,
    @Schema(nullable = true, types = {"string", "null"}, requiredMode = Schema.RequiredMode.REQUIRED)
    String result,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    Instant createdAt,
    @Schema(nullable = true, types = {"string", "null"}, format = "date-time",
        requiredMode = Schema.RequiredMode.REQUIRED)
    Instant startedAt,
    @Schema(nullable = true, types = {"string", "null"}, format = "date-time",
        requiredMode = Schema.RequiredMode.REQUIRED)
    Instant finishedAt,
    @Schema(description = "Deadline for the customer to finish 3D Secure", nullable = true,
        types = {"string", "null"}, format = "date-time", requiredMode = Schema.RequiredMode.REQUIRED)
    Instant actionExpiresAt,
    @Schema(description = "Current payment version for a card change")
    Long version
) {
}
