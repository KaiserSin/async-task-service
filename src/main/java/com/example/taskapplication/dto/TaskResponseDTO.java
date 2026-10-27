package com.example.taskapplication.dto;

import com.example.taskapplication.model.TaskStatus;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

public record TaskResponseDTO(
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    Long id,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    UUID eventId,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    String name,
    @Schema(description = "Duration in milliseconds", requiredMode = Schema.RequiredMode.REQUIRED)
    long duration,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    TaskStatus status,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    int progress,
    @Schema(nullable = true, types = {"string", "null"}, requiredMode = Schema.RequiredMode.REQUIRED)
    String result,
    @Schema(requiredMode = Schema.RequiredMode.REQUIRED)
    Instant createdAt,
    @Schema(nullable = true, types = {"string", "null"}, format = "date-time",
        requiredMode = Schema.RequiredMode.REQUIRED)
    Instant startedAt,
    @Schema(nullable = true, types = {"string", "null"}, format = "date-time",
        requiredMode = Schema.RequiredMode.REQUIRED)
    Instant finishedAt
) {
}
