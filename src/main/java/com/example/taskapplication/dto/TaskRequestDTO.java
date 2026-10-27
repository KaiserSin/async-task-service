package com.example.taskapplication.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.UUID;

@Data
public class TaskRequestDTO {

    @NotNull(message = "Event ID is required")
    private UUID eventId;

    @NotBlank(message = "Task name is required")
    @Size(max = 255, message = "Task name must be 255 characters or fewer")
    private String name;

    @NotNull(message = "Duration is required")
    @Positive(message = "Duration must be greater than 0")
    @Max(value = 60_000, message = "Duration must be 60 seconds or less")
    private Long duration;
}
