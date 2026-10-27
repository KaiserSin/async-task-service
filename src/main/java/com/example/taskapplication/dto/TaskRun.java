package com.example.taskapplication.dto;

import java.util.UUID;

public record TaskRun(Long id, UUID executionId, long duration) {
}
