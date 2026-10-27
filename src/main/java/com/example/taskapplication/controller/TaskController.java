package com.example.taskapplication.controller;

import com.example.taskapplication.dto.ErrorResponse;
import com.example.taskapplication.dto.TaskResponseDTO;
import com.example.taskapplication.service.query.TaskQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
@Validated
@Tag(name = "Task API", description = "Get task status and results")
@ApiResponses({
    @ApiResponse(responseCode = "200", description = "Task found",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = TaskResponseDTO.class))),
    @ApiResponse(responseCode = "400", description = "Invalid ID",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ErrorResponse.class))),
    @ApiResponse(responseCode = "404", description = "Task not found",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ErrorResponse.class))),
    @ApiResponse(responseCode = "503", description = "Database is not available",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ErrorResponse.class))),
    @ApiResponse(responseCode = "500", description = "Internal server error",
        content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = ErrorResponse.class)))
})
public class TaskController {

    private final TaskQueryService taskQueryService;

    @GetMapping("/{id}")
    @Operation(summary = "Get task status and result")
    public ResponseEntity<TaskResponseDTO> getTask(
        @PathVariable("id") @Positive Long id
    ) {
        return ResponseEntity.ok()
            .cacheControl(CacheControl.noStore())
            .body(taskQueryService.findById(id));
    }

    @GetMapping("/by-event-id/{eventId}")
    @Operation(
        summary = "Get task by event ID",
        description = "404 means the task is not in the database. Its message may still be waiting to be processed."
    )
    public ResponseEntity<TaskResponseDTO> getTaskByEventId(
        @PathVariable("eventId") UUID eventId
    ) {
        return ResponseEntity.ok()
            .cacheControl(CacheControl.noStore())
            .body(taskQueryService.findByEventId(eventId));
    }
}
