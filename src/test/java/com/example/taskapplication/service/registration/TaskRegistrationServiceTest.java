package com.example.taskapplication.service.registration;

import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.exception.InvalidTaskMessageException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskRegistrationServiceTest {

    @Mock
    private TaskRepository taskRepository;

    private TaskRegistrationService taskRegistrationService;

    @BeforeEach
    void setUp() {
        taskRegistrationService = new TaskRegistrationService(taskRepository);
    }

    @Test
    @DisplayName("Strips surrounding whitespace and registers a new task without a duplicate lookup")
    void registersNewTaskWithNormalizedName() {
        TaskRequestDTO dto = request("  My task  ");
        when(taskRepository.insertIfAbsent(dto.getEventId(), "My task", 1_000L))
            .thenReturn(1);

        taskRegistrationService.register(dto);

        verify(taskRepository).insertIfAbsent(dto.getEventId(), "My task", 1_000L);
        verify(taskRepository, never()).findByEventId(any());
    }

    @Test
    @DisplayName("Preserves spaces inside the name")
    void preservesSpacesInsideName() {
        TaskRequestDTO dto = request("  My  task  ");
        when(taskRepository.insertIfAbsent(dto.getEventId(), "My  task", 1_000L))
            .thenReturn(1);

        taskRegistrationService.register(dto);

        verify(taskRepository).insertIfAbsent(dto.getEventId(), "My  task", 1_000L);
    }

    @Test
    @DisplayName("Accepts a duplicate with identical parameters after name normalization")
    void acceptsIdenticalDuplicate() {
        TaskRequestDTO dto = request("  My task  ");
        Task existing = existingTask(dto);
        when(taskRepository.insertIfAbsent(dto.getEventId(), "My task", 1_000L))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.getEventId()))
            .thenReturn(Optional.of(existing));

        assertThatCode(() -> taskRegistrationService.register(dto))
            .doesNotThrowAnyException();

        verify(taskRepository).findByEventId(dto.getEventId());
    }

    @Test
    @DisplayName("Rejects a duplicate with a different name")
    void rejectsDuplicateWithDifferentName() {
        TaskRequestDTO dto = request("My task");
        Task existing = existingTask(dto);
        existing.setName("Another task");
        when(taskRepository.insertIfAbsent(dto.getEventId(), "My task", 1_000L))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.getEventId()))
            .thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(InvalidTaskMessageException.class);
    }

    @Test
    @DisplayName("Rejects a duplicate with a different duration")
    void rejectsDuplicateWithDifferentDuration() {
        TaskRequestDTO dto = request("My task");
        Task existing = existingTask(dto);
        existing.setDuration(2_000L);
        when(taskRepository.insertIfAbsent(dto.getEventId(), "My task", 1_000L))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.getEventId()))
            .thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(InvalidTaskMessageException.class);
    }

    @Test
    @DisplayName("Reports an error when the event record is missing after a conflict")
    void rejectsMissingTaskAfterInsertConflict() {
        TaskRequestDTO dto = request("My task");
        when(taskRepository.insertIfAbsent(dto.getEventId(), "My task", 1_000L))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.getEventId()))
            .thenReturn(Optional.empty());

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(IllegalStateException.class);
    }

    private TaskRequestDTO request(String name) {
        TaskRequestDTO dto = new TaskRequestDTO();
        dto.setEventId(UUID.randomUUID());
        dto.setName(name);
        dto.setDuration(1_000L);
        return dto;
    }

    private Task existingTask(TaskRequestDTO dto) {
        Task task = new Task();
        task.setEventId(dto.getEventId());
        task.setName(dto.getName().strip());
        task.setDuration(dto.getDuration());
        return task;
    }
}
