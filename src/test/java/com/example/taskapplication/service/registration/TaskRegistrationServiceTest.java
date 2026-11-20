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

import static com.example.taskapplication.model.TaskFixtures.task;
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
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, dto.ownerUsername()))
            .thenReturn(1);

        taskRegistrationService.register(dto);

        verify(taskRepository).insertIfAbsent(dto.eventId(), "My task", 1_000L, dto.ownerUsername());
        verify(taskRepository, never()).findByEventId(any());
    }

    @Test
    @DisplayName("Preserves spaces inside the name")
    void preservesSpacesInsideName() {
        TaskRequestDTO dto = request("  My  task  ");
        when(taskRepository.insertIfAbsent(dto.eventId(), "My  task", 1_000L, dto.ownerUsername()))
            .thenReturn(1);

        taskRegistrationService.register(dto);

        verify(taskRepository).insertIfAbsent(dto.eventId(), "My  task", 1_000L, dto.ownerUsername());
    }

    @Test
    @DisplayName("Accepts a duplicate with identical parameters after name normalization")
    void acceptsIdenticalDuplicate() {
        TaskRequestDTO dto = request("  My task  ");
        Task existing = existingTask(dto);
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, dto.ownerUsername()))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.of(existing));

        assertThatCode(() -> taskRegistrationService.register(dto))
            .doesNotThrowAnyException();

        verify(taskRepository).findByEventId(dto.eventId());
    }

    @Test
    @DisplayName("Rejects a duplicate with a different name")
    void rejectsDuplicateWithDifferentName() {
        TaskRequestDTO dto = request("My task");
        Task existing = task(dto.eventId(), dto.ownerUsername(), "Another task", dto.duration());
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, dto.ownerUsername()))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(InvalidTaskMessageException.class);
    }

    @Test
    @DisplayName("Rejects a duplicate with a different duration")
    void rejectsDuplicateWithDifferentDuration() {
        TaskRequestDTO dto = request("My task");
        Task existing = task(dto.eventId(), dto.ownerUsername(), dto.name().strip(), 2_000L);
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, dto.ownerUsername()))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(InvalidTaskMessageException.class);
    }

    @Test
    @DisplayName("A duplicate event cannot change the task owner")
    void rejectsDuplicateWithDifferentOwner() {
        TaskRequestDTO dto = request("My task");
        Task existing = task(dto.eventId(), "bob", dto.name().strip(), dto.duration());
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, "alice"))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.eventId())).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(InvalidTaskMessageException.class);
    }

    @Test
    @DisplayName("A new message cannot claim ownership of a legacy task")
    void rejectsDuplicateOfUnownedLegacyTask() {
        TaskRequestDTO dto = request("My task");
        Task existing = task(dto.eventId(), null, dto.name().strip(), dto.duration());
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, "alice"))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.eventId())).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(InvalidTaskMessageException.class);
    }

    @Test
    @DisplayName("Preserves the exact owner identity without stripping or changing case")
    void preservesExactOwnerIdentity() {
        TaskRequestDTO dto = new TaskRequestDTO(UUID.randomUUID(), " Alice ", "My task", 1_000L);
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, " Alice "))
            .thenReturn(1);

        taskRegistrationService.register(dto);

        verify(taskRepository).insertIfAbsent(dto.eventId(), "My task", 1_000L, " Alice ");
    }

    @Test
    @DisplayName("Reports an error when the event record is missing after a conflict")
    void rejectsMissingTaskAfterInsertConflict() {
        TaskRequestDTO dto = request("My task");
        when(taskRepository.insertIfAbsent(dto.eventId(), "My task", 1_000L, dto.ownerUsername()))
            .thenReturn(0);
        when(taskRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.empty());

        assertThatThrownBy(() -> taskRegistrationService.register(dto))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("eventId=" + dto.eventId());
    }

    private TaskRequestDTO request(String name) {
        return new TaskRequestDTO(UUID.randomUUID(), "alice", name, 1_000L);
    }

    private Task existingTask(TaskRequestDTO dto) {
        return task(dto.eventId(), dto.ownerUsername(), dto.name().strip(), dto.duration());
    }
}
