package com.example.taskapplication.service.query;

import com.example.taskapplication.dto.TaskResponseDTO;
import com.example.taskapplication.exception.TaskNotFoundException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TaskQueryService {

    private final TaskRepository taskRepository;

    @Transactional(readOnly = true)
    public TaskResponseDTO findById(Long id) {
        Task task = taskRepository.findById(id)
            .orElseThrow(() -> new TaskNotFoundException(id));

        return toResponse(task);
    }

    @Transactional(readOnly = true)
    public TaskResponseDTO findByEventId(UUID eventId) {
        Task task = taskRepository.findByEventId(eventId)
            .orElseThrow(() -> new TaskNotFoundException(eventId));

        return toResponse(task);
    }

    private TaskResponseDTO toResponse(Task task) {
        return new TaskResponseDTO(
            task.getId(),
            task.getEventId(),
            task.getName(),
            task.getDuration(),
            task.getStatus(),
            task.getProgress(),
            task.getResult(),
            task.getCreatedAt(),
            task.getStartedAt(),
            task.getFinishedAt()
        );
    }
}
