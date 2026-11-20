package com.example.taskapplication.service.registration;

import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.exception.InvalidTaskMessageException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class TaskRegistrationService {

    private final TaskRepository taskRepository;

    @Transactional
    public void register(TaskRequestDTO dto) {
        String normalizedName = dto.name().strip();
        int inserted = taskRepository.insertIfAbsent(
            dto.eventId(), normalizedName, dto.duration(), dto.ownerUsername()
        );

        if (inserted == 0) {
            Task existing = taskRepository.findByEventId(dto.eventId())
                .orElseThrow(() -> new IllegalStateException(
                    "Task not found after insert conflict for eventId=" + dto.eventId()
                ));

            if (!existing.getName().equals(normalizedName)
                || !existing.getDuration().equals(dto.duration())
                || !Objects.equals(existing.getOwnerUsername(), dto.ownerUsername())) {
                throw new InvalidTaskMessageException(
                    "eventId=" + dto.eventId() + " is already used with different values"
                );
            }
        }
    }
}
