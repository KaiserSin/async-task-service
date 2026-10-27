package com.example.taskapplication.service.registration;

import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.exception.InvalidTaskMessageException;
import com.example.taskapplication.model.Task;
import com.example.taskapplication.repository.TaskRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TaskRegistrationService {

    private final TaskRepository taskRepository;

    @Transactional
    public void register(TaskRequestDTO dto) {
        String normalizedName = dto.getName().strip();
        int inserted = taskRepository.insertIfAbsent(
            dto.getEventId(), normalizedName, dto.getDuration()
        );

        if (inserted == 0) {
            Task existing = taskRepository.findByEventId(dto.getEventId())
                .orElseThrow(() -> new IllegalStateException("Event was not found"));

            if (!existing.getName().equals(normalizedName)
                || !existing.getDuration().equals(dto.getDuration())) {
                throw new InvalidTaskMessageException(
                    "eventId is already used with different values"
                );
            }
        }
    }
}
