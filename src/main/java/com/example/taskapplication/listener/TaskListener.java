package com.example.taskapplication.listener;

import com.example.taskapplication.dto.TaskRequestDTO;
import com.example.taskapplication.exception.InvalidTaskMessageException;
import com.example.taskapplication.service.registration.TaskRegistrationService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@RequiredArgsConstructor
public class TaskListener {

    private final Validator validator;
    private final TaskRegistrationService taskRegistrationService;

    @KafkaListener(
        topics = "${kafka.topic:tasks}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void receive(ConsumerRecord<String, TaskRequestDTO> record) {
        TaskRequestDTO dto = record.value();
        if (dto == null) {
            throw new InvalidTaskMessageException("Message has no task");
        }

        Set<ConstraintViolation<TaskRequestDTO>> violations = validator.validate(dto);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException(violations);
        }

        if (record.key() != null && !record.key().equals(dto.getEventId().toString())) {
            throw new InvalidTaskMessageException("Kafka key does not match eventId");
        }

        taskRegistrationService.register(dto);
    }
}
