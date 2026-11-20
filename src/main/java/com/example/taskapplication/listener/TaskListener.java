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
import java.util.stream.Collectors;

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
            throw new InvalidTaskMessageException("Message has no task (eventId=null)");
        }

        Set<ConstraintViolation<TaskRequestDTO>> violations = validator.validate(dto);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .sorted()
                .collect(Collectors.joining(", "));
            throw new ConstraintViolationException(
                "Task message validation failed for eventId=" + dto.eventId() + " (" + details + ")", violations
            );
        }

        if (record.key() != null && !record.key().equals(dto.eventId().toString())) {
            throw new InvalidTaskMessageException("Kafka key does not match eventId=" + dto.eventId());
        }

        taskRegistrationService.register(dto);
    }
}
