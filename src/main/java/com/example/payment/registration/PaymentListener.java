package com.example.payment.registration;

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
class PaymentListener {

    private final Validator validator;
    private final PaymentRegistrationService paymentRegistrationService;

    @KafkaListener(
        topics = "${kafka.topic:payments}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void receive(ConsumerRecord<String, PaymentCommand> record) {
        PaymentCommand dto = record.value();
        if (dto == null) {
            throw new InvalidPaymentMessageException("Message has no payment (eventId=null)");
        }

        Set<ConstraintViolation<PaymentCommand>> violations = validator.validate(dto);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                .sorted()
                .collect(Collectors.joining(", "));
            throw new ConstraintViolationException(
                "Payment message validation failed for eventId=" + dto.eventId() + " (" + details + ")", violations
            );
        }

        if (record.key() != null && !record.key().equals(dto.eventId().toString())) {
            throw new InvalidPaymentMessageException("Kafka key does not match eventId=" + dto.eventId());
        }

        paymentRegistrationService.register(dto);
    }
}
