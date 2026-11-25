package com.example.payment.registration;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentRegistrationDetails;
import com.example.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Currency;
import java.util.Locale;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PaymentRegistrationService {

    private final PaymentRepository paymentRepository;
    private int maxNewOperationsPerMinutePerOwner = 10;

    @Value("${payment.registration.max-new-operations-per-minute-per-owner:10}")
    public void setMaxNewOperationsPerMinutePerOwner(int maximum) {
        if (maximum < 1) {
            throw new IllegalArgumentException(
                "payment.registration.max-new-operations-per-minute-per-owner must be at least 1"
            );
        }
        this.maxNewOperationsPerMinutePerOwner = maximum;
    }

    @Transactional
    public void register(PaymentCommand dto) {
        try {
            Currency.getInstance(dto.currency().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new InvalidPaymentMessageException("Unknown currency for eventId=" + dto.eventId());
        }

        paymentRepository.lockOwnerAdmission(dto.ownerUsername());
        if (paymentRepository.recordRegistrationEvent(dto.eventId(), dto.paymentOperationId()) == 0
                && !dto.paymentOperationId().equals(paymentRepository.registeredOperationForEvent(dto.eventId()))) {
            throw new InvalidPaymentMessageException("Event already registered for another operation: " + dto.eventId());
        }
        boolean atLimit = paymentRepository.countRecentByOwnerUsername(dto.ownerUsername())
            >= maxNewOperationsPerMinutePerOwner;
        int inserted = 0;
        if (!atLimit) {
            inserted = paymentRepository.insertIfAbsent(
                dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
                dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
            );
        }

        Optional<Payment> active = inserted == 0 ? findExisting(dto) : Optional.empty();
        active.ifPresent(payment -> validateExisting(dto, payment));

        var archived = paymentRepository.findArchivedRegistration(dto.eventId(), dto.paymentOperationId());
        if (archived.isPresent()) {
            validateExisting(dto, archived.get());
            if (inserted == 1 && paymentRepository.discardUncommittedRegistration(dto.eventId(), dto.paymentOperationId()) != 1) {
                throw new IllegalStateException("Could not discard duplicate registration for eventId=" + dto.eventId());
            }
            return;
        }
        if (inserted == 1 || active.isPresent()) {
            return;
        }
        if (atLimit) {
            throw new InvalidPaymentMessageException("New payment operation limit reached for eventId=" + dto.eventId());
        }
        throw new IllegalStateException("Payment not found after insert conflict for eventId=" + dto.eventId());
    }

    private Optional<Payment> findExisting(PaymentCommand dto) {
        return paymentRepository.findByEventId(dto.eventId())
            .or(() -> paymentRepository.findByPaymentOperationId(dto.paymentOperationId()));
    }

    private void validateExisting(PaymentCommand dto, PaymentRegistrationDetails existing) {
        if (!existing.getPaymentOperationId().equals(dto.paymentOperationId())
            || !existing.getOrderId().equals(dto.orderId())
            || !dto.ownerUsername().equals(existing.getOwnerUsername())
            || !existing.getAmount().equals(dto.amount())
            || !existing.getCurrency().equals(dto.currency())
            || !existing.getPaymentMethodId().equals(dto.paymentMethodId())) {
            throw new InvalidPaymentMessageException(
                "Payment already registered with different values for eventId=" + dto.eventId()
            );
        }
    }
}
