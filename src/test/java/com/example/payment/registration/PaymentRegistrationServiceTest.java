package com.example.payment.registration;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static com.example.payment.domain.PaymentFixtures.payment;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentRegistrationServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    private PaymentRegistrationService paymentRegistrationService;

    @BeforeEach
    void setUp() {
        paymentRegistrationService = new PaymentRegistrationService(paymentRepository);
        when(paymentRepository.recordRegistrationEvent(any(), any())).thenReturn(1);
    }

    @Test
    @DisplayName("Registers a new payment without a duplicate lookup")
    void registersNewPayment() {
        PaymentCommand dto = request("pm_card_visa");
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(1);

        paymentRegistrationService.register(dto);

        verify(paymentRepository).insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        );
        verify(paymentRepository, never()).findByEventId(any());
    }

    @Test
    @DisplayName("Preserves the exact payment method ID")
    void preservesExactPaymentMethodId() {
        PaymentCommand dto = request("pm_card_visa_debit");
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(1);

        paymentRegistrationService.register(dto);

        verify(paymentRepository).insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        );
    }

    @Test
    @DisplayName("Accepts a duplicate with identical parameters and the same payment method")
    void acceptsIdenticalDuplicate() {
        PaymentCommand dto = request("pm_card_visa");
        Payment existing = existingPayment(dto);
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(0);
        when(paymentRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.of(existing));

        assertThatCode(() -> paymentRegistrationService.register(dto))
            .doesNotThrowAnyException();

        verify(paymentRepository).findByEventId(dto.eventId());
    }

    @Test
    @DisplayName("Rejects a duplicate with a different payment method")
    void rejectsDuplicateWithDifferentPaymentMethodId() {
        PaymentCommand dto = request("pm_card_visa");
        Payment existing = payment(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), "pm_card_mastercard"
        );
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(0);
        when(paymentRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> paymentRegistrationService.register(dto))
            .isInstanceOf(InvalidPaymentMessageException.class);
    }

    @Test
    @DisplayName("Rejects a duplicate with a different amount")
    void rejectsDuplicateWithDifferentAmount() {
        PaymentCommand dto = request("pm_card_visa");
        Payment existing = payment(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            2_000L, dto.currency(), dto.paymentMethodId()
        );
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(0);
        when(paymentRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> paymentRegistrationService.register(dto))
            .isInstanceOf(InvalidPaymentMessageException.class);
    }

    @Test
    @DisplayName("A duplicate event cannot change the payment owner")
    void rejectsDuplicateWithDifferentOwner() {
        PaymentCommand dto = request("pm_card_visa");
        Payment existing = payment(
            dto.eventId(), "bob", dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId()
        );
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(0);
        when(paymentRepository.findByEventId(dto.eventId())).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> paymentRegistrationService.register(dto))
            .isInstanceOf(InvalidPaymentMessageException.class);
    }

    @Test
    @DisplayName("Accepts an existing payment operation delivered under a new event ID")
    void acceptsRepeatedOperationWithNewEventId() {
        PaymentCommand dto = request("pm_card_visa");
        Payment existing = payment(
            UUID.randomUUID(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId()
        );
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(0);
        when(paymentRepository.findByEventId(dto.eventId())).thenReturn(Optional.empty());
        when(paymentRepository.findByPaymentOperationId(dto.paymentOperationId()))
            .thenReturn(Optional.of(existing));

        assertThatCode(() -> paymentRegistrationService.register(dto))
            .doesNotThrowAnyException();

        verify(paymentRepository).findByPaymentOperationId(dto.paymentOperationId());
    }

    @Test
    @DisplayName("Preserves the exact owner identity without stripping or changing case")
    void preservesExactOwnerIdentity() {
        PaymentCommand dto = new PaymentCommand(
            UUID.randomUUID(), " Alice ", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_visa"
        );
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(1);

        paymentRegistrationService.register(dto);

        verify(paymentRepository).insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        );
    }

    @Test
    @DisplayName("Reports an error when the event record is missing after a conflict")
    void rejectsMissingPaymentAfterInsertConflict() {
        PaymentCommand dto = request("pm_card_visa");
        when(paymentRepository.insertIfAbsent(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId(), dto.paymentOperationId().toString()
        ))
            .thenReturn(0);
        when(paymentRepository.findByEventId(dto.eventId()))
            .thenReturn(Optional.empty());

        assertThatThrownBy(() -> paymentRegistrationService.register(dto))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("eventId=" + dto.eventId());
    }

    private PaymentCommand request(String paymentMethodId) {
        return new PaymentCommand(
            UUID.randomUUID(), "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", paymentMethodId
        );
    }

    private Payment existingPayment(PaymentCommand dto) {
        return payment(
            dto.eventId(), dto.ownerUsername(), dto.paymentOperationId(), dto.orderId(),
            dto.amount(), dto.currency(), dto.paymentMethodId()
        );
    }
}
