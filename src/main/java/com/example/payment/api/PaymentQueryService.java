package com.example.payment.api;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;
import com.example.payment.api.dto.PaymentResponseDTO;
import com.example.payment.api.error.PaymentNotFoundException;
import com.example.payment.api.security.PaymentAccessService;

@Service
@RequiredArgsConstructor
class PaymentQueryService {

    private final PaymentRepository paymentRepository;
    private final PaymentAccessService paymentAccessService;

    @Transactional(readOnly = true)
    public PaymentResponseDTO findById(Long id) {
        Payment payment = paymentRepository.findById(id)
            .filter(paymentAccessService::canAccess)
            .orElseThrow(() -> new PaymentNotFoundException(id));

        return toResponse(payment);
    }

    @Transactional(readOnly = true)
    public PaymentResponseDTO findByEventId(UUID eventId) {
        Payment payment = paymentRepository.findByEventId(eventId)
            .filter(paymentAccessService::canAccess)
            .orElseThrow(() -> new PaymentNotFoundException(eventId));

        return toResponse(payment);
    }

    private PaymentResponseDTO toResponse(Payment payment) {
        return new PaymentResponseDTO(
            payment.getId(),
            payment.getEventId(),
            payment.getPaymentOperationId(),
            payment.getOrderId(),
            payment.getAmount(),
            payment.getCurrency(),
            payment.currentPaymentMethodId(),
            payment.getExecutionStatus(),
            payment.getPaymentStatus(),
            payment.getProviderPaymentId(),
            payment.getResult(),
            payment.getCreatedAt(),
            payment.getStartedAt(),
            payment.getFinishedAt(),
            payment.getPaymentStatus() == PaymentStatus.REQUIRES_ACTION ? payment.getActionExpiresAt() : null,
            payment.getVersion()
        );
    }
}
