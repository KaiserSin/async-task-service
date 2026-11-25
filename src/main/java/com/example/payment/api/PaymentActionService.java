package com.example.payment.api;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import com.example.payment.api.dto.ChangePaymentMethodRequest;
import com.example.payment.api.error.PaymentNotFoundException;
import com.example.payment.api.security.PaymentAccessService;

/**
 * Handles card changes and retries requested through the REST API.
 * Only the payment owner can change the card. The owner or an admin can retry a failed payment.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentActionService {

    private final PaymentRepository paymentRepository;
    private final PaymentAccessService paymentAccessService;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean changePaymentMethod(Long id, String owner, ChangePaymentMethodRequest request) {
        Payment payment = paymentRepository.findByIdForUpdate(id)
            .filter(row -> owner.equals(row.getOwnerUsername()))
            .orElseThrow(() -> new PaymentNotFoundException(id));
        return !payment.isAbandonmentStarted()
            && payment.changePaymentMethod(request.paymentMethodId(), request.version());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean retryFailedPayment(Long id) {
        Payment payment = paymentRepository.findByIdForUpdate(id)
            .filter(paymentAccessService::canAccess)
            .orElseThrow(() -> new PaymentNotFoundException(id));

        if (payment.isAbandonmentStarted() || !payment.resetForRetry()) {
            return false;
        }

        log.info("Manual retry requested by {} for payment {}",
            SecurityContextHolder.getContext().getAuthentication().getName(), id);
        return true;
    }
}
