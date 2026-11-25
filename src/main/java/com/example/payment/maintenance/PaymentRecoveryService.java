package com.example.payment.maintenance;

import com.example.payment.database.DatabaseLogFilter;
import com.example.payment.worker.WorkerProperties;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.domain.PaymentRepository.RecoveryCounts;
import com.example.payment.stripe.StripeGateway;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class PaymentRecoveryService {

    private static final int STATUS_CHECK_INTERVAL_MINUTES = 15;

    private final PaymentRepository paymentRepository;
    private final WorkerProperties workerProperties;

    private final StripeGateway stripe;

    public void reconcileOrCancelPendingPayment() {
        paymentRepository.reservePendingIntent(STATUS_CHECK_INTERVAL_MINUTES).ifPresent(intent -> {
            DatabaseLogFilter.setBackgroundOperation(intent.operationId());
            var status = stripe.retrieveStatus(intent.operationId(), intent.intentId());
            long version = intent.version();
            boolean terminal = status == PaymentStatus.SUCCEEDED || status == PaymentStatus.CANCELED;
            if (!terminal && status != PaymentStatus.PROCESSING && (intent.abandonmentStarted()
                    || (status == PaymentStatus.REQUIRES_ACTION && status.name().equals(intent.paymentStatus())))) {
                if (paymentRepository.beginAbandonment(intent.intentId(), version) == 0) {
                    return;
                }
                version++;
                status = stripe.cancelAbandoned(intent.operationId(), intent.intentId());
            }
            if (!status.name().equals(intent.paymentStatus())) {
                paymentRepository.updateReconciledStatus(intent.intentId(), version, status.name());
            }
        });
    }

    public void reconcileFailedUnknownPayment() {
        paymentRepository.reserveFailedUnknownPayment().ifPresent(payment -> {
            DatabaseLogFilter.setBackgroundOperation(payment.operationId());
            var match = stripe.findUnknownIntent(payment.operationId(), payment.orderId(), payment.amount(),
                payment.currency(), payment.intentId());
            match.ifPresent(intent -> paymentRepository.resolveFailedUnknownPayment(
                payment.operationId(), payment.version(), intent.id(), intent.status().name()));
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public RecoveryCounts recoverExpiredLeases() {
        return paymentRepository.recoverExpiredLeases(workerProperties.retryBackoffSeconds());
    }
}
