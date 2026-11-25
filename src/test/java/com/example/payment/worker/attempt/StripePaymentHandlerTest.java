package com.example.payment.worker.attempt;

import com.example.payment.observability.PaymentMetrics;
import com.example.payment.domain.PaymentStatus;
import com.example.payment.stripe.StripeGateway;
import com.stripe.StripeClient;
import com.stripe.exception.ApiConnectionException;
import com.stripe.exception.ApiException;
import com.stripe.exception.InvalidRequestException;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.StripeError;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentConfirmParams;
import com.stripe.param.PaymentIntentCreateParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.util.ReflectionTestUtils.setField;

class StripePaymentHandlerTest {
    private final PaymentUpdateService updates = mock(PaymentUpdateService.class);
    private final PaymentMetrics metrics = mock(PaymentMetrics.class);
    private final StripeClient stripe = mock(StripeClient.class, RETURNS_DEEP_STUBS);
    private StripePaymentHandler handler;
    private PaymentAttempt attempt;

    @BeforeEach
    void setUp() {
        var gateway = new StripeGateway("sk_test_unit");
        setField(gateway, "stripe", stripe);
        handler = new StripePaymentHandler(gateway, updates, metrics);
        attempt = newAttempt(true, null, System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
    }

    @Test
    void commitsSubmissionBeforeCreatingIntent() throws Exception {
        PaymentIntent intent = intent();
        when(stripe.v1().paymentIntents().create(any(PaymentIntentCreateParams.class), any(RequestOptions.class)))
            .thenAnswer(invocation -> {
                verify(updates).markStripeSubmission(attempt);
                return intent;
            });
        handler.handle(attempt);
        verify(updates).saveStripeResult(attempt, "pi_test", PaymentStatus.PROCESSING);
    }

    @Test
    void commitsSubmissionBeforeConfirmingIntent() throws Exception {
        attempt = newAttempt(false, "confirm-key", System.nanoTime() + TimeUnit.SECONDS.toNanos(30));
        when(stripe.v1().paymentIntents().confirm(any(String.class), any(PaymentIntentConfirmParams.class), any(RequestOptions.class)))
            .thenAnswer(invocation -> {
                verify(updates).markStripeSubmission(attempt);
                return intent();
            });
        handler.handle(attempt);
        verify(updates).saveStripeResult(attempt, "pi_test", PaymentStatus.PROCESSING);
    }

    @Test
    void failedFenceDoesNotSendCreateRequest() throws Exception {
        doThrow(new IllegalStateException("Lease expired")).when(updates).markStripeSubmission(attempt);
        assertThatThrownBy(() -> handler.handle(attempt)).isInstanceOf(IllegalStateException.class);
        verify(stripe.v1().paymentIntents(), never()).create(any(PaymentIntentCreateParams.class), any(RequestOptions.class));
    }

    @Test
    void expiredLocalDeadlineDoesNotReachSubmissionBoundary() throws Exception {
        attempt = newAttempt(true, null, System.nanoTime() - 1);
        assertThatThrownBy(() -> handler.handle(attempt)).isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(updates);
        verify(stripe.v1().paymentIntents(), never()).create(any(PaymentIntentCreateParams.class), any(RequestOptions.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"amount_too_small", "amount_too_large", "parameter_missing"})
    void confirmedCreationValidationFailureIsTerminal(String code) throws Exception {
        var failure = new InvalidRequestException("Validation failed", "amount", "req_test", code, 400, null);
        failCreation(failure);
        assertThatThrownBy(() -> handler.handle(attempt))
            .isInstanceOfSatisfying(StripeRequestRejectedException.class,
                rejection -> assertThat(rejection.allowsAutomaticRetry()).isFalse());
        verify(updates, never()).saveStripeResult(any(), any(), any());
    }

    @Test
    void networkAndServerFailuresRemainUncertain() throws Exception {
        for (StripeException failure : new StripeException[] {
                new ApiConnectionException("Connection lost"), new ApiException("Server error", "req_test", null, 500, null),
                new InvalidRequestException("Conflict", null, "req_test", "idempotency_key_in_use", 400, null),
                new InvalidRequestException("Unclassified", null, "req_test", null, 400, null)}) {
            failCreation(failure);
            assertThatThrownBy(() -> handler.handle(attempt))
                .isInstanceOf(IllegalStateException.class).isNotInstanceOf(StripeRequestRejectedException.class);
        }
    }

    @Test
    void embeddedIntentTakesPrecedenceOverValidationClassification() throws Exception {
        var failure = new InvalidRequestException("Validation failed", "amount", "req_test", "amount_too_small", 400, null);
        var error = new StripeError();
        error.setPaymentIntent(intent());
        failure.setStripeError(error);
        failCreation(failure);
        handler.handle(attempt);
        verify(updates).saveStripeResult(attempt, "pi_test", PaymentStatus.PROCESSING);
    }

    private void failCreation(StripeException failure) throws StripeException {
        var intents = stripe.v1().paymentIntents();
        doThrow(failure).when(intents)
            .create(any(PaymentIntentCreateParams.class), any(RequestOptions.class));
    }

    private PaymentIntent intent() {
        var intent = new PaymentIntent();
        intent.setId("pi_test");
        intent.setStatus("processing");
        return intent;
    }

    private PaymentAttempt newAttempt(boolean create, String confirmationKey, long deadline) {
        return new PaymentAttempt(1L, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
            1000L, "eur", "pm_test", "create-key", deadline, create, create ? null : "pi_test", 1L,
            confirmationKey, false);
    }
}
