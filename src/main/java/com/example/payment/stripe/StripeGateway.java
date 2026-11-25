package com.example.payment.stripe;

import com.example.payment.domain.PaymentStatus;
import com.stripe.StripeClient;
import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.StripeObject;
import com.stripe.net.RequestOptions;
import com.stripe.param.PaymentIntentCancelParams;
import com.stripe.param.PaymentIntentConfirmParams;
import com.stripe.param.PaymentIntentCreateParams;
import com.stripe.param.PaymentIntentSearchParams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Calls Stripe to create, confirm, look up or cancel a PaymentIntent.
 * The payment worker, webhook processor and recovery service share this client.
 * It logs the Stripe request ID when Stripe provides one.
 */
@Component
@Slf4j
public class StripeGateway {

    private static final int BACKGROUND_TIMEOUT_MILLIS = 5000;

    private final StripeClient stripe;

    public StripeGateway(@Value("${stripe.secret-key}") String secretKey) {
        if (!secretKey.startsWith("sk_test_") && !secretKey.startsWith("rk_test_")) {
            throw new IllegalArgumentException("STRIPE_SECRET_KEY must be a Stripe test key");
        }
        this.stripe = new StripeClient(secretKey);
    }

    public record ResolvedIntent(String id, PaymentStatus status) {}

    public static PaymentStatus paymentStatus(PaymentIntent intent) {
        return PaymentStatus.valueOf(intent.getStatus().toUpperCase(Locale.ROOT));
    }

    public static RequestOptions requestOptions(String idempotencyKey, int timeoutMillis) {
        return RequestOptions.builder()
            .setIdempotencyKey(idempotencyKey)
            .setConnectTimeout(timeoutMillis)
            .setReadTimeout(timeoutMillis)
            .setMaxNetworkRetries(0)
            .build();
    }

    public PaymentIntent create(UUID operationId, PaymentIntentCreateParams params, RequestOptions options)
            throws StripeException {
        return call(operationId, "create", () -> stripe.v1().paymentIntents().create(params, options));
    }

    public PaymentIntent confirm(UUID operationId, String intentId, PaymentIntentConfirmParams params,
                                 RequestOptions options) throws StripeException {
        return call(operationId, "confirm", () -> stripe.v1().paymentIntents().confirm(intentId, params, options));
    }

    public PaymentStatus retrieveStatus(UUID operationId, String intentId) {
        try {
            return paymentStatus(call(operationId, "retrieve", () ->
                stripe.v1().paymentIntents().retrieve(intentId, requestOptions(null, BACKGROUND_TIMEOUT_MILLIS))));
        } catch (StripeException e) {
            throw new IllegalStateException("Could not retrieve Stripe PaymentIntent " + intentId
                + " (HTTP " + e.getStatusCode() + ", requestId=" + e.getRequestId() + ")");
        }
    }

    public PaymentStatus cancelAbandoned(UUID operationId, String intentId) {
        var params = PaymentIntentCancelParams.builder()
            .setCancellationReason(PaymentIntentCancelParams.CancellationReason.ABANDONED)
            .build();
        try {
            var intent = call(operationId, "cancel", () -> stripe.v1().paymentIntents().cancel(intentId, params,
                requestOptions("abandoned-" + intentId, BACKGROUND_TIMEOUT_MILLIS)));
            return paymentStatus(intent);
        } catch (StripeException e) {
            if ("payment_intent_unexpected_state".equals(e.getCode())) {
                return retrieveStatus(operationId, intentId);
            }
            throw new IllegalStateException("Could not cancel Stripe PaymentIntent " + intentId
                + " (HTTP " + e.getStatusCode() + ", requestId=" + e.getRequestId() + ")");
        }
    }

    public Optional<ResolvedIntent> findUnknownIntent(UUID operationId, UUID orderId, long amount,
                                                     String currency, String intentId) {
        try {
            return findMatchingIntent(operationId, orderId, amount, currency, intentId,
                () -> requestOptions(null, BACKGROUND_TIMEOUT_MILLIS))
                .map(intent -> new ResolvedIntent(intent.getId(), paymentStatus(intent)));
        } catch (StripeException e) {
            throw new IllegalStateException("Could not reconcile Stripe payment " + operationId
                + " (HTTP " + e.getStatusCode() + ", requestId=" + e.getRequestId() + ")");
        }
    }

    /**
     * Looks up a PaymentIntent by its saved Stripe ID. If there is no saved ID,
     * it searches Stripe by payment operation ID. Each Stripe call gets fresh request options.
     *
     * @return the matching PaymentIntent, or an empty result if the search finds zero or multiple matches
     * @throws IllegalStateException if the operation ID, order ID, amount or currency does not match
     * @throws StripeException if a Stripe request fails
     */
    public Optional<PaymentIntent> findMatchingIntent(UUID operationId, UUID orderId, long amount, String currency,
                                                     String intentId, Supplier<RequestOptions> requestOptions)
            throws StripeException {
        if (intentId == null) {
            var search = PaymentIntentSearchParams.builder()
                .setQuery("metadata['paymentOperationId']:'" + operationId + "'")
                .setLimit(2L)
                .build();
            var matches = call(operationId, "search", () ->
                stripe.v1().paymentIntents().search(search, requestOptions.get()));
            if (matches.getData().size() != 1 || Boolean.TRUE.equals(matches.getHasMore())) {
                return Optional.empty();
            }
            intentId = matches.getData().getFirst().getId();
        }

        String matchedIntentId = intentId;
        var intent = call(operationId, "retrieve", () ->
            stripe.v1().paymentIntents().retrieve(matchedIntentId, requestOptions.get()));
        var metadata = intent.getMetadata();
        if (metadata == null || !operationId.toString().equals(metadata.get("paymentOperationId"))
                || !orderId.toString().equals(metadata.get("orderId"))
                || !Long.valueOf(amount).equals(intent.getAmount()) || !currency.equals(intent.getCurrency())) {
            throw new IllegalStateException("Stripe PaymentIntent does not match payment operation " + operationId);
        }
        return Optional.of(intent);
    }

    @FunctionalInterface
    private interface StripeCall<T> {
        T execute() throws StripeException;
    }

    private <T extends StripeObject> T call(UUID operationId, String action, StripeCall<T> call)
            throws StripeException {
        try {
            T result = call.execute();
            var response = result.getLastResponse();
            if (response != null && response.requestId() != null && !response.requestId().isBlank()) {
                log.info("Stripe action={} paymentOperationId={} stripeRequestId={} httpStatus={}",
                    action, operationId, response.requestId(), response.code());
            }
            return result;
        } catch (StripeException e) {
            if (e.getRequestId() != null && !e.getRequestId().isBlank()) {
                log.warn("Stripe action={} paymentOperationId={} stripeRequestId={} httpStatus={}",
                    action, operationId, e.getRequestId(), e.getStatusCode());
            }
            throw e;
        }
    }
}
