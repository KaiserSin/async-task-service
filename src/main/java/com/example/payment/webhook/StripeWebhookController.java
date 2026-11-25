package com.example.payment.webhook;

import com.stripe.exception.SignatureVerificationException;
import com.stripe.model.Event;
import com.stripe.model.PaymentIntent;
import com.stripe.net.Webhook;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

@RestController
public class StripeWebhookController {

    private static final Set<String> SUPPORTED_EVENT_TYPES = Set.of(
        "payment_intent.succeeded",
        "payment_intent.payment_failed",
        "payment_intent.canceled",
        "payment_intent.processing",
        "payment_intent.requires_action"
    );

    private final StripeWebhookEventRepository events;
    private final String signingSecret;
    private final Counter missingSignatures;
    private final Counter invalidSignatures;
    private final Counter duplicateDeliveries;

    public StripeWebhookController(StripeWebhookEventRepository events, MeterRegistry registry,
                                   @Value("${stripe.webhook-secret}") String signingSecret) {
        if (!StringUtils.hasText(signingSecret) || !signingSecret.startsWith("whsec_")) {
            throw new IllegalArgumentException("STRIPE_WEBHOOK_SECRET must be a Stripe endpoint signing secret");
        }
        this.events = events;
        this.signingSecret = signingSecret;
        missingSignatures = registry.counter("payment.webhook.signature.errors", "reason", "missing");
        invalidSignatures = registry.counter("payment.webhook.signature.errors", "reason", "invalid");
        duplicateDeliveries = registry.counter("payment.webhook.duplicate.deliveries");
    }

    @PostMapping(path = "/api/webhooks/stripe", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> receive(@RequestBody(required = false) byte[] body,
                                        @RequestHeader(value = "Stripe-Signature", required = false) String signature,
                                        HttpServletRequest request) {
        if (!StringUtils.hasText(signature)) {
            missingSignatures.increment();
            return ResponseEntity.badRequest().build();
        }
        if (body == null) {
            return ResponseEntity.badRequest().build();
        }
        String payload;
        Event event;
        try {
            payload = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException invalidBody) {
            return ResponseEntity.badRequest().build();
        }
        try {
            event = Webhook.constructEvent(payload, signature, signingSecret);
        } catch (SignatureVerificationException invalidSignature) {
            invalidSignatures.increment();
            return ResponseEntity.badRequest().build();
        } catch (RuntimeException invalidEvent) {
            try {
                Webhook.Signature.verifyHeader(payload, signature, signingSecret, Webhook.DEFAULT_TOLERANCE);
            } catch (SignatureVerificationException | RuntimeException invalidSignature) {
                invalidSignatures.increment();
            }
            return ResponseEntity.badRequest().build();
        }
        if (event == null || !StringUtils.hasText(event.getId()) || !StringUtils.hasText(event.getType())) {
            return ResponseEntity.badRequest().build();
        }

        if (Boolean.FALSE.equals(event.getLivemode()) && SUPPORTED_EVENT_TYPES.contains(event.getType())) {
            try {
                if (!events.insertIfAbsent(event.getId(), event.getType(), payload)) {
                    duplicateDeliveries.increment();
                }
            } catch (DataAccessException ex) {
                request.setAttribute("paymentOperationId", operationId(event));
                throw ex;
            }
        }
        return ResponseEntity.ok().build();
    }

    private UUID operationId(Event event) {
        try {
            if (event.getDataObjectDeserializer().getObject().orElse(null) instanceof PaymentIntent intent
                    && intent.getMetadata() != null && intent.getMetadata().get("paymentOperationId") != null) {
                return UUID.fromString(intent.getMetadata().get("paymentOperationId"));
            }
        } catch (RuntimeException ignored) {
        }
        return null;
    }
}
