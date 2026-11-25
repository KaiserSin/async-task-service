package com.example.payment.registration;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentCommandTest {

    private static final UUID EVENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void closeValidator() {
        validatorFactory.close();
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 99_999_999})
    @DisplayName("Accepts the amount boundaries")
    void acceptsAmountBoundaries(long amount) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            amount, "usd", "pm_card_visa"
        );

        var violations = validator.validate(dto);

        assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("Accepts a paymentMethodId with 255 characters")
    void acceptsPaymentMethodIdWithMaximumLength() {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_" + "a".repeat(252)
        );

        var violations = validator.validate(dto);

        assertThat(violations).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n"})
    @DisplayName("Rejects a missing or blank owner")
    void rejectsMissingOwner(String ownerUsername) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, ownerUsername, UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_visa"
        );

        assertThat(validator.validate(dto))
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsOnly("ownerUsername");
    }

    @ParameterizedTest
    @MethodSource("controlCharacters")
    @DisplayName("Rejects control characters in the owner's identity")
    void rejectsControlCharacterInOwner(int codePoint) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice" + (char) codePoint + "smith", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_visa"
        );

        assertThat(validator.validate(dto))
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("ownerUsername");
    }

    @Test
    @DisplayName("Accepts the owner length limit and rejects longer identities")
    void validatesOwnerLength() {
        PaymentCommand maximum = new PaymentCommand(
            EVENT_ID, "a".repeat(255), UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_visa"
        );
        assertThat(validator.validate(maximum)).isEmpty();

        PaymentCommand aboveMaximum = new PaymentCommand(
            EVENT_ID, "a".repeat(256), UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_visa"
        );
        assertThat(validator.validate(aboveMaximum))
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("ownerUsername");
    }

    @ParameterizedTest
    @ValueSource(strings = {"pm_card_visa", "pm_card_mastercard", "pm_123ABC", "pm_card_visa_debit"})
    @DisplayName("Accepts valid payment method identifiers")
    void acceptsValidPaymentMethodId(String paymentMethodId) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", paymentMethodId
        );

        assertThat(validator.validate(dto)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("controlCharacters")
    @DisplayName("Rejects every Unicode control character in a paymentMethodId")
    void rejectsControlCharacterInPaymentMethodId(int codePoint) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_" + (char) codePoint + "visa"
        );

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("paymentMethodId");
        assertThat(violations)
            .extracting(violation -> violation.getMessage())
            .containsExactly("Expected a Stripe PaymentMethod ID");
    }

    private static IntStream controlCharacters() {
        return IntStream.concat(IntStream.rangeClosed(0, 31), IntStream.rangeClosed(127, 159));
    }

    @Test
    @DisplayName("Rejects a missing event ID")
    void rejectsMissingEventId() {
        PaymentCommand dto = new PaymentCommand(
            null, "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_card_visa"
        );

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("eventId");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   ", "\t\n"})
    @DisplayName("Rejects a missing or blank paymentMethodId")
    void rejectsBlankPaymentMethodId(String paymentMethodId) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", paymentMethodId
        );

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsOnly("paymentMethodId");
    }

    @Test
    @DisplayName("Rejects a paymentMethodId longer than 255 characters")
    void rejectsPaymentMethodIdAboveMaximumLength() {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            1_000L, "usd", "pm_" + "a".repeat(253)
        );

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("paymentMethodId");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = {0, -1, 100_000_000})
    @DisplayName("Rejects a missing or invalid amount")
    void rejectsInvalidAmount(Long amount) {
        PaymentCommand dto = new PaymentCommand(
            EVENT_ID, "alice", UUID.randomUUID(), UUID.randomUUID(),
            amount, "usd", "pm_card_visa"
        );

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("amount");
    }
}
