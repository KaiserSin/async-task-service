package com.example.taskapplication.dto;

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

class TaskRequestDTOTest {

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
    @ValueSource(longs = {1, 60_000})
    @DisplayName("Accepts the minimum and maximum duration")
    void acceptsDurationBoundaries(long duration) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", "My task", duration);

        var violations = validator.validate(dto);

        assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("Accepts a name with 255 characters")
    void acceptsNameWithMaximumLength() {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", "a".repeat(255), 1_000L);

        var violations = validator.validate(dto);

        assertThat(violations).isEmpty();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n"})
    @DisplayName("Rejects a missing or blank owner")
    void rejectsMissingOwner(String ownerUsername) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, ownerUsername, "My task", 1_000L);

        assertThat(validator.validate(dto))
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsOnly("ownerUsername");
    }

    @ParameterizedTest
    @MethodSource("controlCharacters")
    @DisplayName("Rejects control characters in the owner's identity")
    void rejectsControlCharacterInOwner(int codePoint) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice" + (char) codePoint + "smith", "My task", 1_000L);

        assertThat(validator.validate(dto))
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("ownerUsername");
    }

    @Test
    @DisplayName("Accepts the owner length limit and rejects longer identities")
    void validatesOwnerLength() {
        TaskRequestDTO maximum = new TaskRequestDTO(EVENT_ID, "a".repeat(255), "My task", 1_000L);
        assertThat(validator.validate(maximum)).isEmpty();

        TaskRequestDTO aboveMaximum = new TaskRequestDTO(EVENT_ID, "a".repeat(256), "My task", 1_000L);
        assertThat(validator.validate(aboveMaximum))
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("ownerUsername");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Обычная задача", "Tâche à réaliser", "任务 ✅", "Проверить 👩‍💻"})
    @DisplayName("Accepts Unicode names without control characters")
    void acceptsUnicodeName(String name) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", name, 1_000L);

        assertThat(validator.validate(dto)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("controlCharacters")
    @DisplayName("Rejects every Unicode control character in a name")
    void rejectsControlCharacterInName(int codePoint) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", "Task" + (char) codePoint + "name", 1_000L);

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("name");
        assertThat(violations)
            .extracting(violation -> violation.getMessage())
            .containsExactly("Task name must not contain control characters");
    }

    private static IntStream controlCharacters() {
        return IntStream.concat(IntStream.rangeClosed(0, 31), IntStream.rangeClosed(127, 159));
    }

    @Test
    @DisplayName("Rejects a missing event ID")
    void rejectsMissingEventId() {
        TaskRequestDTO dto = new TaskRequestDTO(null, "alice", "My task", 1_000L);

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("eventId");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "   ", "\t\n"})
    @DisplayName("Rejects a missing or blank name")
    void rejectsBlankName(String name) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", name, 1_000L);

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsOnly("name");
        assertThat(violations)
            .extracting(violation -> violation.getMessage())
            .contains("Task name is required");
    }

    @Test
    @DisplayName("Rejects a name longer than 255 characters")
    void rejectsNameAboveMaximumLength() {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", "a".repeat(256), 1_000L);

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("name");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = {0, -1, 60_001})
    @DisplayName("Rejects a missing or invalid duration")
    void rejectsInvalidDuration(Long duration) {
        TaskRequestDTO dto = new TaskRequestDTO(EVENT_ID, "alice", "My task", duration);

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("duration");
    }
}
