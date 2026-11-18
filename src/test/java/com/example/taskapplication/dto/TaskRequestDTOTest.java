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
        TaskRequestDTO dto = validRequest();
        dto.setDuration(duration);

        var violations = validator.validate(dto);

        assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("Accepts a name with 255 characters")
    void acceptsNameWithMaximumLength() {
        TaskRequestDTO dto = validRequest();
        dto.setName("a".repeat(255));

        var violations = validator.validate(dto);

        assertThat(violations).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"Обычная задача", "Tâche à réaliser", "任务 ✅", "Проверить 👩‍💻"})
    @DisplayName("Accepts Unicode names without control characters")
    void acceptsUnicodeName(String name) {
        TaskRequestDTO dto = validRequest();
        dto.setName(name);

        assertThat(validator.validate(dto)).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("controlCharacters")
    @DisplayName("Rejects every Unicode control character in a name")
    void rejectsControlCharacterInName(int codePoint) {
        TaskRequestDTO dto = validRequest();
        dto.setName("Task" + (char) codePoint + "name");

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
        TaskRequestDTO dto = validRequest();
        dto.setEventId(null);

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
        TaskRequestDTO dto = validRequest();
        dto.setName(name);

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
        TaskRequestDTO dto = validRequest();
        dto.setName("a".repeat(256));

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
        TaskRequestDTO dto = validRequest();
        dto.setDuration(duration);

        var violations = validator.validate(dto);

        assertThat(violations)
            .extracting(violation -> violation.getPropertyPath().toString())
            .containsExactly("duration");
    }

    private TaskRequestDTO validRequest() {
        TaskRequestDTO dto = new TaskRequestDTO();
        dto.setEventId(UUID.randomUUID());
        dto.setName("My task");
        dto.setDuration(1_000L);
        return dto;
    }
}
