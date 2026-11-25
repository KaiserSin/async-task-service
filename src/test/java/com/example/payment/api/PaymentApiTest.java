package com.example.payment.api;

import com.example.payment.registration.PaymentCommand;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentFixtures;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.doThrow;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.util.ReflectionTestUtils.setField;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = ApiTestConfig.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false"
})
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentApiTest {

    private static final UUID EVENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final List<String> PAYMENT_FIELDS = List.of(
        "id", "eventId", "paymentOperationId", "orderId", "amount", "currency", "paymentMethodId",
        "executionStatus", "paymentStatus", "providerPaymentId", "result",
        "createdAt", "startedAt", "finishedAt", "actionExpiresAt", "version"
    );

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PaymentRepository paymentRepository;

    @MockitoSpyBean
    private PaymentQueryService paymentQueryService;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAllInBatch();
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"NEW", "COMPLETED"})
    @DisplayName("Returns public fields of new and completed payments by ID")
    void findsPayment(ExecutionStatus paymentStatus) throws Exception {
        Payment payment = paymentRepository.saveAndFlush(payment(paymentStatus));

        JsonNode body = getJson("/api/payments/" + payment.getId(), 200);

        assertPayment(body, payment);
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"NEW", "COMPLETED"})
    @DisplayName("Returns the same DTO with the internal ID by eventId")
    void findsByEventId(ExecutionStatus paymentStatus) throws Exception {
        Payment payment = paymentRepository.saveAndFlush(payment(paymentStatus));

        JsonNode body = getJson("/api/payments/by-event-id/" + payment.getEventId(), 200);

        assertPayment(body, payment);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc", "9223372036854775808"})
    @DisplayName("Invalid ID returns 400 with a clear message")
    void rejectsId(String id) throws Exception {
        assertError(getJson("/api/payments/" + id, 400), "INVALID_ARGUMENT",
            "Payment ID must be a whole number greater than 0");
    }

    @Test
    void reportsConstraintViolationForAnotherField() throws Exception {
        try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
            var violations = validatorFactory.getValidator().validateValue(PaymentCommand.class, "paymentMethodId", null);
            doThrow(new ConstraintViolationException(violations)).when(paymentQueryService).findById(1L);

            assertError(getJson("/api/payments/1", 400), "INVALID_ARGUMENT", "Payment method ID is required");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"not-a-uuid", "123"})
    @DisplayName("Invalid eventId returns 400 with a UUID message")
    void rejectsEventId(String eventId) throws Exception {
        assertError(getJson("/api/payments/by-event-id/" + eventId, 400), "INVALID_ARGUMENT",
            "eventId must be a valid UUID");
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "eventId"})
    @DisplayName("Missing payment returns 404 for both identifiers")
    void missesPayment(String lookup) throws Exception {
        String value = lookup.equals("id") ? "9223372036854775807" : EVENT_ID.toString();
        String path = lookup.equals("id") ? "/api/payments/" : "/api/payments/by-event-id/";
        String message = lookup.equals("id")
            ? "Payment with ID " + value + " was not found"
            : "Payment with eventId " + value + " was not found";

        assertError(getJson(path + value, 404), "PAYMENT_NOT_FOUND", message);
    }

    @Test
    @DisplayName("Connection and transaction creation failures return 503")
    void handlesDatabaseError() throws Exception {

        doThrow(new CannotCreateTransactionException("Sensitive detail about the connection"))
            .when(paymentQueryService).findById(1L);
        doThrow(new DataAccessResourceFailureException("Sensitive detail about SQL"))
            .when(paymentQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/payments/1", "/api/payments/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
        }
    }

    @Test
    @DisplayName("Connection failure during rollback returns 503")
    void handlesRollbackError() throws Exception {
        doThrow(new JpaSystemException(new org.hibernate.TransactionException(
            "Unable to rollback against JDBC Connection", new SQLRecoverableException("Sensitive recoverable connection failure")
        ))).when(paymentQueryService).findById(1L);
        doThrow(new TransactionSystemException("Sensitive detail about rollback",
            new SQLException("Sensitive detail about the connection", "08006")))
            .when(paymentQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/payments/1", "/api/payments/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"25006", "40001", "53300", "57P01"})
    @DisplayName("Shared retryable database classification maps JPA and transaction failures to 503")
    void handlesRetryableDatabaseStates(String sqlState) throws Exception {
        doThrow(new JpaSystemException(new org.hibernate.exception.GenericJDBCException(
            "Sensitive JDBC detail", new SQLException("Sensitive database detail", sqlState))))
            .when(paymentQueryService).findById(1L);
        doThrow(new TransactionSystemException("Sensitive transaction detail",
            new SQLException("Sensitive database detail", sqlState)))
            .when(paymentQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/payments/1", "/api/payments/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
        }
    }

    @Test
    @DisplayName("A retryable next SQLException is found through a generic application wrapper")
    void handlesConnectionFailureInNextSqlException() throws Exception {
        SQLException failure = new SQLException("Sensitive first SQL detail");
        failure.setNextException(new SQLException("Sensitive connection detail", "08006"));
        doThrow(new IllegalStateException("Sensitive application wrapper", failure))
            .when(paymentQueryService).findById(1L);

        assertError(getJson("/api/payments/1", 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
    }

    @ParameterizedTest
    @ValueSource(strings = {"28P01", "42P01"})
    @DisplayName("Permanent SQL errors remain 500 even inside unavailable-resource wrappers")
    void doesNotTreatPermanentDatabaseFailuresAsTemporary(String sqlState) throws Exception {
        doThrow(new CannotCreateTransactionException("Sensitive transaction detail",
            new SQLException("Sensitive permanent SQL detail", sqlState)))
            .when(paymentQueryService).findById(1L);
        doThrow(new DataAccessResourceFailureException("Sensitive resource detail",
            new SQLException("Sensitive permanent SQL detail", sqlState)))
            .when(paymentQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/payments/1", "/api/payments/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 500), "INTERNAL_ERROR", "Internal server error");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"22021", "23505"})
    @DisplayName("Database data errors are not mislabeled as temporary API unavailability")
    void doesNotTreatDatabaseDataFailuresAsTemporary(String sqlState) throws Exception {
        doThrow(new JpaSystemException(new org.hibernate.exception.GenericJDBCException(
            "Sensitive JDBC detail", new SQLException("Sensitive data error detail", sqlState))))
            .when(paymentQueryService).findById(1L);

        assertError(getJson("/api/payments/1", 500), "INTERNAL_ERROR", "Internal server error");
    }

    @Test
    @DisplayName("Connection-is-closed text alone does not determine database availability")
    void doesNotClassifyFailuresByTheirMessage() throws Exception {
        doThrow(new JpaSystemException(new org.hibernate.exception.GenericJDBCException(
            "Sensitive JDBC detail", new SQLException("Connection is closed")
        ))).when(paymentQueryService).findById(1L);
        doThrow(new IllegalStateException("Connection is closed"))
            .when(paymentQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/payments/1", "/api/payments/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 500), "INTERNAL_ERROR", "Internal server error");
        }
    }

    @Test
    @DisplayName("A JDBC transaction failure without SQLState returns 503 independently of its message")
    void handlesJdbcTransactionFailureWithoutSqlState() throws Exception {
        doThrow(new JpaSystemException(new org.hibernate.TransactionException(
            "Sensitive transaction detail", new SQLException("Arbitrary JDBC rollback failure")
        ))).when(paymentQueryService).findById(1L);

        assertError(getJson("/api/payments/1", 503), "SERVICE_UNAVAILABLE", "Service is not available right now");
    }

    @Test
    @DisplayName("Unexpected failure returns 500 without internal details")
    void handlesUnexpectedError() throws Exception {
        doThrow(new IllegalStateException("Sensitive detail about execution"))
            .when(paymentQueryService).findById(1L);
        doThrow(new JpaSystemException(new org.hibernate.TransactionException(
            "Sensitive detail about execution", new SQLException("Sensitive detail about SQL", "42601")
        ))).when(paymentQueryService).findByEventId(EVENT_ID);

        for (String path : List.of("/api/payments/1", "/api/payments/by-event-id/" + EVENT_ID)) {
            assertError(getJson(path, 500), "INTERNAL_ERROR", "Internal server error");
        }
    }

    @Test
    @DisplayName("Handler preserves the standard 405 status and Allow header")
    void keepsHttpStatus() throws Exception {
        MockHttpServletResponse response = mockMvc.perform(post("/api/payments/1").with(httpBasic("admin", "admin-test-password")).with(csrf()))
            .andExpect(status().isMethodNotAllowed())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn().getResponse();

        assertThat(response.getHeader(HttpHeaders.ALLOW)).contains("GET");
        assertError(objectMapper.readTree(response.getContentAsByteArray()),
            "HTTP_405", "Invalid request");
    }

    static Payment payment(ExecutionStatus paymentStatus) {
        return payment(paymentStatus, "alice");
    }

    static Payment payment(ExecutionStatus paymentStatus, String owner) {
        return ApiFixtures.payment(paymentStatus, owner);
    }

    private JsonNode getJson(String path, int expectedStatus) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().is(expectedStatus))
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn().getResponse();
        if ((expectedStatus == 200 || expectedStatus == 404) && path.startsWith("/api/payments/")) {
            assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        }
        return objectMapper.readTree(response.getContentAsByteArray());
    }

    private void assertPayment(JsonNode body, Payment payment) {
        assertThat(fields(body)).containsExactlyInAnyOrderElementsOf(PAYMENT_FIELDS);
        assertThat(body.path("id").isIntegralNumber()).isTrue();
        assertThat(body.path("id").asLong()).isEqualTo(payment.getId());
        assertThat(body.path("eventId").asText()).isEqualTo(payment.getEventId().toString());
        assertThat(body.path("paymentOperationId").asText()).isEqualTo(payment.getPaymentOperationId().toString());
        assertThat(body.path("orderId").asText()).isEqualTo(payment.getOrderId().toString());
        assertThat(body.path("currency").asText()).isEqualTo(payment.getCurrency());
        assertThat(body.path("paymentStatus").asText()).isEqualTo(payment.getPaymentStatus().name());
        assertThat(body.path("providerPaymentId").isNull()).isTrue();
        assertThat(body.path("actionExpiresAt").isNull()).isTrue();
        assertThat(body.path("version").isIntegralNumber()).isTrue();
        assertThat(body.path("version").longValue()).isEqualTo(payment.getVersion());
        assertThat(body.path("paymentMethodId").asText()).isEqualTo(payment.getPaymentMethodId());
        assertThat(body.path("amount").isIntegralNumber()).isTrue();
        assertThat(body.path("amount").asLong()).isEqualTo(payment.getAmount());
        assertThat(body.path("executionStatus").asText()).isEqualTo(payment.getExecutionStatus().name());
        assertThat(Instant.parse(body.path("createdAt").asText())).isEqualTo(payment.getCreatedAt());
        if (payment.getExecutionStatus() == ExecutionStatus.NEW) {
            assertThat(body.path("result").isNull()).isTrue();
            assertThat(body.path("startedAt").isNull()).isTrue();
            assertThat(body.path("finishedAt").isNull()).isTrue();
        } else {
            assertThat(body.path("result").asText()).isEqualTo(payment.getResult());
            assertThat(Instant.parse(body.path("startedAt").asText())).isEqualTo(payment.getStartedAt());
            assertThat(Instant.parse(body.path("finishedAt").asText())).isEqualTo(payment.getFinishedAt());
        }
    }

    static void assertError(JsonNode body, String code, String message) {
        assertThat(fields(body)).containsExactlyInAnyOrder("code", "message");
        assertThat(body.path("code").asText()).isEqualTo(code);
        assertThat(body.path("message").asText()).isEqualTo(message);
        assertThat(body.toString()).doesNotContain("Sensitive detail", "org.springframework", "SQLException");
    }

    private static List<String> fields(JsonNode object) {
        List<String> fields = new ArrayList<>();
        object.fieldNames().forEachRemaining(fields::add);
        return fields;
    }
}

@SpringBootTest(classes = ApiTestConfig.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false",
    "spring.datasource.hikari.connection-timeout=1000",
    "spring.datasource.hikari.validation-timeout=500",
    "spring.datasource.hikari.data-source-properties.connectTimeout=1",
    "spring.datasource.hikari.data-source-properties.socketTimeout=1"
})
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentDatabaseTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("Actual PostgreSQL outage returns 503 within a bounded time")
    void losesDatabase() throws Exception {
        Payment payment = paymentRepository.saveAndFlush(PaymentApiTest.payment(ExecutionStatus.NEW));
        String path = "/api/payments/" + payment.getId();
        mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password"))).andExpect(status().isOk());

        postgres.stop();

        assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
            MockHttpServletResponse response = mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andReturn().getResponse();
            PaymentApiTest.assertError(objectMapper.readTree(response.getContentAsByteArray()),
                "SERVICE_UNAVAILABLE", "Service is not available right now");
        });
    }
}
