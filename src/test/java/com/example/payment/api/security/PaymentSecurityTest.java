package com.example.payment.api.security;

import com.example.payment.domain.Payment;
import com.example.payment.domain.ExecutionStatus;
import com.example.payment.domain.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.http.HttpHeaders;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import com.example.payment.api.ApiTestConfig;
import com.example.payment.api.ApiFixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.util.ReflectionTestUtils.setField;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = ApiTestConfig.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false"
})
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PaymentSecurityTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private PaymentRepository paymentRepository;
    @Autowired
    private ObjectMapper mapper;

    @BeforeEach
    void clearPayments() {
        paymentRepository.deleteAllInBatch();
    }

    @Test
    @DisplayName("Both lookup routes reject anonymous and incorrect Basic credentials")
    void authenticatesBeforeDisclosingPayments() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.NEW, "alice");
        for (String path : new String[]{byId(payment), byEvent(payment)}) {
            MockHttpServletResponse anonymous = mockMvc.perform(get(path))
                .andExpect(status().isUnauthorized()).andReturn().getResponse();
            assertThat(anonymous.getHeader(HttpHeaders.WWW_AUTHENTICATE)).contains("Basic");
            assertCode(anonymous, "UNAUTHORIZED");
            mockMvc.perform(get(path).with(httpBasic("alice", "wrong-password")))
                .andExpect(status().isUnauthorized());
            mockMvc.perform(get(path).with(httpBasic("missing", "alice-test-password")))
                .andExpect(status().isUnauthorized());
        }
    }

    @Test
    @DisplayName("A user reads their own payment through either lookup route")
    void readsOwnPayment() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.NEW, "alice");
        for (String path : new String[]{byId(payment), byEvent(payment)}) {
            MockHttpServletResponse response = mockMvc.perform(get(path)
                    .with(httpBasic("alice", "alice-test-password")))
                .andExpect(status().isOk()).andReturn().getResponse();
            assertThat(mapper.readTree(response.getContentAsByteArray()).path("id").asLong())
                .isEqualTo(payment.getId());
            assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        }
    }

    @Test
    @DisplayName("Another user's payment is indistinguishable from a missing payment")
    void hidesForeignPayments() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.NEW, "alice");
        for (String path : new String[]{byId(payment), byEvent(payment)}) {
            MockHttpServletResponse response = mockMvc.perform(get(path)
                    .with(httpBasic("bob", "bob-test-password")))
                .andExpect(status().isNotFound()).andReturn().getResponse();
            assertCode(response, "PAYMENT_NOT_FOUND");
            assertThat(response.getContentAsString()).doesNotContain("alice", "owner", "forbidden");
        }
    }

    @Test
    @DisplayName("Only an administrator reads payments belonging to an inactive owner")
    void restrictsInactiveOwnerPaymentsToAdmin() throws Exception {
        Payment legacy = ownedPayment(ExecutionStatus.NEW, "inactive-owner");
        for (String path : new String[]{byId(legacy), byEvent(legacy)}) {
            mockMvc.perform(get(path).with(httpBasic("alice", "alice-test-password")))
                .andExpect(status().isNotFound());
            mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")))
                .andExpect(status().isOk());
        }
    }

    @Test
    @DisplayName("An administrator reads another user's payment")
    void adminReadsAnyOwner() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.NEW, "alice");
        mockMvc.perform(get(byId(payment)).with(httpBasic("admin", "admin-test-password")))
            .andExpect(status().isOk());
    }

    @Test
    @DisplayName("A principal without USER or ADMIN cannot access payment endpoints")
    void requiresAnApplicationRole() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.NEW, "intruder");
        MockHttpServletResponse response = mockMvc.perform(get(byId(payment))
                .with(user("intruder").roles("OTHER")))
            .andExpect(status().isForbidden()).andReturn().getResponse();
        assertCode(response, "FORBIDDEN");
    }

    @Test
    @DisplayName("An owner retries FAILED once and gets a fresh attempt budget")
    void retriesOwnFailedPayment() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.FAILED, "alice");

        MockHttpServletResponse response = mockMvc.perform(post(retry(payment))
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isNoContent()).andReturn().getResponse();

        assertThat(response.getContentAsByteArray()).isEmpty();
        assertThat(response.getHeader(HttpHeaders.CACHE_CONTROL)).isEqualTo("no-store");
        Payment queued = find(payment);
        assertThat(queued.getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
        assertThat(queued.getOwnerUsername()).isEqualTo("alice");
        assertThat(queued.getAttempts()).isZero();
        assertThat(queued.getExecutionId()).isNull();
        assertThat(queued.getLeaseUntil()).isNull();
        assertThat(queued.getNextRetryAt()).isNull();
        assertThat(queued.getStartedAt()).isNull();
        assertThat(queued.getFinishedAt()).isNull();
        assertThat(queued.getResult()).isNull();
        assertThat(queued.getVersion()).isEqualTo(payment.getVersion() + 1);
        MockHttpServletResponse repeated = mockMvc.perform(post(retry(payment))
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isConflict()).andReturn().getResponse();
        assertCode(repeated, "PAYMENT_NOT_RETRYABLE");
        assertThat(find(payment)).usingRecursiveComparison().isEqualTo(queued);
    }

    @ParameterizedTest
    @ValueSource(strings = {"alice", "inactive-owner"})
    @DisplayName("An administrator retries FAILED payments for active and inactive owners")
    void adminRetriesAnyOwner(String owner) throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.FAILED, owner);
        mockMvc.perform(post(retry(payment)).with(httpBasic("admin", "admin-test-password")).with(csrf()))
            .andExpect(status().isNoContent());
        assertThat(find(payment).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
    }

    @ParameterizedTest
    @EnumSource(value = ExecutionStatus.class, names = {"NEW", "IN_PROGRESS", "COMPLETED"})
    @DisplayName("A manual retry cannot reset a payment that is not FAILED")
    void rejectsOtherStates(ExecutionStatus state) throws Exception {
        Payment payment = ownedPayment(state, "alice");
        MockHttpServletResponse response = mockMvc.perform(post(retry(payment))
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isConflict()).andReturn().getResponse();
        assertCode(response, "PAYMENT_NOT_RETRYABLE");
        assertThat(find(payment)).usingRecursiveComparison().isEqualTo(payment);
    }

    @Test
    @DisplayName("Anonymous, foreign-owner and missing-CSRF retries leave the payment unchanged")
    void doesNotMutateUnauthorizedRetries() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.FAILED, "alice");
        mockMvc.perform(post(retry(payment)).with(csrf())).andExpect(status().isUnauthorized());
        mockMvc.perform(post(retry(payment)).with(httpBasic("bob", "bob-test-password")).with(csrf()))
            .andExpect(status().isNotFound());
        mockMvc.perform(post(retry(payment)).with(httpBasic("alice", "alice-test-password")))
            .andExpect(status().isForbidden());
        assertThat(find(payment)).usingRecursiveComparison().isEqualTo(payment);
    }

    @Test
    @DisplayName("The CSRF endpoint provides a token usable with the same session and Basic credentials")
    void retriesWithARealCsrfToken() throws Exception {
        Payment payment = ownedPayment(ExecutionStatus.FAILED, "alice");
        var result = mockMvc.perform(get("/api/csrf").with(httpBasic("alice", "alice-test-password")))
            .andExpect(status().isOk()).andReturn();
        JsonNode token = mapper.readTree(result.getResponse().getContentAsByteArray());
        MockHttpSession session = (MockHttpSession) result.getRequest().getSession(false);
        assertThat(session).isNotNull();
        assertThat(token.path("token").asText()).isNotBlank();

        mockMvc.perform(get(byId(payment)).session(session))
            .andExpect(status().isUnauthorized());
        mockMvc.perform(post(retry(payment)).session(session)
                .header(token.path("headerName").asText(), token.path("token").asText()))
            .andExpect(status().isUnauthorized());
        assertThat(find(payment).getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);

        mockMvc.perform(post(retry(payment)).with(httpBasic("alice", "alice-test-password"))
                .session(session).header(token.path("headerName").asText(), token.path("token").asText()))
            .andExpect(status().isNoContent());
        assertThat(find(payment).getExecutionStatus()).isEqualTo(ExecutionStatus.NEW);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "abc"})
    @DisplayName("The retry endpoint validates its payment ID")
    void validatesRetryId(String id) throws Exception {
        mockMvc.perform(post("/api/payments/" + id + "/retry")
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("A manual retry of a missing payment returns 404")
    void rejectsMissingRetry() throws Exception {
        mockMvc.perform(post("/api/payments/9223372036854775807/retry")
                .with(httpBasic("alice", "alice-test-password")).with(csrf()))
            .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("Health is public; documentation and management require ADMIN; unspecified routes stay denied")
    void restrictsManagementAndDocumentation() throws Exception {
        mockMvc.perform(get("/actuator/health")).andExpect(status().isOk());
        for (String path : new String[]{"/v3/api-docs", "/actuator/metrics"}) {
            mockMvc.perform(get(path)).andExpect(status().isUnauthorized());
            mockMvc.perform(get(path).with(httpBasic("alice", "alice-test-password")))
                .andExpect(status().isForbidden());
            mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")))
                .andExpect(status().isOk());
        }
        mockMvc.perform(get("/unspecified").with(httpBasic("admin", "admin-test-password")))
            .andExpect(status().isForbidden());
    }

    private Payment ownedPayment(ExecutionStatus status, String owner) {
        Payment payment = ApiFixtures.payment(status == ExecutionStatus.NEW ? ExecutionStatus.NEW : ExecutionStatus.COMPLETED, owner);
        setField(payment, "executionStatus", status);
        if (status == ExecutionStatus.FAILED) {
            setField(payment, "attempts", 3);
            setField(payment, "result", "Payment failed");
        } else if (status == ExecutionStatus.IN_PROGRESS) {
            setField(payment, "finishedAt", null);
        }
        return paymentRepository.saveAndFlush(payment);
    }

    private Payment find(Payment payment) {
        return paymentRepository.findById(payment.getId()).orElseThrow();
    }

    private String byId(Payment payment) {
        return "/api/payments/" + payment.getId();
    }

    private String byEvent(Payment payment) {
        return "/api/payments/by-event-id/" + payment.getEventId();
    }

    private String retry(Payment payment) {
        return byId(payment) + "/retry";
    }

    private void assertCode(MockHttpServletResponse response, String expected) throws Exception {
        assertThat(mapper.readTree(response.getContentAsByteArray()).path("code").asText()).isEqualTo(expected);
    }
}
