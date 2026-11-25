package com.example.payment.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
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
class PaymentOpenApiTest {

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

    @Test
    @DisplayName("OpenAPI describes both routes, status codes, and response formats")
    void showsApi() throws Exception {
        JsonNode document = getJson("/v3/api-docs", 200);
        assertThat(document.path("openapi").asText()).startsWith("3.");
        for (String path : List.of("/api/payments/{id}", "/api/payments/by-event-id/{eventId}")) {
            JsonNode operation = document.path("paths").path(path).path("get");
            assertThat(operation.isObject()).isTrue();
            assertThat(operation.path("summary").asText()).isNotBlank();
            for (String code : List.of("200", "400", "401", "403", "404", "503", "500")) {
                JsonNode response = operation.path("responses").path(code);
                assertThat(response.path("description").asText()).isNotBlank();
                String model = code.equals("200") ? "PaymentResponseDTO" : "ApiError";
                assertThat(response.path("content").path("application/json")
                    .path("schema").path("$ref").asText())
                    .isEqualTo("#/components/schemas/" + model);
            }
        }

        JsonNode scheme = document.path("components").path("securitySchemes").path("basicAuth");
        assertThat(scheme.path("type").asText()).isEqualTo("http");
        assertThat(scheme.path("scheme").asText()).isEqualTo("basic");
        for (String path : List.of("/api/payments/{id}", "/api/payments/by-event-id/{eventId}")) {
            assertThat(document.path("paths").path(path).path("get").path("security").toString())
                .contains("basicAuth");
        }
        JsonNode retry = document.path("paths").path("/api/payments/{id}/retry").path("post");
        assertThat(retry.path("summary").asText()).isNotBlank();
        assertThat(retry.path("security").toString()).contains("basicAuth");
        assertThat(retry.path("responses").has("200")).isFalse();
        assertThat(retry.path("responses").path("204").path("description").asText()).isNotBlank();
        assertThat(retry.path("responses").path("204").path("content").isMissingNode()).isTrue();
        for (String code : List.of("400", "401", "403", "404", "409", "503", "500")) {
            assertThat(retry.path("responses").path(code).path("content").path("application/json")
                .path("schema").path("$ref").asText()).isEqualTo("#/components/schemas/ApiError");
        }

        JsonNode id = parameter(document, "/api/payments/{id}", "id");
        assertThat(id.path("required").asBoolean()).isTrue();
        assertThat(id.path("in").asText()).isEqualTo("path");
        JsonNode idSchema = id.path("schema");
        assertThat(hasType(idSchema, "integer")).isTrue();
        assertThat(idSchema.path("format").asText()).isEqualTo("int64");
        boolean minimumOne = idSchema.path("minimum").asDouble(Double.NaN) == 1;
        boolean exclusiveZero = idSchema.path("exclusiveMinimum").isNumber()
            && idSchema.path("exclusiveMinimum").asDouble() == 0;
        boolean oldExclusiveZero = idSchema.path("minimum").asDouble(Double.NaN) == 0
            && idSchema.path("exclusiveMinimum").asBoolean();
        assertThat(minimumOne || exclusiveZero || oldExclusiveZero).isTrue();

        JsonNode eventId = parameter(document, "/api/payments/by-event-id/{eventId}", "eventId");
        assertThat(eventId.path("required").asBoolean()).isTrue();
        assertThat(eventId.path("in").asText()).isEqualTo("path");
        assertThat(hasType(eventId.path("schema"), "string")).isTrue();
        assertThat(eventId.path("schema").path("format").asText()).isEqualTo("uuid");
        assertThat(document.path("paths").path("/api/payments/by-event-id/{eventId}")
            .path("get").path("description").asText()).contains("404", "waiting to be processed");

        JsonNode paymentSchema = document.path("components").path("schemas").path("PaymentResponseDTO");
        JsonNode properties = paymentSchema.path("properties");
        assertThat(fields(properties)).containsExactlyInAnyOrderElementsOf(PAYMENT_FIELDS);
        assertThat(texts(paymentSchema.path("required"))).containsExactlyInAnyOrderElementsOf(
            PAYMENT_FIELDS.stream().filter(field -> !field.equals("version")).toList());
        Map<String, String> types = Map.ofEntries(
            Map.entry("id", "integer"),
            Map.entry("eventId", "string"),
            Map.entry("paymentOperationId", "string"),
            Map.entry("orderId", "string"),
            Map.entry("amount", "integer"),
            Map.entry("currency", "string"),
            Map.entry("paymentMethodId", "string"),
            Map.entry("executionStatus", "string"),
            Map.entry("paymentStatus", "string"),
            Map.entry("providerPaymentId", "string"),
            Map.entry("result", "string"),
            Map.entry("createdAt", "string"),
            Map.entry("startedAt", "string"),
            Map.entry("finishedAt", "string"),
            Map.entry("actionExpiresAt", "string"),
            Map.entry("version", "integer")
        );
        for (Map.Entry<String, String> field : types.entrySet()) {
            assertThat(hasType(resolve(document, properties.path(field.getKey())), field.getValue()))
                .as(field.getKey()).isTrue();
        }
        assertThat(properties.path("id").path("format").asText()).isEqualTo("int64");
        assertThat(properties.path("eventId").path("format").asText()).isEqualTo("uuid");
        assertThat(properties.path("version").path("format").asText()).isEqualTo("int64");
        assertThat(properties.path("amount").path("format").asText()).isEqualTo("int64");
        assertThat(texts(resolve(document, properties.path("executionStatus")).path("enum")))
            .containsExactlyInAnyOrder("NEW", "IN_PROGRESS", "COMPLETED", "FAILED");
        for (String name : List.of("createdAt", "startedAt", "finishedAt", "actionExpiresAt")) {
            assertThat(properties.path(name).path("format").asText()).isEqualTo("date-time");
        }
        for (String name : List.of("providerPaymentId", "result", "startedAt", "finishedAt", "actionExpiresAt")) {
            JsonNode field = properties.path(name);
            assertThat(field.path("nullable").asBoolean() || hasType(field, "null")).as(name).isTrue();
        }

        JsonNode errorSchema = document.path("components").path("schemas").path("ApiError");
        assertThat(fields(errorSchema.path("properties"))).containsExactlyInAnyOrder("code", "message");
        assertThat(texts(errorSchema.path("required"))).containsExactlyInAnyOrder("code", "message");
        assertThat(hasType(errorSchema.path("properties").path("code"), "string")).isTrue();
        assertThat(hasType(errorSchema.path("properties").path("message"), "string")).isTrue();
    }

    private JsonNode getJson(String path, int expectedStatus) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(get(path).with(httpBasic("admin", "admin-test-password")).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().is(expectedStatus))
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn().getResponse();
        return objectMapper.readTree(response.getContentAsByteArray());
    }

    private static List<String> fields(JsonNode object) {
        List<String> fields = new ArrayList<>();
        object.fieldNames().forEachRemaining(fields::add);
        return fields;
    }

    private List<String> texts(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(value -> values.add(value.asText()));
        return values;
    }

    private JsonNode parameter(JsonNode document, String path, String name) {
        for (JsonNode parameter : document.path("paths").path(path).path("get").path("parameters")) {
            if (parameter.path("name").asText().equals(name)) {
                return parameter;
            }
        }
        throw new AssertionError("Parameter was not found: " + name);
    }

    private JsonNode resolve(JsonNode document, JsonNode schema) {
        String reference = schema.path("$ref").asText();
        return reference.startsWith("#/") ? document.at(reference.substring(1)) : schema;
    }

    private boolean hasType(JsonNode schema, String expected) {
        JsonNode type = schema.path("type");
        if (type.isTextual()) {
            return type.asText().equals(expected);
        }
        for (JsonNode value : type) {
            if (value.asText().equals(expected)) {
                return true;
            }
        }
        return false;
    }

}
