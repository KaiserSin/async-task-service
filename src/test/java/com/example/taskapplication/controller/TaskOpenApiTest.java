package com.example.taskapplication.controller;

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
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TaskApiTest.Config.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false"
})
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class TaskOpenApiTest {

    private static final List<String> TASK_FIELDS = List.of(
        "id", "eventId", "name", "duration", "status", "progress", "result",
        "createdAt", "startedAt", "finishedAt"
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
        for (String path : List.of("/api/tasks/{id}", "/api/tasks/by-event-id/{eventId}")) {
            JsonNode operation = document.path("paths").path(path).path("get");
            assertThat(operation.isObject()).isTrue();
            assertThat(operation.path("summary").asText()).isNotBlank();
            for (String code : List.of("200", "400", "404", "503", "500")) {
                JsonNode response = operation.path("responses").path(code);
                assertThat(response.path("description").asText()).isNotBlank();
                String model = code.equals("200") ? "TaskResponseDTO" : "ErrorResponse";
                assertThat(response.path("content").path("application/json")
                    .path("schema").path("$ref").asText())
                    .isEqualTo("#/components/schemas/" + model);
            }
        }

        JsonNode id = parameter(document, "/api/tasks/{id}", "id");
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

        JsonNode eventId = parameter(document, "/api/tasks/by-event-id/{eventId}", "eventId");
        assertThat(eventId.path("required").asBoolean()).isTrue();
        assertThat(eventId.path("in").asText()).isEqualTo("path");
        assertThat(hasType(eventId.path("schema"), "string")).isTrue();
        assertThat(eventId.path("schema").path("format").asText()).isEqualTo("uuid");
        assertThat(document.path("paths").path("/api/tasks/by-event-id/{eventId}")
            .path("get").path("description").asText()).contains("404", "waiting to be processed");

        JsonNode taskSchema = document.path("components").path("schemas").path("TaskResponseDTO");
        JsonNode properties = taskSchema.path("properties");
        assertThat(fields(properties)).containsExactlyInAnyOrderElementsOf(TASK_FIELDS);
        assertThat(texts(taskSchema.path("required"))).containsExactlyInAnyOrderElementsOf(TASK_FIELDS);
        Map<String, String> types = Map.of(
            "id", "integer", "eventId", "string", "name", "string", "duration", "integer",
            "status", "string", "progress", "integer", "result", "string",
            "createdAt", "string", "startedAt", "string", "finishedAt", "string"
        );
        for (Map.Entry<String, String> field : types.entrySet()) {
            assertThat(hasType(resolve(document, properties.path(field.getKey())), field.getValue()))
                .as(field.getKey()).isTrue();
        }
        assertThat(properties.path("id").path("format").asText()).isEqualTo("int64");
        assertThat(properties.path("eventId").path("format").asText()).isEqualTo("uuid");
        assertThat(properties.path("duration").path("format").asText()).isEqualTo("int64");
        assertThat(properties.path("duration").path("description").asText()).contains("milliseconds");
        assertThat(texts(resolve(document, properties.path("status")).path("enum")))
            .containsExactlyInAnyOrder("NEW", "IN_PROGRESS", "COMPLETED", "FAILED");
        for (String name : List.of("createdAt", "startedAt", "finishedAt")) {
            assertThat(properties.path(name).path("format").asText()).isEqualTo("date-time");
        }
        for (String name : List.of("result", "startedAt", "finishedAt")) {
            JsonNode field = properties.path(name);
            assertThat(field.path("nullable").asBoolean() || hasType(field, "null")).as(name).isTrue();
        }

        JsonNode errorSchema = document.path("components").path("schemas").path("ErrorResponse");
        assertThat(fields(errorSchema.path("properties"))).containsExactlyInAnyOrder("code", "message");
        assertThat(texts(errorSchema.path("required"))).containsExactlyInAnyOrder("code", "message");
        assertThat(hasType(errorSchema.path("properties").path("code"), "string")).isTrue();
        assertThat(hasType(errorSchema.path("properties").path("message"), "string")).isTrue();
    }

    private JsonNode getJson(String path, int expectedStatus) throws Exception {
        MockHttpServletResponse response = mockMvc.perform(get(path).accept(MediaType.APPLICATION_JSON))
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
