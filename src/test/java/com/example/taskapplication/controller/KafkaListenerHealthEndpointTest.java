package com.example.taskapplication.controller;

import com.example.taskapplication.health.KafkaListenerHealthCheck;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = TaskApiTest.Config.class, properties = {
    "spring.jpa.hibernate.ddl-auto=validate",
    "spring.datasource.hikari.register-mbeans=false"
})
@Import(KafkaListenerHealthCheck.class)
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class KafkaListenerHealthEndpointTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @MockitoBean
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private MeterRegistry meterRegistry;
    @Autowired
    private ObjectMapper mapper;

    private MessageListenerContainer listener;

    @BeforeEach
    void setUp() {
        listener = mock(MessageListenerContainer.class);
        when(listenerRegistry.getListenerContainers()).thenReturn(List.of(listener));
    }

    @Test
    void stoppedListenerMakesHealthUnavailableWithoutFailingLiveness() throws Exception {
        when(listener.isRunning()).thenReturn(true);
        assertAvailability(true);

        when(listener.isRunning()).thenReturn(false);
        assertAvailability(false);

        when(listener.isRunning()).thenReturn(true);
        assertAvailability(true);
    }

    @Test
    void pausedListenerMakesHealthUnavailableUntilItActuallyResumes() throws Exception {
        when(listener.isRunning()).thenReturn(true);
        when(listener.isPauseRequested()).thenReturn(true);
        assertAvailability(false);

        when(listener.isPauseRequested()).thenReturn(false);
        when(listener.isContainerPaused()).thenReturn(true);
        assertAvailability(false);

        when(listener.isContainerPaused()).thenReturn(false);
        assertAvailability(true);
    }

    private void assertAvailability(boolean ready) throws Exception {
        int expectedStatus = ready ? 200 : 503;
        for (String path : List.of("/actuator/health", "/actuator/health/readiness")) {
            mockMvc.perform(get(path))
                .andExpect(status().is(expectedStatus))
                .andExpect(jsonPath("$.status").value(ready ? "UP" : "DOWN"))
                .andExpect(jsonPath("$.components").doesNotExist())
                .andExpect(jsonPath("$.details").doesNotExist());
        }
        mockMvc.perform(get("/actuator/health/liveness"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("UP"));

        double expectedGauge = ready ? 1 : 0;
        assertThat(meterRegistry.get("task.kafka.listener.ready").gauge().value()).isEqualTo(expectedGauge);
        var response = mockMvc.perform(get("/actuator/metrics/task.kafka.listener.ready")
                .with(httpBasic("admin", "admin-test-password")))
            .andExpect(status().isOk()).andReturn().getResponse();
        var metric = mapper.readTree(response.getContentAsByteArray());
        assertThat(metric.path("measurements").get(0).path("value").asDouble()).isEqualTo(expectedGauge);
    }
}
