package com.example.taskapplication.health;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component("taskKafka")
public class KafkaListenerHealthCheck extends AbstractHealthIndicator {

    private final KafkaListenerEndpointRegistry listenerRegistry;

    public KafkaListenerHealthCheck(KafkaListenerEndpointRegistry listenerRegistry, MeterRegistry meterRegistry) {
        this.listenerRegistry = listenerRegistry;
        Gauge.builder("task.kafka.listener.ready", this,
                health -> health.allReady(health.listenerRegistry.getListenerContainers()) ? 1 : 0)
            .description("1 when all Kafka listener containers are running and not paused; otherwise 0")
            .register(meterRegistry);
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        var containers = listenerRegistry.getListenerContainers();
        if (allReady(containers)) {
            builder.up();
        } else {
            builder.down();
        }
        builder.withDetail("containers", containers.size());
    }

    private boolean allReady(Collection<? extends MessageListenerContainer> containers) {
        return !containers.isEmpty() && containers.stream().allMatch(this::isReady);
    }

    private boolean isReady(MessageListenerContainer container) {
        if (!container.isRunning() || container.isPauseRequested() || container.isContainerPaused()) {
            return false;
        }
        if (container instanceof ConcurrentMessageListenerContainer<?, ?> concurrent) {
            return allReady(concurrent.getContainers());
        }
        return true;
    }
}
