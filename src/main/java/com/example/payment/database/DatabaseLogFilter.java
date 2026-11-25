package com.example.payment.database;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.helpers.MessageFormatter;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Optional;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DatabaseLogFilter extends TurboFilter {

    private static final ThreadLocal<Optional<UUID>> backgroundOperation = new ThreadLocal<>();

    private final DatabaseFailureClassifier classifier;

    public static Runnable backgroundTask(Runnable task) {
        return () -> {
            backgroundOperation.set(Optional.empty());
            try {
                task.run();
            } finally {
                backgroundOperation.remove();
            }
        };
    }

    public static void setBackgroundOperation(UUID operationId) {
        if (backgroundOperation.get() != null) {
            backgroundOperation.set(Optional.ofNullable(operationId));
        }
    }

    @PostConstruct
    void register() {
        var context = (LoggerContext) LoggerFactory.getILoggerFactory();
        setContext(context);
        start();
        context.addTurboFilter(this);
    }

    @PreDestroy
    void unregister() {
        ((LoggerContext) getContext()).getTurboFilterList().remove(this);
        stop();
    }

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] args, Throwable error) {
        var operation = backgroundOperation.get();
        if (logger.getName().equals("org.hibernate.engine.jdbc.spi.SqlExceptionHelper")
                && (operation != null || RequestContextHolder.getRequestAttributes() != null
                    || KafkaUtils.getConsumerGroupId() != null)) {
            return FilterReply.DENY;
        }
        if (error != null && logger.getName().startsWith("org.springframework.kafka.")
                && classifier.classify(error) != DatabaseFailureClassifier.Action.NOT_DATABASE) {
            return FilterReply.DENY;
        }
        if (operation != null) {
            Throwable cause = error != null ? error : MessageFormatter.getThrowableCandidate(args);
            if (cause != null) {
                var failure = classifier.inspect(cause);
                if (failure.action() != DatabaseFailureClassifier.Action.NOT_DATABASE) {
                    logger.log(null, DatabaseLogFilter.class.getName(), Level.toLocationAwareLoggerInteger(level),
                        "Database error " + failure.logDetails(operation.orElse(null)), null, null);
                    return FilterReply.DENY;
                }
            }
        }
        return FilterReply.NEUTRAL;
    }
}
