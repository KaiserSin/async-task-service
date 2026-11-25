package com.example.payment.api;

import com.example.payment.database.DatabaseFailureClassifier;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentRepository;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import com.example.payment.api.error.GlobalExceptionHandler;
import com.example.payment.api.security.PaymentAccessService;
import com.example.payment.api.security.SecurityConfig;

/**
 * Loads the REST API, security rules and database for tests.
 * API, management port and health tests share this setup and can run without Kafka, workers or Stripe.
 */
@Configuration(proxyBeanMethods = false)
@TestComponent
@EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
@EntityScan(basePackageClasses = Payment.class)
@EnableJpaRepositories(basePackageClasses = PaymentRepository.class)
@Import({PaymentController.class, CsrfController.class, GlobalExceptionHandler.class, PaymentQueryService.class,
    PaymentActionService.class, PaymentAccessService.class, SecurityConfig.class, DatabaseFailureClassifier.class})
public class ApiTestConfig {
}
