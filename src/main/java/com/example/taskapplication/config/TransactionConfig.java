package com.example.taskapplication.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
public class TransactionConfig {

    @Bean
    public TransactionTemplate claimTransactionTemplate(
        PlatformTransactionManager transactionManager,
        @Value("${worker.claim-timeout-seconds:5}") int claimTimeoutSeconds
    ) {
        if (claimTimeoutSeconds < 1) {
            throw new IllegalArgumentException("worker.claim-timeout-seconds must be greater than 0");
        }
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        template.setTimeout(claimTimeoutSeconds);
        return template;
    }
}
