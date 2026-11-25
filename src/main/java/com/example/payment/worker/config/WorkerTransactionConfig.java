package com.example.payment.worker.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import com.example.payment.worker.WorkerProperties;

@Configuration
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerTransactionConfig {

    @Bean
    public TransactionTemplate workerTransactionTemplate(
        PlatformTransactionManager transactionManager,
        WorkerProperties workerProperties
    ) {
        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        template.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        template.setTimeout(workerProperties.claimTimeoutSeconds());
        return template;
    }
}
