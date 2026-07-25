package com.adamastorx.workers.clinvar;

import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Typed {@code KafkaTemplate<String, ClinVarIngestionCompletedEvent>},
 * same rationale and shape as {@code api}'s {@code WorkItemProducerConfig}
 * (ADR 0011): Boot's auto-configured template is untyped. This is
 * {@code workers}' first outbound Kafka producer -- until now it has only
 * ever been a consumer ({@code WorkItemConsumerConfig}).
 */
@Configuration
public class ClinVarIngestionProducerConfig {

    @Bean
    public ProducerFactory<String, ClinVarIngestionCompletedEvent> clinVarIngestionProducerFactory(
            KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, ClinVarIngestionCompletedEvent> clinVarIngestionKafkaTemplate(
            ProducerFactory<String, ClinVarIngestionCompletedEvent> clinVarIngestionProducerFactory) {
        KafkaTemplate<String, ClinVarIngestionCompletedEvent> template =
                new KafkaTemplate<>(clinVarIngestionProducerFactory);
        // Same observability#1/ADR 0013 reasoning as WorkItemProducerConfig:
        // a hand-built KafkaTemplate bypasses Boot's
        // spring.kafka.template.observation-enabled autoconfiguration
        // entirely and needs this set directly, or send() produces no span
        // and propagates no trace context into the record headers.
        template.setObservationEnabled(true);
        return template;
    }
}
