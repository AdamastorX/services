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
 *
 * <p><strong>Also restores the untyped {@code KafkaTemplate<Object, Object>}
 * bean</strong> ({@link #kafkaTemplate}) that Boot's own
 * {@code KafkaAutoConfiguration} would otherwise provide. Found the hard
 * way via this repo's own CI: {@code
 * @ConditionalOnMissingBean(KafkaTemplate.class)} matches on raw type,
 * ignoring generics -- the moment this class's own typed {@code
 * clinVarIngestionKafkaTemplate} bean exists anywhere in {@code workers}'
 * context, Boot's conditional backs off and stops creating its untyped
 * one, even though the two beans have unrelated generic parameters.
 * {@code WorkItemConsumerConfig.errorHandler}'s {@code
 * DeadLetterPublishingRecoverer} depends on exactly that untyped {@code
 * KafkaOperations<Object, Object>} bean (it has to be able to republish
 * *any* failed record type to a DLT, not just {@code WorkItem}s) -- it
 * broke the moment this class was added, since {@code workers} previously
 * had zero {@code KafkaTemplate} beans of its own and always got Boot's
 * default for free. Providing it explicitly here (same bean name Boot
 * would have used, {@code "kafkaTemplate"}) makes {@code workers} self-
 * sufficient regardless of how many other typed producers it grows.
 */
@Configuration
public class ClinVarIngestionProducerConfig {

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties()));
    }

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
