package com.adamastorx.api.clinvar;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Same shape as {@code workers.workitem.WorkItemConsumerConfig} (ADR
 * 0011) -- {@code api}'s first Kafka consumer (it has only ever produced,
 * to {@code work-items}, until services#26). A hand-built, typed
 * container factory for the same reason as that class: Boot's
 * auto-configured one is untyped, and this Boot 4.1 line doesn't reliably
 * propagate {@code spring.kafka.listener.ack-mode} onto an
 * auto-configured factory (found the hard way on {@code workers}'
 * consumer, services#3 -- see that class's javadoc for the full story).
 *
 * <p><strong>Also restores the untyped {@code KafkaTemplate<Object, Object>}
 * bean</strong> ({@link #kafkaTemplate}) Boot's own {@code
 * KafkaAutoConfiguration} would otherwise provide. {@code
 * WorkItemProducerConfig} already defines its own typed {@code
 * KafkaTemplate<String, WorkItem>} bean (services#2/#3), which made
 * Boot's {@code @ConditionalOnMissingBean(KafkaTemplate.class)} back off
 * from creating the untyped default -- that condition matches on raw
 * type, ignoring generics. This never mattered before because nothing in
 * {@code api} needed {@code KafkaOperations<Object, Object>}; {@link
 * #clinVarCacheInvalidationErrorHandler}'s {@code
 * DeadLetterPublishingRecoverer} below is the first thing that does
 * (found via this repo's own CI, same failure mode as {@code workers}'
 * identical fix in {@code ClinVarIngestionProducerConfig}).
 */
@Configuration
public class ClinVarCacheInvalidationConsumerConfig {

    private static final long RETRY_BACKOFF_MILLIS = 1_000L;
    private static final long RETRY_ATTEMPTS_AFTER_FIRST_FAILURE = 2L;
    private static final String CLINVAR_EVENT_PACKAGE = "com.adamastorx.api.clinvar";

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties()));
    }

    @Bean
    public DefaultErrorHandler clinVarCacheInvalidationErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations);
        FixedBackOff backOff = new FixedBackOff(RETRY_BACKOFF_MILLIS, RETRY_ATTEMPTS_AFTER_FIRST_FAILURE);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConsumerFactory<String, ClinVarIngestionCompletedEvent> clinVarIngestionEventConsumerFactory(
            KafkaProperties kafkaProperties) {
        JsonDeserializer<ClinVarIngestionCompletedEvent> valueDeserializer =
                new JsonDeserializer<>(ClinVarIngestionCompletedEvent.class, false);
        valueDeserializer.addTrustedPackages(CLINVAR_EVENT_PACKAGE);
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(), new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ClinVarIngestionCompletedEvent>
            clinVarIngestionEventKafkaListenerContainerFactory(
                    ConsumerFactory<String, ClinVarIngestionCompletedEvent> clinVarIngestionEventConsumerFactory,
                    DefaultErrorHandler clinVarCacheInvalidationErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, ClinVarIngestionCompletedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(clinVarIngestionEventConsumerFactory);
        factory.setCommonErrorHandler(clinVarCacheInvalidationErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
