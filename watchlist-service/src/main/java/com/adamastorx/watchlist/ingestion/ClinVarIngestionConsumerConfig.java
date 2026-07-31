package com.adamastorx.watchlist.ingestion;

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
 * Same hand-built, typed container factory shape as api's own {@code
 * ClinVarCacheInvalidationConsumerConfig} (ADR 0011's documented reason:
 * Boot's auto-configured factory is untyped). {@code group-id} defaults to
 * {@code spring.application.name} ({@code watchlist-service}, see
 * application.yml) -- a distinct consumer group from api's {@code api}
 * group on the exact same topic is what makes this a second, genuinely
 * independent consumer (backlog #53's stated purpose) rather than a
 * replacement or a competing consumer within api's own group.
 *
 * <p>The {@link DeadLetterPublishingRecoverer}/{@link DefaultErrorHandler}
 * below is the *Kafka listener's own* error path (a message this listener's
 * code cannot process at all, e.g. malformed JSON or a bug in {@link
 * DeliveryResolutionService}) -- entirely separate from {@link
 * com.adamastorx.watchlist.delivery.NotificationRelay}'s per-subscriber
 * dead-letter (a specific subscriber's own ntfy target permanently
 * failing). Two different failure classes, two different DLQs; conflating
 * them would mean one bad subscriber's target could block every other
 * subscriber's fan-out, which is exactly what backlog #53's AC rules out.
 */
@Configuration
public class ClinVarIngestionConsumerConfig {

    private static final long RETRY_BACKOFF_MILLIS = 1_000L;
    private static final long RETRY_ATTEMPTS_AFTER_FIRST_FAILURE = 2L;
    private static final String EVENT_PACKAGE = "com.adamastorx.watchlist.ingestion";

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(KafkaProperties kafkaProperties) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties()));
    }

    @Bean
    public DefaultErrorHandler clinVarIngestionErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations);
        FixedBackOff backOff = new FixedBackOff(RETRY_BACKOFF_MILLIS, RETRY_ATTEMPTS_AFTER_FIRST_FAILURE);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConsumerFactory<String, ClinVarIngestionCompletedEvent> clinVarIngestionEventConsumerFactory(
            KafkaProperties kafkaProperties) {
        JsonDeserializer<ClinVarIngestionCompletedEvent> valueDeserializer =
                new JsonDeserializer<>(ClinVarIngestionCompletedEvent.class, false);
        valueDeserializer.addTrustedPackages(EVENT_PACKAGE);
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(), new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ClinVarIngestionCompletedEvent>
            clinVarIngestionEventKafkaListenerContainerFactory(
                    ConsumerFactory<String, ClinVarIngestionCompletedEvent> clinVarIngestionEventConsumerFactory,
                    DefaultErrorHandler clinVarIngestionErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, ClinVarIngestionCompletedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(clinVarIngestionEventConsumerFactory);
        factory.setCommonErrorHandler(clinVarIngestionErrorHandler);
        // MANUAL_IMMEDIATE, same as api's own consumer -- the listener below
        // only acknowledges after DeliveryResolutionService's transaction
        // commits the PENDING delivery rows, not before. That ordering is
        // the actual "event consumed" checkpoint the crash test targets: ack
        // happens after durability, never before.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
