package com.adamastorx.workers.workitem;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The consumer side of ADR 0011, built explicitly (same rationale as
 * {@code api}'s {@code WorkItemProducerConfig}): Boot's auto-configured
 * {@code kafkaListenerContainerFactory} is untyped ({@code Object,Object})
 * and, in this Boot 4.1 line, doesn't reliably propagate
 * {@code spring.kafka.listener.ack-mode} onto the container it builds --
 * {@code @KafkaListener}'s manual-ack {@link WorkItemListener#onMessage}
 * threw {@code IllegalStateException} ("No Acknowledgment available")
 * instead of getting one. Setting {@link ContainerProperties#setAckMode}
 * directly here removes that dependency on property auto-wiring; see
 * {@link WorkItemListener} for how the container factory is referenced.
 *
 * <p>The ack mode has to be {@link ContainerProperties.AckMode#MANUAL_IMMEDIATE}
 * (or {@code MANUAL}) -- {@link WorkItemListener#onMessage} takes an
 * {@code Acknowledgment} parameter and calls {@code acknowledge()} itself.
 * The default {@code RECORD} mode commits automatically after the listener
 * returns and doesn't support an explicit {@code acknowledge()} call; doing
 * so throws {@code IllegalStateException}, which the error handler treated
 * as a processing failure -- so every record was "failing", retried, then
 * dead-lettered without ever really being processed.
 */
@Configuration
public class WorkItemConsumerConfig {

    /** 3 attempts total (1 initial + 2 retries), 1s fixed backoff (ADR 0011). */
    private static final long RETRY_BACKOFF_MILLIS = 1_000L;

    private static final long RETRY_ATTEMPTS_AFTER_FIRST_FAILURE = 2L;

    private static final String WORK_ITEM_PACKAGE = "com.adamastorx.workers.workitem";

    /**
     * Bounded retry then dead-letter (ADR 0011): a processing failure gets
     * retried up to twice more, then the record is published to
     * {@code work-items.DLT} ({@link DeadLetterPublishingRecoverer}'s
     * default naming) instead of being dropped. Rejected the non-blocking
     * {@code @RetryableTopic} cascade (several auto-created retry topics +
     * consumer groups) as disproportionate machinery for one dev topic.
     *
     * <p>Scope note: this covers listener processing exceptions, per ADR
     * 0011's explicit ask. It does not wrap the consumer's deserializer
     * with {@code ErrorHandlingDeserializer} for malformed payloads -- the
     * AC is "a message produced by API is consumed by a worker", not
     * resilience to hostile/corrupt input, and both ends of this topic are
     * owned in this same repo. Flagged here as a reasonable future
     * hardening step, not built now, to avoid gold-plating a scaffold.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations);
        FixedBackOff backOff = new FixedBackOff(RETRY_BACKOFF_MILLIS, RETRY_ATTEMPTS_AFTER_FIRST_FAILURE);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConsumerFactory<String, WorkItem> workItemConsumerFactory(KafkaProperties kafkaProperties) {
        JsonDeserializer<WorkItem> valueDeserializer = new JsonDeserializer<>(WorkItem.class, false);
        valueDeserializer.addTrustedPackages(WORK_ITEM_PACKAGE);
        return new DefaultKafkaConsumerFactory<>(
                kafkaProperties.buildConsumerProperties(), new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, WorkItem> workItemKafkaListenerContainerFactory(
            ConsumerFactory<String, WorkItem> workItemConsumerFactory, DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, WorkItem> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(workItemConsumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // enable.auto.commit: false (from spring.kafka.consumer.*) + manual
        // immediate ack (ADR 0011): offset commits synchronously, only
        // after onMessage returns normally -- at-least-once delivery.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // Same reasoning as api's WorkItemProducerConfig (observability#1,
        // ADR 0013): spring.kafka.listener.observation-enabled only
        // wires observation into Boot's own auto-configured listener
        // container factory, and this one is hand-built. Without this,
        // WorkItemListener#onMessage runs with no trace context at all
        // (an empty trace-id bracket in the logs) even though api's
        // producer send() correctly attaches one to the record headers.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
