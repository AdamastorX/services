package com.adamastorx.workers.clinvar;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** Publishes {@link ClinVarIngestionCompletedEvent}s (services#25, ADR 0018). */
@Service
class ClinVarIngestionProducer {

    private final KafkaTemplate<String, ClinVarIngestionCompletedEvent> kafkaTemplate;
    private final String topic;

    ClinVarIngestionProducer(
            KafkaTemplate<String, ClinVarIngestionCompletedEvent> kafkaTemplate,
            @Value("${app.kafka.clinvar-ingestion-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    void publish(ClinVarIngestionCompletedEvent event) {
        // Null key, same reasoning as WorkItemProducer: no natural
        // partitioning key, and this topic has exactly one consumer group
        // (api's cache invalidation) so ordering across releases isn't a
        // real concern -- ingestions are already serialized by the
        // weekly/manual trigger, never concurrent with each other.
        kafkaTemplate.send(topic, null, event);
    }
}
