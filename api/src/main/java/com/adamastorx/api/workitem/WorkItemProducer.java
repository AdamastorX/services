package com.adamastorx.api.workitem;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes {@link WorkItem}s to the {@code work-items} topic (ADR 0011).
 * Publishes with a {@code null} key, so Kafka round-robins across the
 * topic's 3 partitions — there is no domain entity yet that would give a
 * natural ordering key.
 */
@Service
public class WorkItemProducer {

    private final KafkaTemplate<String, WorkItem> kafkaTemplate;
    private final String topic;

    public WorkItemProducer(
            KafkaTemplate<String, WorkItem> kafkaTemplate,
            @Value("${app.kafka.work-items-topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void publish(WorkItem workItem) {
        kafkaTemplate.send(topic, null, workItem);
    }
}
