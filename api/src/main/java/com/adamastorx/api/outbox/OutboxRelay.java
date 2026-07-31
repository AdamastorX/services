package com.adamastorx.api.outbox;

import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Backlog #16 / ADR 0026's relay half: an independent poll loop that
 * publishes {@code PENDING} outbox_events rows to Kafka and marks them
 * PUBLISHED, decoupled from the request thread that created them
 * ({@code WorkItemOutboxService}). A Kafka outage no longer risks losing a
 * committed work item -- the row simply stays PENDING and is retried on
 * the next tick, proven by {@code WorkItemOutboxFailureIntegrationTest}.
 *
 * <p>Sends the already-serialized JSON payload as a raw String (not a
 * typed {@code WorkItem}) via a dedicated {@code String}-valued {@link
 * KafkaTemplate} -- this relay is intentionally generic across any future
 * outbox_events row, not work-items-specific, the same way the table
 * itself is topic-agnostic.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxEventJpaRepository repository;
    private final KafkaTemplate<String, String> outboxKafkaTemplate;
    private final int batchSize;

    public OutboxRelay(
            OutboxEventJpaRepository repository,
            KafkaTemplate<String, String> outboxKafkaTemplate,
            @Value("${app.outbox.batch-size:50}") int batchSize) {
        this.repository = repository;
        this.outboxKafkaTemplate = outboxKafkaTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${app.outbox.relay-interval-ms:2000}")
    public void relayPendingEvents() {
        List<OutboxEventEntity> batch = repository.findByStatusOrderByCreatedAtAsc("PENDING", PageRequest.of(0, batchSize));
        for (OutboxEventEntity event : batch) {
            publish(event);
        }
    }

    private void publish(OutboxEventEntity event) {
        try {
            // Bounded, synchronous get() -- this relay's own scheduled thread is
            // the only thing waiting on it, and a short timeout here is what
            // keeps one unreachable broker from silently wedging every future
            // tick forever; a real failure just leaves the row PENDING for the
            // next tick, same outcome as an exception below.
            outboxKafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload()).get();
            markPublished(event);
        } catch (Exception ex) {
            log.warn("Failed to publish outbox event {} (topic {}), will retry next tick: {}", event.getId(), event.getTopic(), ex.getMessage());
        }
    }

    @Transactional
    void markPublished(OutboxEventEntity event) {
        repository.markPublished(event.getId(), Instant.now());
    }
}
