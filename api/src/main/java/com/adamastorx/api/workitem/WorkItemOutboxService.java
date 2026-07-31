package com.adamastorx.api.workitem;

import com.adamastorx.api.outbox.OutboxEventEntity;
import com.adamastorx.api.outbox.OutboxEventJpaRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Backlog #16 / ADR 0026: replaces {@code WorkItemController}'s old
 * "save, then call KafkaTemplate.send() directly" two-step with a single
 * transaction that persists both the {@code work_items} row and an
 * {@code outbox_events} row -- the row {@link
 * com.adamastorx.api.outbox.OutboxRelay} independently publishes and marks
 * PUBLISHED. If this transaction commits, the work item is durably
 * recorded in PostgreSQL <em>and</em> guaranteed to eventually reach Kafka,
 * regardless of whether the broker happens to be reachable at the moment
 * this method returns -- unlike the code this replaces, a publish failure
 * (or the broker simply being down right now) can no longer silently drop
 * the Kafka side of a successfully committed save.
 *
 * <p>Builds its own {@code ObjectMapper} rather than injecting a Spring
 * bean -- found live via CI (not assumed): this module has no classic
 * Jackson 2 ({@code com.fasterxml.jackson.databind.ObjectMapper}) bean
 * anywhere. Boot 4.1's own Jackson autoconfiguration provides a Jackson 3
 * ({@code tools.jackson.databind.ObjectMapper}) bean instead (see {@code
 * pom.xml}'s comment on why {@code jackson-databind} -- the classic
 * Jackson 2 artifact spring-kafka's {@code JsonSerializer} needs -- is a
 * plain, non-autoconfigured dependency here), so injecting the classic
 * type failed to start the whole application context with {@code
 * NoSuchBeanDefinitionException}, taking down every integration test in
 * the module. A plain {@code new ObjectMapper()} needs no bean at all.
 */
@Service
public class WorkItemOutboxService {

    private final WorkItemJpaRepository workItemRepository;
    private final OutboxEventJpaRepository outboxRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public WorkItemOutboxService(
            WorkItemJpaRepository workItemRepository,
            OutboxEventJpaRepository outboxRepository,
            @Value("${app.kafka.work-items-topic}") String topic) {
        this.workItemRepository = workItemRepository;
        this.outboxRepository = outboxRepository;
        this.topic = topic;
    }

    @Transactional
    public WorkItem createAndEnqueue(String message) {
        WorkItemEntity entity = new WorkItemEntity(UUID.randomUUID(), message, Instant.now());
        workItemRepository.save(entity);

        WorkItem workItem = new WorkItem(entity.getId().toString(), entity.getMessage());
        try {
            // JSON payload, no Java-type header -- identical wire shape the old
            // KafkaTemplate<String, WorkItem> + spring.json.add.type.headers:
            // false configuration produced (application.yml), so workers'
            // consumer needs no change.
            String payload = objectMapper.writeValueAsString(workItem);
            outboxRepository.save(new OutboxEventEntity(UUID.randomUUID(), topic, null, payload, Instant.now()));
        } catch (JsonProcessingException ex) {
            // WorkItem is two plain Strings -- cannot actually fail to serialize;
            // rethrown as unchecked to keep this method's signature clean rather
            // than forcing every caller to handle a practically-impossible case.
            throw new IllegalStateException("Failed to serialize WorkItem for the outbox", ex);
        }

        return workItem;
    }
}
