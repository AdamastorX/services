package com.adamastorx.api.workitem;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Proves both halves of services#4's AC (ADR 0012: reads/writes to
 * PostgreSQL) and services#3's AC (ADR 0011: the async Kafka path):
 * {@code POST /work-items} persists a {@link WorkItemEntity} row *before*
 * publishing to Kafka — if the DB write fails, nothing gets published —
 * then {@code GET} proves the row is actually there. No validation
 * beyond "a message was supplied"; this is still a scaffold/proof, real
 * domain endpoints arrive with a future issue.
 */
@RestController
public class WorkItemController {

    private final WorkItemJpaRepository repository;

    private final WorkItemProducer producer;

    public WorkItemController(WorkItemJpaRepository repository, WorkItemProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    @PostMapping("/work-items")
    public ResponseEntity<WorkItem> create(@RequestBody Map<String, String> body) {
        WorkItemEntity entity = new WorkItemEntity(UUID.randomUUID(), body.get("message"), Instant.now());
        repository.save(entity);

        WorkItem workItem = toWorkItem(entity);
        producer.publish(workItem);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(workItem);
    }

    @GetMapping("/work-items/{id}")
    public ResponseEntity<WorkItem> get(@PathVariable UUID id) {
        return repository.findById(id).map(entity -> ResponseEntity.ok(toWorkItem(entity)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/work-items")
    public List<WorkItem> list() {
        return repository.findAll().stream().map(WorkItemController::toWorkItem).toList();
    }

    private static WorkItem toWorkItem(WorkItemEntity entity) {
        return new WorkItem(entity.getId().toString(), entity.getMessage());
    }
}
