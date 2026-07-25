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
 *
 * <p>{@code GET /work-items/{id}} is fronted by {@link WorkItemCacheService}
 * (services#5, ADR 0016) — cache-aside, read path only: a hit skips
 * PostgreSQL entirely, a miss (including a Redis outage, which the cache
 * service also reports as an empty {@code Optional}, deliberately
 * indistinguishable from a plain miss here — see that class's javadoc)
 * reads PostgreSQL and best-effort fills the cache for next time.
 * {@code POST}/{@code GET /work-items} (the list) are untouched by this —
 * see ADR 0016 for why the per-id read was chosen and the list wasn't.
 */
@RestController
public class WorkItemController {

    private final WorkItemJpaRepository repository;

    private final WorkItemProducer producer;

    private final WorkItemCacheService cache;

    public WorkItemController(WorkItemJpaRepository repository, WorkItemProducer producer, WorkItemCacheService cache) {
        this.repository = repository;
        this.producer = producer;
        this.cache = cache;
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
        return cache.get(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> repository.findById(id)
                        .map(WorkItemController::toWorkItem)
                        .map(workItem -> {
                            cache.put(id, workItem);
                            return ResponseEntity.ok(workItem);
                        })
                        .orElseGet(() -> ResponseEntity.notFound().build()));
    }

    @GetMapping("/work-items")
    public List<WorkItem> list() {
        return repository.findAll().stream().map(WorkItemController::toWorkItem).toList();
    }

    private static WorkItem toWorkItem(WorkItemEntity entity) {
        return new WorkItem(entity.getId().toString(), entity.getMessage());
    }
}
