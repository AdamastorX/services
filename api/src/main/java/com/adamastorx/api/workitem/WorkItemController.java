package com.adamastorx.api.workitem;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Proves both halves of services#4's AC (ADR 0012: reads/writes to
 * PostgreSQL) and services#3's AC (ADR 0011: the async Kafka path):
 * {@code POST /work-items} persists a {@link WorkItemEntity} row and an
 * outbox_events row in one transaction (backlog #16, ADR 0026 --
 * {@link WorkItemOutboxService}, superseding the old direct {@code
 * WorkItemProducer.publish()} call this class used to make here). {@code
 * GET} proves the row is actually there. No validation beyond "a message
 * was supplied"; this is still a scaffold/proof, real domain endpoints
 * arrive with a future issue.
 *
 * <p>{@code GET /work-items/{id}} is fronted by {@link WorkItemCacheService}
 * (services#5, ADR 0016) — cache-aside, read path only: a hit skips
 * PostgreSQL entirely, a miss (including a Redis outage, which the cache
 * service also reports as an empty {@code Optional}, deliberately
 * indistinguishable from a plain miss here — see that class's javadoc)
 * reads PostgreSQL and best-effort fills the cache for next time.
 * {@code POST /work-items} is untouched by this — see ADR 0016 for why
 * the per-id read was chosen for caching and the list wasn't.
 *
 * <p>{@code GET /work-items} (the list) is real-paginated (backlog #130)
 * — {@code ?page}/{@code ?size} query params, a stated default page size
 * and a hard maximum a caller cannot exceed, returned as a
 * {@link WorkItemPage} envelope rather than a bare array so a real
 * caller can tell whether there's more to fetch.
 */
@RestController
public class WorkItemController {

    // backlog #130: GET /work-items had no limit at all -- a real request
    // returned ~14MB/108,000+ rows, past blackbox-exporter's own 5s probe
    // timeout and, confirmed live (2026-08-14, 282 real restarts over
    // 41h), capable of OOMing a correctly-sized pod on ordinary read
    // traffic (workload-generator's own work_item_read_weight), not just
    // a synthetic worst case. MAX_PAGE_SIZE is a hard ceiling regardless
    // of what a caller asks for -- a caller can't opt back into the old
    // unbounded behavior by passing a huge ?size=.
    private static final int DEFAULT_PAGE_SIZE = 50;
    private static final int MAX_PAGE_SIZE = 200;

    private final WorkItemJpaRepository repository;

    private final WorkItemOutboxService outboxService;

    private final WorkItemCacheService cache;

    public WorkItemController(
            WorkItemJpaRepository repository, WorkItemOutboxService outboxService, WorkItemCacheService cache) {
        this.repository = repository;
        this.outboxService = outboxService;
        this.cache = cache;
    }

    @PostMapping("/work-items")
    public ResponseEntity<WorkItem> create(@RequestBody Map<String, String> body) {
        WorkItem workItem = outboxService.createAndEnqueue(body.get("message"));
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
    public WorkItemPage list(
            @RequestParam(defaultValue = "0") int page, @RequestParam(name = "size", required = false) Integer size) {
        int pageSize = size == null ? DEFAULT_PAGE_SIZE : Math.min(Math.max(size, 1), MAX_PAGE_SIZE);
        Page<WorkItemEntity> result =
                repository.findAll(PageRequest.of(Math.max(page, 0), pageSize, Sort.by(Sort.Direction.DESC, "createdAt")));
        List<WorkItem> items =
                result.getContent().stream().map(WorkItemController::toWorkItem).toList();
        return new WorkItemPage(items, result.getNumber(), result.getSize(), result.getTotalElements(), result.getTotalPages());
    }

    private static WorkItem toWorkItem(WorkItemEntity entity) {
        return new WorkItem(entity.getId().toString(), entity.getMessage());
    }
}
