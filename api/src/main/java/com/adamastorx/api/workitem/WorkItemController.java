package com.adamastorx.api.workitem;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Smallest reasonable trigger proving the async Kafka path end to end
 * (services#3 / ADR 0011): accepts a short message, wraps it in a
 * {@link WorkItem}, and publishes it for a {@code workers} replica to
 * consume. No persistence, no validation beyond "a message was supplied" —
 * this is a scaffold/proof, real domain endpoints arrive with future
 * issues (PostgreSQL/Redis, services#4/#5).
 */
@RestController
public class WorkItemController {

    private final WorkItemProducer producer;

    public WorkItemController(WorkItemProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/work-items")
    public ResponseEntity<WorkItem> create(@RequestBody Map<String, String> body) {
        WorkItem workItem = new WorkItem(UUID.randomUUID().toString(), body.get("message"));
        producer.publish(workItem);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(workItem);
    }
}
