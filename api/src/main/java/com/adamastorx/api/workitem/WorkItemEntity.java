package com.adamastorx.api.workitem;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * JPA row for the {@code work_items} table (services#4, ADR 0012). Kept
 * distinct from the {@link WorkItem} record used for the REST response
 * and the Kafka payload — same persistence/DTO layering every Spring Boot
 * app uses, not a special case here.
 */
@Entity
@Table(name = "work_items")
public class WorkItemEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String message;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected WorkItemEntity() {
        // JPA
    }

    public WorkItemEntity(UUID id, String message, Instant createdAt) {
        this.id = id;
        this.message = message;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getMessage() {
        return message;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
