package com.adamastorx.watchlist.delivery;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * JPA row for the {@code deliveries} table (V2 migration) -- the outbox row
 * ADR 0026 decided on. One row per (subscription, release, changed variant)
 * -- see that migration's comment for the durability/dedupe reasoning.
 */
@Entity
@Table(name = "deliveries")
public class DeliveryEntity {

    @Id
    private UUID id;

    @Column(name = "subscription_id", nullable = false)
    private UUID subscriptionId;

    @Column(name = "release_id", nullable = false)
    private String releaseId;

    @Column(name = "variant_key", nullable = false)
    private String variantKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DeliveryStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected DeliveryEntity() {
        // JPA
    }

    public DeliveryEntity(UUID id, UUID subscriptionId, String releaseId, String variantKey, Instant createdAt) {
        this.id = id;
        this.subscriptionId = subscriptionId;
        this.releaseId = releaseId;
        this.variantKey = variantKey;
        this.status = DeliveryStatus.PENDING;
        this.attempts = 0;
        this.createdAt = createdAt;
        this.updatedAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public UUID getSubscriptionId() {
        return subscriptionId;
    }

    public String getReleaseId() {
        return releaseId;
    }

    public String getVariantKey() {
        return variantKey;
    }

    public DeliveryStatus getStatus() {
        return status;
    }

    public void setStatus(DeliveryStatus status) {
        this.status = status;
        this.updatedAt = Instant.now();
    }

    public int getAttempts() {
        return attempts;
    }

    public void recordAttempt(String error) {
        this.attempts++;
        this.lastError = error;
        this.updatedAt = Instant.now();
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
