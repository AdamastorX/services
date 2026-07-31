package com.adamastorx.watchlist.subscription;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** JPA row for the {@code subscriptions} table (backlog #53, V1 migration). */
@Entity
@Table(name = "subscriptions")
public class SubscriptionEntity {

    @Id
    private UUID id;

    @Column(name = "variant_key")
    private String variantKey;

    @Column(name = "gene_symbol")
    private String geneSymbol;

    @Column(name = "ntfy_topic", nullable = false)
    private String ntfyTopic;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected SubscriptionEntity() {
        // JPA
    }

    public SubscriptionEntity(UUID id, String variantKey, String geneSymbol, String ntfyTopic, Instant createdAt) {
        this.id = id;
        this.variantKey = variantKey;
        this.geneSymbol = geneSymbol;
        this.ntfyTopic = ntfyTopic;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getVariantKey() {
        return variantKey;
    }

    public String getGeneSymbol() {
        return geneSymbol;
    }

    public String getNtfyTopic() {
        return ntfyTopic;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
