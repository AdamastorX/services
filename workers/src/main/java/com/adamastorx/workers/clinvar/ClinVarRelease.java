package com.adamastorx.workers.clinvar;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * JPA row for {@code clinvar_release} (services#25, ADR 0018) --
 * schema owned by {@code api}'s Flyway history
 * ({@code V2__create_clinvar_tables.sql}), this class just maps it. A
 * near-identical class exists in {@code api} (read-only there): the same
 * duplication precedent {@code WorkItem} already established (ADR 0007
 * keeps producer/consumer DTOs decoupled per module rather than reaching
 * for the still-inert {@code shared} module) applied here too, even
 * though this is a real persisted entity rather than a wire DTO --
 * standing up {@code shared} as an actual Maven module is a bigger
 * structural change than these three issues call for, and the schema
 * itself (the actual source of truth) stays single-owned regardless of
 * how many Java projections of it exist. Worth revisiting as a deliberate
 * future call if this duplication ever causes real drift, not before.
 */
@Entity
@Table(name = "clinvar_release")
public class ClinVarRelease {

    @Id
    @Column(name = "release_id")
    private UUID releaseId;

    @Column(name = "source_url", nullable = false)
    private String sourceUrl;

    @Column(name = "file_sha256", nullable = false)
    private String fileSha256;

    @Column(name = "published_date", nullable = false)
    private LocalDate publishedDate;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;

    @Column(name = "variant_count", nullable = false)
    private long variantCount;

    @Column(name = "is_active", nullable = false)
    private boolean active;

    protected ClinVarRelease() {
        // JPA
    }

    public ClinVarRelease(
            UUID releaseId,
            String sourceUrl,
            String fileSha256,
            LocalDate publishedDate,
            Instant ingestedAt,
            long variantCount,
            boolean active) {
        this.releaseId = releaseId;
        this.sourceUrl = sourceUrl;
        this.fileSha256 = fileSha256;
        this.publishedDate = publishedDate;
        this.ingestedAt = ingestedAt;
        this.variantCount = variantCount;
        this.active = active;
    }

    public UUID getReleaseId() {
        return releaseId;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public String getFileSha256() {
        return fileSha256;
    }

    public LocalDate getPublishedDate() {
        return publishedDate;
    }

    public Instant getIngestedAt() {
        return ingestedAt;
    }

    public long getVariantCount() {
        return variantCount;
    }

    public void setVariantCount(long variantCount) {
        this.variantCount = variantCount;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}
