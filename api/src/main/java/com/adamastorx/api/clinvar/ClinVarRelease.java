package com.adamastorx.api.clinvar;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/**
 * JPA row for {@code clinvar_release} (services#24/#25, ADR 0018) --
 * read-only from {@code api}'s side (only {@code workers} ever writes
 * this table). Same duplication precedent as {@code workers}'s own
 * {@code ClinVarRelease} -- see that class's javadoc for why this isn't
 * pulled into the still-inert {@code shared} module instead.
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

    public boolean isActive() {
        return active;
    }
}
