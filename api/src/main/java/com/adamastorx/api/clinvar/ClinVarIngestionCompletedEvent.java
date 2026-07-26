package com.adamastorx.api.clinvar;

import java.util.List;

/**
 * Consumed from the {@code clinvar.ingestion.completed} topic (ADR 0019
 * -- shape changed from services#26/ADR 0018's original). {@code
 * clinvar-service} is now the sole producer (previously {@code workers}
 * was); it holds both the old and new release's tabix files locally and
 * computes exactly which cached keys' classification changed, publishing
 * that list directly as {@code changedKeys} -- {@code api} no longer
 * needs to (and, since it no longer has any local tabix/Postgres access
 * under ADR 0019, no longer can) re-derive that diff itself.
 *
 * <p>{@code changedKeys} entries are the exact Redis keys to delete
 * (e.g. {@code "variantAnnotation:17:43057062:T:TG"}), already in {@link
 * VariantAnnotationCacheService#key}'s format -- {@link
 * VariantInvalidationService} deletes them verbatim, no parsing needed.
 */
public record ClinVarIngestionCompletedEvent(
        String newReleaseId,
        String previousReleaseId,
        String publishedDate,
        long variantCount,
        String ingestedAt,
        List<String> changedKeys) {}
