package com.adamastorx.api.clinvar;

/**
 * Consumed from the {@code clinvar.ingestion.completed} topic
 * (services#26, ADR 0018) -- {@code api}'s own copy of the record {@code
 * workers.clinvar.ClinVarIngestionCompletedEvent} publishes, decoupled on
 * purpose (same wire-contract-not-shared-class precedent as
 * {@code WorkItem}, ADR 0007). Field shape must match the producer's
 * exactly (plain JSON-friendly types, no shared Java type).
 */
public record ClinVarIngestionCompletedEvent(
        String releaseId,
        String previousReleaseId,
        String publishedDate,
        long variantCount,
        String ingestedAt) {}
