package com.adamastorx.workers.clinvar;

/**
 * Published to the {@code clinvar.ingestion.completed} topic once an
 * ingestion is fully committed (services#25, ADR 0018) -- {@code api}'s
 * cache-invalidation consumer (services#26) is the trigger this event
 * exists for. Same wire-contract-not-shared-class precedent as {@code
 * WorkItem} (ADR 0007): {@code api} has its own copy of this record for
 * deserialization, decoupled from this one on purpose. All fields are
 * plain JSON-friendly types (no {@code UUID}/{@code Instant}/{@code
 * LocalDate}) for the same reason {@code WorkItem} uses a plain
 * {@code String} id -- a stable wire shape, not a reflection of either
 * module's internal Java types.
 */
public record ClinVarIngestionCompletedEvent(
        String releaseId,
        String previousReleaseId,
        String publishedDate,
        long variantCount,
        String ingestedAt) {}
