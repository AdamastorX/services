package com.adamastorx.watchlist.ingestion;

import java.util.List;

/**
 * Wire shape copied verbatim from api's own {@code ClinVarIngestionCompletedEvent}
 * (ADR 0019) -- same producer (clinvar-service), same topic, second
 * independent consumer group. Kept as watchlist-service's own copy rather
 * than a shared library, same producer/consumer decoupling precedent ADR
 * 0011 already established for WorkItem.
 */
public record ClinVarIngestionCompletedEvent(
        String newReleaseId,
        String previousReleaseId,
        String publishedDate,
        long variantCount,
        String ingestedAt,
        List<String> changedKeys) {}
