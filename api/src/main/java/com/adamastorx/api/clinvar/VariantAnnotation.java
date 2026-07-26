package com.adamastorx.api.clinvar;

/**
 * REST response shape and Redis cache-entry value for a variant lookup
 * (services#24, ADR 0018; upstream simplified to an HTTP call under ADR
 * 0019). {@code clinvarReleaseId} is ADR 0018's provenance requirement,
 * stamped once by {@code clinvar-service} and passed through unchanged
 * ({@link VariantLookupService#toAnnotation}) into this single shared
 * field -- not recomputed independently anywhere else, so a cache entry
 * and the response that originally produced it can never silently
 * disagree about which release backs them.
 *
 * <p>{@code gnomadAlleleFrequency} is {@code null} whenever {@code
 * clinvar-service} hasn't populated it -- ADR 0018 called out gnomAD
 * population allele frequency as optional for M5; wiring an actual gnomAD
 * chr21/chr22 slice is that service's concern now, not this module's.
 */
public record VariantAnnotation(
        String chrom,
        int pos,
        String ref,
        String alt,
        String rsid,
        String clinicalSignificance,
        String reviewStatus,
        Double gnomadAlleleFrequency,
        String clinvarReleaseId) {}
