package com.adamastorx.api.clinvar;

/**
 * REST response shape and Redis cache-entry value for a variant lookup
 * (services#24, ADR 0018). {@code clinvarReleaseId} is ADR 0018's
 * provenance requirement stamped once, at response-assembly time
 * ({@link ClinVarVcfQueryService}), into this single shared field -- not
 * recomputed independently anywhere else, so a cache entry and the
 * response that originally produced it can never silently disagree about
 * which release backs them.
 *
 * <p>{@code gnomadAlleleFrequency} is always {@code null} in this PR --
 * ADR 0018 calls out gnomAD population allele frequency as optional for
 * M5; wiring an actual gnomAD chr21/chr22 slice is deliberately deferred
 * out of this issue's scope (see services#24's PR description). The field
 * is kept on the shape now rather than added later specifically so a
 * future PR only has to populate it, not change the response contract.
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
