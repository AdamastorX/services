package com.adamastorx.api.clinvar;

import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * Orchestrates one variant lookup (ADR 0019): cache-aside in front of an
 * HTTP call to {@code clinvar-service}, rather than the direct
 * htsjdk/tabix-file and JPA/Postgres access this service used before
 * (services#24/#26, ADR 0018 -- removed; see the class javadocs of the
 * types deleted alongside this one for why).
 *
 * <p><strong>Coordinate lookups</strong> check the Redis cache first
 * (exactly as before -- {@link VariantAnnotationCacheService}'s
 * cache-aside mechanics are unchanged, just fronting a different
 * upstream now) and only call {@code clinvar-service} on a miss.
 *
 * <p><strong>rsID lookups cannot check the cache first</strong>, unlike
 * before: the cache key is coordinate-based ({@code
 * variantAnnotation:{chrom}:{pos}:{ref}:{alt}}), and resolving an rsID to
 * coordinates was previously a cheap local Postgres query
 * ({@code clinvar_variant_index}) done before the cache check. That index
 * no longer lives in {@code api} at all under ADR 0019 -- {@code
 * clinvar-service}'s lookup endpoint resolves rsID to coordinates and the
 * full annotation in one call, so an rsID lookup always calls {@code
 * clinvar-service}. The result is still written into the cache under its
 * resolved coordinate key afterward, so a subsequent coordinate-based
 * lookup (or a repeat rsID lookup that happens to be preceded by a
 * coordinate one) still hits.
 */
@Service
public class VariantLookupService {

    private final ClinVarServiceClient clinVarServiceClient;
    private final VariantAnnotationCacheService cache;

    public VariantLookupService(ClinVarServiceClient clinVarServiceClient, VariantAnnotationCacheService cache) {
        this.clinVarServiceClient = clinVarServiceClient;
        this.cache = cache;
    }

    /** Coordinate-based lookup: {@code (chrom, pos, ref, alt)} given directly. */
    public Optional<VariantAnnotation> lookupByCoordinates(String chrom, int pos, String ref, String alt) {
        Optional<VariantAnnotation> cached = cache.get(chrom, pos, ref, alt);
        if (cached.isPresent()) {
            return cached;
        }

        Optional<VariantAnnotation> fetched =
                clinVarServiceClient.lookupByCoordinates(chrom, pos, ref, alt).map(VariantLookupService::toAnnotation);
        fetched.ifPresent(annotation -> cache.put(annotation.chrom(), annotation.pos(), annotation.ref(),
                annotation.alt(), annotation));
        return fetched;
    }

    /** rsID-based lookup: resolved entirely by {@code clinvar-service} (see class javadoc). */
    public Optional<VariantAnnotation> lookupByRsid(String rsid) {
        Optional<VariantAnnotation> fetched =
                clinVarServiceClient.lookupByRsid(rsid).map(VariantLookupService::toAnnotation);
        fetched.ifPresent(annotation -> cache.put(annotation.chrom(), annotation.pos(), annotation.ref(),
                annotation.alt(), annotation));
        return fetched;
    }

    private static VariantAnnotation toAnnotation(ClinVarLookupResponse response) {
        return new VariantAnnotation(
                response.chrom(),
                response.pos(),
                response.ref(),
                response.alt(),
                response.rsid(),
                response.clinicalSignificance(),
                response.clinicalReviewStatus(),
                response.gnomadAlleleFrequency(),
                response.clinvarReleaseId());
    }
}
