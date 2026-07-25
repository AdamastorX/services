package com.adamastorx.api.clinvar;

import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

/**
 * Orchestrates one variant lookup (services#24, ADR 0018): resolve an
 * rsID to coordinates first if that's how the caller asked, then
 * cache-aside around the actual tabix point lookup, stamping the current
 * {@code clinvarReleaseId} into the response either way.
 */
@Service
public class VariantLookupService {

    private final ClinVarVariantIndexRepository variantIndexRepository;
    private final ClinVarReleaseRepository releaseRepository;
    private final ClinVarVcfQueryService vcfQueryService;
    private final VariantAnnotationCacheService cache;

    public VariantLookupService(
            ClinVarVariantIndexRepository variantIndexRepository,
            ClinVarReleaseRepository releaseRepository,
            ClinVarVcfQueryService vcfQueryService,
            VariantAnnotationCacheService cache) {
        this.variantIndexRepository = variantIndexRepository;
        this.releaseRepository = releaseRepository;
        this.vcfQueryService = vcfQueryService;
        this.cache = cache;
    }

    /** Coordinate-based lookup: {@code (chrom, pos, ref, alt)} given directly. */
    public Optional<VariantAnnotation> lookupByCoordinates(String chrom, int pos, String ref, String alt) {
        return resolve(chrom, pos, ref, alt);
    }

    /**
     * rsID-based lookup: resolves to coordinates via {@code
     * clinvar_variant_index} first (ADR 0018 -- tabix indexes are
     * position-based, scanning 250MB per rsID lookup is a non-starter),
     * then defers to the same coordinate path. More than one candidate
     * coordinate can exist for a rare rsID (see
     * {@link ClinVarVariantIndexRepository#findByRsid}'s javadoc); the
     * first one that actually resolves against the current release wins.
     */
    public Optional<VariantAnnotation> lookupByRsid(String rsid) {
        List<ClinVarVariantIndexEntity> candidates = variantIndexRepository.findByRsid(normalizeRsid(rsid));
        for (ClinVarVariantIndexEntity candidate : candidates) {
            Optional<VariantAnnotation> resolved =
                    resolve(candidate.getChrom(), candidate.getPos(), candidate.getRef(), candidate.getAlt());
            if (resolved.isPresent()) {
                return resolved;
            }
        }
        return Optional.empty();
    }

    private Optional<VariantAnnotation> resolve(String chrom, int pos, String ref, String alt) {
        Optional<VariantAnnotation> cached = cache.get(chrom, pos, ref, alt);
        if (cached.isPresent()) {
            return cached;
        }

        Optional<ClinVarVcfQueryService.VcfHit> hit = vcfQueryService.query(chrom, pos, ref, alt);
        if (hit.isEmpty()) {
            return Optional.empty();
        }

        String currentReleaseId = releaseRepository
                .findByActiveTrue()
                .map(release -> release.getReleaseId().toString())
                .orElse(null);

        VariantAnnotation annotation = new VariantAnnotation(
                chrom,
                pos,
                ref,
                alt,
                hit.get().rsid(),
                hit.get().clinicalSignificance(),
                hit.get().reviewStatus(),
                null, // gnomAD allele frequency -- deferred, see VariantAnnotation's javadoc
                currentReleaseId);

        cache.put(chrom, pos, ref, alt, annotation);
        return Optional.of(annotation);
    }

    private static String normalizeRsid(String rawRsid) {
        return "rs" + rawRsid.replaceFirst("(?i)^rs", "");
    }
}
