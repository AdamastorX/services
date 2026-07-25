package com.adamastorx.api.clinvar;

import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * In-process htsjdk point lookup against the current release's
 * tabix-indexed VCF (services#24, ADR 0018 -- explicit call: not a
 * bcftools/tabix subprocess, per-request fork/exec latency and
 * stdout-parsing fragility are worse than this one extra JVM dependency
 * for a request-path lookup).
 *
 * <p>Opens a fresh {@link VCFFileReader} per call rather than holding one
 * open across requests -- htsjdk's own examples use it this way for point
 * lookups, and this project's real traffic (a portfolio/demo cluster) is
 * nowhere near the volume where the file-handle overhead would matter. A
 * pooled/reused reader is a reasonable future optimization if that ever
 * changes, not something this milestone needs.
 */
@Component
class ClinVarVcfQueryService {

    private final ClinVarRefdataPaths paths;

    ClinVarVcfQueryService(ClinVarRefdataPaths paths) {
        this.paths = paths;
    }

    /**
     * Queries position {@code (chrom, pos)} against the current release
     * and returns the record whose REF/ALT exactly match, if any --
     * ClinVar normalizes one VCF record per allele, so more than one
     * record can share a position (see {@code fixture-release-1.vcf}'s
     * own real BRCA1 example), and only an exact allele match is the
     * variant the caller actually asked about.
     */
    Optional<VcfHit> query(String chrom, int pos, String ref, String alt) {
        String normalizedChrom = chrom.startsWith("chr") ? chrom.substring(3) : chrom;
        try (VCFFileReader reader = new VCFFileReader(paths.currentVcfPath().toFile(), true);
                CloseableIterator<VariantContext> hits = reader.query(normalizedChrom, pos, pos)) {
            while (hits.hasNext()) {
                VariantContext context = hits.next();
                if (!context.getReference().getDisplayString().equals(ref)) {
                    continue;
                }
                for (Allele altAllele : context.getAlternateAlleles()) {
                    if (altAllele.getDisplayString().equals(alt)) {
                        return Optional.of(toHit(context));
                    }
                }
            }
        }
        return Optional.empty();
    }

    private static VcfHit toHit(VariantContext context) {
        String clinicalSignificance = joinedAttribute(context, "CLNSIG");
        String reviewStatus = joinedAttribute(context, "CLNREVSTAT");
        String rsid = joinedAttribute(context, "RS");
        return new VcfHit(
                clinicalSignificance,
                reviewStatus,
                rsid == null ? null : "rs" + rsid.replaceFirst("(?i)^rs", ""));
    }

    /** Reconstructs a Number=. INFO value's original comma-joined text (see class discussion). */
    private static String joinedAttribute(VariantContext context, String key) {
        List<String> values = context.getAttributeAsStringList(key, null);
        return values.isEmpty() ? null : String.join(",", values);
    }

    record VcfHit(String clinicalSignificance, String reviewStatus, String rsid) {}
}
