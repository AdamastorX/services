package com.adamastorx.api.clinvar;

/**
 * Wire shape of {@code clinvar-service}'s {@code GET
 * /internal/clinvar/lookup} response (ADR 0019) -- a plain JSON-friendly
 * DTO deserialized by {@link ClinVarServiceClient}, deliberately not the
 * same type as the public {@link VariantAnnotation} response/cache-entry
 * shape even though most fields line up 1:1: {@code clinicalReviewStatus}
 * here (clinvar-service's field name) maps onto {@code
 * VariantAnnotation.reviewStatus} (this module's already-established
 * field name, unchanged since services#24 to avoid a public API/cache
 * key-format change) -- see {@link VariantLookupService#toAnnotation}.
 */
record ClinVarLookupResponse(
        String chrom,
        int pos,
        String ref,
        String alt,
        String rsid,
        String clinicalSignificance,
        String clinicalReviewStatus,
        Double gnomadAlleleFrequency,
        String clinvarReleaseId) {}
