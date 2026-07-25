package com.adamastorx.api.clinvar;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

interface ClinVarVariantIndexRepository extends JpaRepository<ClinVarVariantIndexEntity, Long> {

    /**
     * Usually resolves to exactly one row -- an rsID is rare but not
     * strictly guaranteed to map to a single (chrom, pos, ref, alt) tuple
     * (ClinVar records normalize by allele, and a single rsID can in
     * principle be attached to more than one). {@link VariantLookupService}
     * tries each candidate in order against the current release's tabix
     * file and returns the first that actually resolves.
     */
    List<ClinVarVariantIndexEntity> findByRsid(String rsid);
}
