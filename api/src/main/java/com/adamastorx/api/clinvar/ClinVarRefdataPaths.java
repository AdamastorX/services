package com.adamastorx.api.clinvar;

import java.nio.file.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Read-only counterpart of {@code workers.clinvar.ClinVarRefdataPaths}
 * (services#24, ADR 0018) -- {@code api} only ever resolves {@code
 * current} to query it, never writes a release or flips the pointer
 * itself. Same shared RWX PVC mount point
 * ({@code app.clinvar.refdata-path}, defaulting to platform#35's stated
 * path), same {@code current} symlink convention.
 */
@Component
class ClinVarRefdataPaths {

    private static final String VCF_FILENAME = "clinvar.vcf.gz";
    private static final String CURRENT_LINK_NAME = "current";

    private final Path root;

    ClinVarRefdataPaths(@Value("${app.clinvar.refdata-path}") String refdataPath) {
        this.root = Path.of(refdataPath);
    }

    /**
     * Resolves the VCF a lookup should query right now. Re-resolved on
     * every call (not cached at startup) -- a plain filesystem path
     * lookup is cheap, and this is exactly what lets a running {@code api}
     * instance pick up a new ClinVar release the moment {@code workers}
     * flips {@code current}, with no restart.
     */
    Path currentVcfPath() {
        return root.resolve(CURRENT_LINK_NAME).resolve(VCF_FILENAME);
    }
}
