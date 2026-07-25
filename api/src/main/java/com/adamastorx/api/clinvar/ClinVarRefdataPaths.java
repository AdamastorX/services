package com.adamastorx.api.clinvar;

import java.nio.file.Path;
import java.util.UUID;
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
    private static final String RELEASES_DIR_NAME = "releases";

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

    /**
     * Resolves a <em>specific</em> release's VCF by id, bypassing {@code
     * current} entirely -- services#26's {@code VariantInvalidationService}
     * needs both the old and new release's files at once, and {@code
     * current} only ever points at one of them (the new one, by the time
     * the invalidation event is even published -- see
     * {@code workers.clinvar.ClinVarIngestionService}'s ordering). Relies
     * on {@code workers.clinvar.ClinVarRefdataPaths}' retention policy
     * keeping the previous release's directory on disk until the next
     * ingestion -- see that class's javadoc.
     */
    Path releaseVcfPath(UUID releaseId) {
        return root.resolve(RELEASES_DIR_NAME).resolve(releaseId.toString()).resolve(VCF_FILENAME);
    }
}
