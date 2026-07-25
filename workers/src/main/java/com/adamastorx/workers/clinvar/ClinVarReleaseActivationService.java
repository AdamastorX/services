package com.adamastorx.workers.clinvar;

import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Transactional boundaries around {@code clinvar_release} (services#25,
 * ADR 0018). Deliberately its own bean rather than {@code @Transactional}
 * methods on {@link ClinVarIngestionService} itself -- Spring's
 * transactional proxy only intercepts calls that come in through the
 * bean's external interface, not self-invocation within the same class,
 * so an in-class {@code @Transactional} method called via {@code
 * this.activate(...)} would silently run non-transactionally. A separate
 * bean sidesteps that gotcha instead of relying on everyone who touches
 * this code remembering it.
 *
 * <p><strong>Two steps, not one</strong> -- {@link #insertPending} then,
 * later, {@link #activate} -- found necessary the hard way (this repo's
 * own CI): {@code clinvar_variant_index} rows carry a foreign key to
 * {@code clinvar_release.release_id}, and {@link ClinVarVariantIndexBuilder#build}
 * has to reference the *new* release's id while writing those rows. A
 * single "insert everything active, at the end" step would mean the
 * variant-index insert runs against a {@code clinvar_release} row that
 * doesn't exist in Postgres yet at all -- an FK violation, not a race.
 * {@link ClinVarIngestionService} therefore calls {@link #insertPending}
 * (a normal, harmless insert -- {@code is_active=false} doesn't touch the
 * one-active-row partial unique index) *before* the variant-index build,
 * then {@link #activate} afterward once the variant count is known --
 * still the single commit that ADR 0018's "readers never see a
 * half-written release" ordering depends on, since {@code
 * findByActiveTrue()} never sees the pending row until {@link #activate}
 * flips it.
 */
@Service
class ClinVarReleaseActivationService {

    private final ClinVarReleaseRepository releaseRepository;

    ClinVarReleaseActivationService(ClinVarReleaseRepository releaseRepository) {
        this.releaseRepository = releaseRepository;
    }

    /** Inserts the new release row inactive, before anything references its id via a foreign key. */
    @Transactional
    void insertPending(ClinVarRelease pendingRelease) {
        releaseRepository.save(pendingRelease);
    }

    /**
     * Sets the final variant count and flips this release active,
     * deactivating whichever release was previously active -- the actual
     * commit {@link ClinVarIngestionService} waits on before flipping the
     * filesystem {@code current} pointer.
     */
    @Transactional
    void activate(UUID releaseId, long variantCount) {
        ClinVarRelease release = releaseRepository
                .findById(releaseId)
                .orElseThrow(() -> new IllegalStateException(
                        "No pending clinvar_release row for " + releaseId + " -- insertPending was never called"));
        release.setVariantCount(variantCount);
        release.setActive(true);

        // Flip the old row to inactive first, then save the new one as
        // active -- clinvar_release's partial unique index
        // (uq_clinvar_release_active) only ever allows one is_active=true
        // row to exist at a time, so doing this the other way round would
        // violate it mid-transaction.
        releaseRepository
                .findByActiveTrue()
                .filter(previous -> !previous.getReleaseId().equals(releaseId))
                .ifPresent(previous -> {
                    previous.setActive(false);
                    releaseRepository.save(previous);
                });
        releaseRepository.save(release);
    }

    Optional<ClinVarRelease> currentActive() {
        return releaseRepository.findByActiveTrue();
    }
}
