package com.adamastorx.workers.clinvar;

import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The single transactional boundary in the ingestion flow (services#25,
 * ADR 0018): deactivates the previous active release (if any) and inserts
 * the new one as active, both in one commit. Deliberately its own bean
 * rather than a {@code @Transactional} method on {@link ClinVarIngestionService}
 * itself -- Spring's transactional proxy only intercepts calls that come
 * in through the bean's external interface, not self-invocation within
 * the same class, so an in-class {@code @Transactional} method called via
 * {@code this.activate(...)} would silently run non-transactionally. A
 * separate bean sidesteps that gotcha instead of relying on everyone who
 * touches this code remembering it.
 *
 * <p>{@link ClinVarIngestionService} calls this <em>before</em> flipping
 * the filesystem {@code current} pointer (see
 * {@link ClinVarRefdataPaths#flipCurrent}) -- ADR 0018's ordering
 * requirement is that readers never see a half-written release, which
 * means the Postgres commit here has to happen first.
 */
@Service
class ClinVarReleaseActivationService {

    private final ClinVarReleaseRepository releaseRepository;

    ClinVarReleaseActivationService(ClinVarReleaseRepository releaseRepository) {
        this.releaseRepository = releaseRepository;
    }

    @Transactional
    void activate(ClinVarRelease newRelease) {
        // Flip the old row to inactive first, then insert the new one as
        // active -- clinvar_release's partial unique index
        // (uq_clinvar_release_active) only ever allows one is_active=true
        // row to exist at a time, so doing this the other way round would
        // violate it mid-transaction.
        Optional<ClinVarRelease> previousActive = releaseRepository.findByActiveTrue();
        previousActive.ifPresent(previous -> {
            previous.setActive(false);
            releaseRepository.save(previous);
        });
        releaseRepository.save(newRelease);
    }

    Optional<ClinVarRelease> currentActive() {
        return releaseRepository.findByActiveTrue();
    }
}
