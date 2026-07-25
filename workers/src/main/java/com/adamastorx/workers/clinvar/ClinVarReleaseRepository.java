package com.adamastorx.workers.clinvar;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

interface ClinVarReleaseRepository extends JpaRepository<ClinVarRelease, UUID> {

    Optional<ClinVarRelease> findByActiveTrue();
}
