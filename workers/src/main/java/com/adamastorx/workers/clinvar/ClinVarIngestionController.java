package com.adamastorx.workers.clinvar;

import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Manual admin-triggered re-ingestion endpoint (services#25, ADR 0018
 * explicit AC), alongside {@link ClinVarIngestionScheduler}'s weekly
 * trigger -- for dev/CI use, where waiting for Monday 03:00 isn't
 * practical. Synchronous by design: the caller (a human, or a CI step)
 * gets the new release id back directly, or a 500 with the failure
 * surfaced, rather than having to poll for completion.
 *
 * <p>{@code workers} still gets no Kubernetes {@code Service} (ADR 0009)
 * -- this endpoint is reachable the same way the actuator probes already
 * are (embedded servlet container, no cluster-routed ingress), i.e. via
 * {@code kubectl port-forward} or exec today. Exposing it behind a real
 * routed path (gateway, auth) is a platform/deploy concern out of scope
 * for this issue, not solved here.
 */
@RestController
class ClinVarIngestionController {

    private final ClinVarIngestionService ingestionService;

    ClinVarIngestionController(ClinVarIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/internal/clinvar/ingest")
    ResponseEntity<Map<String, String>> triggerIngestion() {
        UUID releaseId = ingestionService.ingest();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("releaseId", releaseId.toString()));
    }
}
