package com.adamastorx.workers.clinvar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Weekly in-process trigger (services#25, ADR 0018) -- deliberately
 * {@code @Scheduled}, not a Kubernetes {@code CronJob}: a {@code CronJob}
 * spawns a {@code Job} under the hood, which on a literal reading would
 * violate this same ADR's "no Kubernetes Jobs" boundary, and brings a
 * K8s primitive this project has never used (missed-schedule handling,
 * concurrency policy, job-history GC, RBAC for job creation) for no
 * benefit here. Requires {@code @EnableScheduling} on
 * {@code WorkersApplication}.
 *
 * <p>A failure here is caught and logged rather than left to propagate
 * out of the scheduled method (Spring's default scheduled-task error
 * handling just logs and moves on regardless, but doing it explicitly
 * here keeps the "ingestion failures are observable" AC visibly true at
 * this call site rather than relying on framework default behaviour) --
 * {@link ClinVarIngestionService#ingest} has already incremented the
 * failure counter and logged the underlying cause before this catches it.
 */
@Component
class ClinVarIngestionScheduler {

    private static final Logger log = LoggerFactory.getLogger(ClinVarIngestionScheduler.class);

    private final ClinVarIngestionService ingestionService;

    ClinVarIngestionScheduler(ClinVarIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @Scheduled(cron = "${app.clinvar.ingestion-cron}")
    void triggerScheduledIngestion() {
        try {
            ingestionService.ingest();
        } catch (RuntimeException ex) {
            log.error("Scheduled ClinVar ingestion failed -- will retry on the next scheduled run", ex);
        }
    }
}
