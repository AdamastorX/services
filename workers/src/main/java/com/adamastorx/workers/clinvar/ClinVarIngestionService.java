package com.adamastorx.workers.clinvar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Orchestrates one full ClinVar ingestion (services#25, ADR 0018):
 * download, validate/rebuild the tabix index, build the rsID lookup
 * table, commit provenance to Postgres, flip the filesystem
 * {@code current} pointer, prune old releases, publish the completion
 * event. Called by both {@link ClinVarIngestionScheduler} (weekly,
 * automatic) and {@link ClinVarIngestionController} (manual, dev/CI) --
 * both are thin triggers over this one path, no separate logic.
 *
 * <p><strong>Ordering is the whole point of this class</strong> (ADR
 * 0018's "readers never see a half-written release"): everything that can
 * fail happens against the *new* release's own private directory first;
 * only {@link ClinVarReleaseActivationService#activate} touches anything
 * a reader could already be looking at (the {@code clinvar_release}
 * table), and the filesystem {@code current} symlink -- the other thing
 * readers actually consult -- only moves after that transaction has
 * committed. A failure at any earlier step leaves {@code current}
 * pointing exactly where it did before this method was ever called.
 *
 * <p>Failures are never swallowed (ADR 0018 AC: "ingestion failures are
 * observable, not silent") -- every exception is logged, counted in
 * {@code clinvar.ingestion.failures}, and rethrown, so both the scheduled
 * trigger (visible in logs/metrics) and the manual endpoint (visible as a
 * 500) surface it.
 */
@Service
public class ClinVarIngestionService {

    private static final Logger log = LoggerFactory.getLogger(ClinVarIngestionService.class);

    private final ClinVarDownloadClient downloadClient;
    private final ClinVarTabixIndexer tabixIndexer;
    private final ClinVarVariantIndexBuilder variantIndexBuilder;
    private final ClinVarReleaseActivationService activationService;
    private final ClinVarRefdataPaths paths;
    private final ClinVarIngestionProducer producer;
    private final URI sourceVcfUrl;
    private final URI sourceTbiUrl;

    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter tbiRebuildCounter;

    public ClinVarIngestionService(
            ClinVarDownloadClient downloadClient,
            ClinVarTabixIndexer tabixIndexer,
            ClinVarVariantIndexBuilder variantIndexBuilder,
            ClinVarReleaseActivationService activationService,
            ClinVarRefdataPaths paths,
            ClinVarIngestionProducer producer,
            @Value("${app.clinvar.source-vcf-url}") String sourceVcfUrl,
            @Value("${app.clinvar.source-tbi-url}") String sourceTbiUrl,
            MeterRegistry meterRegistry) {
        this.downloadClient = downloadClient;
        this.tabixIndexer = tabixIndexer;
        this.variantIndexBuilder = variantIndexBuilder;
        this.activationService = activationService;
        this.paths = paths;
        this.producer = producer;
        this.sourceVcfUrl = URI.create(sourceVcfUrl);
        this.sourceTbiUrl = URI.create(sourceTbiUrl);
        this.successCounter = meterRegistry.counter("clinvar.ingestion.completed", "result", "success");
        this.failureCounter = meterRegistry.counter("clinvar.ingestion.completed", "result", "failure");
        this.tbiRebuildCounter = meterRegistry.counter("clinvar.ingestion.tbi_rebuilds");
    }

    /** Runs one full ingestion. Returns the new release's id. */
    public UUID ingest() {
        UUID releaseId = UUID.randomUUID();
        try {
            UUID result = doIngest(releaseId);
            successCounter.increment();
            return result;
        } catch (Exception ex) {
            failureCounter.increment();
            log.error("ClinVar ingestion failed (attempted release {})", releaseId, ex);
            throw new ClinVarIngestionException("ClinVar ingestion failed for attempted release " + releaseId, ex);
        }
    }

    private UUID doIngest(UUID releaseId) throws Exception {
        Path vcfPath = paths.vcfPath(releaseId);
        Path tbiPath = paths.tbiPath(releaseId);

        log.info("Starting ClinVar ingestion, release {}", releaseId);

        String fileSha256 = downloadClient.download(sourceVcfUrl, vcfPath);
        downloadClient.download(sourceTbiUrl, tbiPath);

        URI tbiChecksumUrl = URI.create(sourceTbiUrl.toString() + ".md5");
        if (!tabixIndexer.validate(tbiPath, tbiChecksumUrl)) {
            log.warn("Published .tbi failed validation for release {} -- rebuilding via htsjdk", releaseId);
            tabixIndexer.rebuild(vcfPath);
            tbiRebuildCounter.increment();
        }

        LocalDate publishedDate = ClinVarVcfHeaderReader.readPublishedDate(vcfPath);
        long variantCount = variantIndexBuilder.build(vcfPath, releaseId);

        UUID previousReleaseId =
                activationService.currentActive().map(ClinVarRelease::getReleaseId).orElse(null);

        ClinVarRelease newRelease =
                new ClinVarRelease(releaseId, sourceVcfUrl.toString(), fileSha256, publishedDate, Instant.now(), variantCount, true);
        // Postgres commit -- must happen before the filesystem pointer
        // moves (ADR 0018's ordering requirement, see class javadoc).
        activationService.activate(newRelease);

        paths.flipCurrent(releaseId);

        Set<UUID> keep = new HashSet<>();
        keep.add(releaseId);
        if (previousReleaseId != null) {
            keep.add(previousReleaseId);
        }
        paths.pruneOtherThan(keep);
        variantIndexBuilder.pruneOtherReleases(releaseId);

        producer.publish(new ClinVarIngestionCompletedEvent(
                releaseId.toString(),
                previousReleaseId == null ? null : previousReleaseId.toString(),
                publishedDate.toString(),
                variantCount,
                Instant.now().toString()));

        log.info(
                "Completed ClinVar ingestion: release={} previousRelease={} publishedDate={} variantCount={}",
                releaseId,
                previousReleaseId,
                publishedDate,
                variantCount);
        return releaseId;
    }

    /** Wraps any failure during {@link #ingest()} so callers see one consistent, unchecked exception type. */
    public static class ClinVarIngestionException extends RuntimeException {
        ClinVarIngestionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
