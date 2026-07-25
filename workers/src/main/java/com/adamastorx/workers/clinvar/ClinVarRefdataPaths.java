package com.adamastorx.workers.clinvar;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Filesystem layout on the shared RWX PVC (platform#35, ADR 0018):
 *
 * <pre>
 * {refdata-path}/
 *   releases/
 *     {releaseId}/
 *       clinvar.vcf.gz
 *       clinvar.vcf.gz.tbi
 *   current -&gt; releases/{releaseId}   (symlink, relative target)
 * </pre>
 *
 * <p>{@link #flipCurrent} is the mechanism behind ADR 0018's "readers
 * never see a half-written release" guarantee. A symlink can't be
 * replaced atomically in place ({@link Files#createSymbolicLink} throws
 * if the target path already exists) -- the standard POSIX trick used
 * here instead is to create a new symlink under a temporary name and then
 * {@link Files#move} it onto {@code current} with {@link
 * StandardCopyOption#ATOMIC_MOVE}, which renames the symlink itself (not
 * its target) as a single filesystem operation. A reader that opens
 * {@code current/clinvar.vcf.gz} either sees the fully-written old release
 * or the fully-written new one, never a torn/partial state.
 *
 * <p>Retention: only the current release and the immediately-previous one
 * are kept on disk ({@link #pruneOtherThan}) -- the PVC is sized (~2Gi,
 * ADR 0018) for exactly this double-buffered download-then-swap, and
 * services#26's cache invalidation deliberately needs both the old and
 * new release's tabix files available at once to diff changed
 * classifications, which is the concrete reason "previous" specifically
 * (not just "current") has to survive an ingestion, not merely "keep
 * everything" or "keep only current".
 */
@Component
class ClinVarRefdataPaths {

    private static final Logger log = LoggerFactory.getLogger(ClinVarRefdataPaths.class);

    static final String VCF_FILENAME = "clinvar.vcf.gz";
    static final String TBI_FILENAME = "clinvar.vcf.gz.tbi";
    private static final String CURRENT_LINK_NAME = "current";
    private static final String RELEASES_DIR_NAME = "releases";

    private final Path root;

    ClinVarRefdataPaths(@Value("${app.clinvar.refdata-path}") String refdataPath) {
        this.root = Path.of(refdataPath);
    }

    Path releaseDir(UUID releaseId) {
        return root.resolve(RELEASES_DIR_NAME).resolve(releaseId.toString());
    }

    Path vcfPath(UUID releaseId) {
        return releaseDir(releaseId).resolve(VCF_FILENAME);
    }

    Path tbiPath(UUID releaseId) {
        return releaseDir(releaseId).resolve(TBI_FILENAME);
    }

    Path currentLink() {
        return root.resolve(CURRENT_LINK_NAME);
    }

    /** Resolves the VCF path a reader should query right now, following {@code current}. */
    Path currentVcfPath() {
        return currentLink().resolve(VCF_FILENAME);
    }

    /**
     * Atomically repoints {@code current} at {@code releaseDir(releaseId)}.
     * Must only be called after the corresponding {@code clinvar_release}
     * Postgres row has committed (ADR 0018's ordering requirement) --
     * enforced by caller discipline in {@link ClinVarIngestionService},
     * not by this method itself.
     */
    void flipCurrent(UUID releaseId) {
        try {
            Files.createDirectories(root);
            Path relativeTarget = Path.of(RELEASES_DIR_NAME, releaseId.toString());
            Path tempLink = root.resolve(CURRENT_LINK_NAME + ".tmp-" + UUID.randomUUID());
            Files.createSymbolicLink(tempLink, relativeTarget);
            Files.move(tempLink, currentLink(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            log.info("Flipped {} -> {}", currentLink(), relativeTarget);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to flip current pointer to release " + releaseId, ex);
        }
    }

    /** Best-effort: which release the {@code current} symlink pointed at before this flip, if any. */
    UUID currentReleaseIdOrNull() {
        if (!Files.exists(currentLink())) {
            return null;
        }
        try {
            Path target = Files.readSymbolicLink(currentLink());
            String name = target.getFileName().toString();
            return UUID.fromString(name);
        } catch (IOException | IllegalArgumentException ex) {
            log.warn("Could not resolve current release id from {}", currentLink(), ex);
            return null;
        }
    }

    /** Deletes every release subdirectory except the ones in {@code keep}. */
    void pruneOtherThan(Set<UUID> keep) {
        Path releasesDir = root.resolve(RELEASES_DIR_NAME);
        if (!Files.isDirectory(releasesDir)) {
            return;
        }
        Set<String> keepNames = new HashSet<>();
        keep.forEach(id -> keepNames.add(id.toString()));

        try (Stream<Path> children = Files.list(releasesDir)) {
            children.filter(Files::isDirectory)
                    .filter(dir -> !keepNames.contains(dir.getFileName().toString()))
                    .forEach(this::deleteRecursively);
        } catch (IOException ex) {
            log.warn("Failed to list {} for pruning old releases", releasesDir, ex);
        }
    }

    private void deleteRecursively(Path dir) {
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException ex) {
                    log.warn("Failed to delete {} while pruning old release {}", path, dir, ex);
                }
            });
            log.info("Pruned old release directory {}", dir);
        } catch (IOException ex) {
            log.warn("Failed to walk {} for pruning", dir, ex);
        }
    }
}
