package com.adamastorx.workers.clinvar;

import htsjdk.tribble.index.IndexFactory;
import htsjdk.tribble.index.tabix.TabixFormat;
import htsjdk.tribble.index.tabix.TabixIndex;
import htsjdk.variant.vcf.VCFCodec;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Validates NCBI's published {@code .tbi} against a checksum, rebuilding
 * via htsjdk's {@code TabixIndexer} machinery only if validation fails
 * (services#25, ADR 0018 -- explicit AC, avoids re-indexing a ~250MB file
 * on every ingestion when the published index is almost always fine).
 *
 * <p><strong>Checksum source, stated explicitly</strong>: NCBI's ClinVar
 * FTP layout publishes an MD5 sidecar file (same name, {@code .md5}
 * suffix) alongside most of its downloadable files -- {@link #validate}
 * fetches {@code <tbiUrl>.md5} via {@link ClinVarDownloadClient} and
 * compares it against a locally computed MD5 of the downloaded
 * {@code .tbi}. This is a different check from {@code clinvar_release
 * .file_sha256} (the project's own SHA-256 of the main VCF, computed
 * unconditionally by {@link ClinVarDownloadClient#download} regardless of
 * what NCBI publishes) -- MD5 here purely because that's the algorithm
 * NCBI's sidecar actually uses; it's an integrity check against transfer
 * corruption, not a security control, so MD5's known weaknesses for
 * adversarial contexts don't apply. If the sidecar is missing, unreachable,
 * or doesn't match, {@link #validate} returns {@code false} -- the caller
 * ({@link ClinVarIngestionService}) treats that as "rebuild", the safe
 * default over trusting an unverifiable index.
 */
@Component
class ClinVarTabixIndexer {

    private static final Logger log = LoggerFactory.getLogger(ClinVarTabixIndexer.class);

    private final ClinVarDownloadClient downloadClient;

    ClinVarTabixIndexer(ClinVarDownloadClient downloadClient) {
        this.downloadClient = downloadClient;
    }

    boolean validate(Path tbiPath, URI tbiChecksumUrl) {
        Optional<String> published = downloadClient.fetchOptionalText(tbiChecksumUrl);
        if (published.isEmpty()) {
            log.warn(
                    "No published checksum available at {} -- treating {} as unvalidated",
                    tbiChecksumUrl,
                    tbiPath);
            return false;
        }
        String expected = extractHex(published.get());
        String actual;
        try {
            actual = md5Hex(tbiPath);
        } catch (IOException ex) {
            log.warn("Could not read {} to compute its checksum -- treating as invalid", tbiPath, ex);
            return false;
        }
        boolean matches = expected.equalsIgnoreCase(actual);
        if (!matches) {
            log.warn("Checksum mismatch for {}: expected {} but computed {}", tbiPath, expected, actual);
        }
        return matches;
    }

    /**
     * Rebuilds the {@code .tbi} for an already block-gzip-compressed VCF
     * using htsjdk's {@link IndexFactory}, writing it next to
     * {@code bgzippedVcf} with the conventional {@code .tbi} suffix. The
     * {@code null} sequence-dictionary argument is deliberate, not an
     * oversight -- htsjdk's own javadoc for this overload says it "may be
     * null", used only as a memory-footprint optimization when present;
     * ClinVar's own VCF header carries no {@code ##contig} lines at all
     * (checked directly against a real download, not assumed), so there
     * is no dictionary to pass here even if it were wanted.
     */
    Path rebuild(Path bgzippedVcf) throws IOException {
        VCFCodec codec = new VCFCodec();
        TabixIndex index = IndexFactory.createTabixIndex(bgzippedVcf, codec, TabixFormat.VCF, null);
        Path tbiPath = Path.of(bgzippedVcf + ".tbi");
        index.write(tbiPath);
        log.info("Rebuilt tabix index {} for {}", tbiPath, bgzippedVcf);
        return tbiPath;
    }

    private static String extractHex(String sidecarContent) {
        // Sidecar files are typically "<hex>  <filename>" (coreutils
        // md5sum format) or just the bare hex digest -- handle both.
        String firstToken = sidecarContent.trim().split("\\s+")[0];
        return firstToken;
    }

    private static String md5Hex(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("MD5 not available on this JVM", ex);
        }
        try (var in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
