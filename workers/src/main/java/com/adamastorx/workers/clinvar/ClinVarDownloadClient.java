package com.adamastorx.workers.clinvar;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Plain {@code java.net.http.HttpClient} downloader (services#25, ADR
 * 0018) -- no new HTTP-client dependency needed, the JDK's own client is
 * sufficient for a handful of large, infrequent (weekly), unauthenticated
 * GETs. Not built on Spring's {@code RestClient}: that's tuned for small
 * JSON request/response bodies against this project's own services, not
 * streaming a ~250MB file to disk with resume support.
 *
 * <p><strong>Resumable</strong>: if the destination file already has
 * partial content (a previous attempt died mid-download -- process
 * restart, network blip), a {@code Range: bytes=<n>-} request continues
 * from the existing length instead of restarting from zero. A server that
 * doesn't honor {@code Range} (responds {@code 200} instead of
 * {@code 206}) is detected from the response status and falls back to a
 * full re-download rather than corrupting the file by blindly appending.
 */
@Component
class ClinVarDownloadClient {

    private static final Logger log = LoggerFactory.getLogger(ClinVarDownloadClient.class);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    /**
     * Downloads {@code source} to {@code destination}, resuming a partial
     * prior attempt if one exists. Returns the SHA-256 of the file's full
     * content (recomputed from the start even on a resumed download, so
     * the digest always reflects the complete file, not just the bytes
     * fetched in this call).
     */
    String download(URI source, Path destination) throws IOException, InterruptedException {
        Files.createDirectories(destination.getParent());

        long existingLength = Files.exists(destination) ? Files.size(destination) : 0L;
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(source).GET().timeout(Duration.ofMinutes(10));
        if (existingLength > 0) {
            requestBuilder.header("Range", "bytes=" + existingLength + "-");
        }

        HttpResponse<InputStream> response =
                httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());

        boolean resumed = existingLength > 0 && response.statusCode() == 206;
        if (existingLength > 0 && response.statusCode() == 200) {
            log.warn(
                    "Server for {} does not support Range requests (got 200, not 206) -- "
                            + "restarting download from scratch instead of risking a corrupt append",
                    source);
        }
        StandardOpenOption writeMode = resumed ? StandardOpenOption.APPEND : StandardOpenOption.CREATE;
        if (!resumed) {
            Files.deleteIfExists(destination);
        }

        try (InputStream in = response.body();
                OutputStream out = Files.newOutputStream(destination, StandardOpenOption.WRITE, writeMode)) {
            in.transferTo(out);
        }

        return sha256Hex(destination);
    }

    /**
     * Best-effort fetch of a small text sidecar file (the checksum
     * companions NCBI publishes alongside the main VCF/index). Absent
     * entirely from {@code Optional} on any failure -- a 404, a network
     * error, anything -- rather than throwing: the caller
     * ({@link ClinVarTabixIndexer#validate}) treats "no checksum
     * available to validate against" as equivalent to "validation
     * failed", the safe default (rebuild rather than trust an
     * unverifiable file).
     */
    Optional<String> fetchOptionalText(URI source) {
        try {
            HttpRequest request =
                    HttpRequest.newBuilder(source).GET().timeout(Duration.ofSeconds(30)).build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.info("Checksum sidecar {} returned HTTP {} -- treating as unavailable", source, response.statusCode());
                return Optional.empty();
            }
            return Optional.of(response.body().trim());
        } catch (IOException | InterruptedException ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.info("Checksum sidecar {} unreachable -- treating as unavailable ({})", source, ex.toString());
            return Optional.empty();
        }
    }

    static String sha256Hex(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available on this JVM", ex);
        }
        try (var in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
