package com.adamastorx.workers.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit-level (no Spring context, no Postgres) proof of services#25's
 * checksum-validation AC: a correct published checksum validates, a wrong
 * one doesn't. A tiny local {@link HttpServer} (JDK built-in, no new test
 * dependency) stands in for NCBI's checksum sidecar endpoint.
 */
class ClinVarTabixIndexerTest {

    private HttpServer server;
    private volatile String checksumResponseBody;

    @TempDir
    private Path tempDir;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/checksum", exchange -> {
            byte[] body = checksumResponseBody.getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void validatesWhenPublishedChecksumMatches() throws Exception {
        Path tbi = tempDir.resolve("clinvar.vcf.gz.tbi");
        Files.writeString(tbi, "not a real tabix index, just checksum test content");
        checksumResponseBody = md5Hex(tbi) + "  clinvar.vcf.gz.tbi\n";

        ClinVarDownloadClient downloadClient = new ClinVarDownloadClient();
        ClinVarTabixIndexer indexer = new ClinVarTabixIndexer(downloadClient);

        boolean valid = indexer.validate(tbi, URI.create(baseUrl() + "/checksum"));

        assertThat(valid).isTrue();
    }

    @Test
    void failsValidationWhenPublishedChecksumDoesNotMatch() throws Exception {
        Path tbi = tempDir.resolve("clinvar.vcf.gz.tbi");
        Files.writeString(tbi, "not a real tabix index, just checksum test content");
        checksumResponseBody = "0000000000000000000000000000000000  clinvar.vcf.gz.tbi\n";

        ClinVarDownloadClient downloadClient = new ClinVarDownloadClient();
        ClinVarTabixIndexer indexer = new ClinVarTabixIndexer(downloadClient);

        boolean valid = indexer.validate(tbi, URI.create(baseUrl() + "/checksum"));

        assertThat(valid).isFalse();
    }

    @Test
    void rebuildsAWorkingTabixIndexFromABgzippedVcf() throws Exception {
        Path releaseDir = tempDir.resolve("release");
        ClinVarFixtureSupport.bgzipAndIndex("clinvar/fixture-release-1.vcf", releaseDir);
        Path vcfGz = releaseDir.resolve(ClinVarRefdataPaths.VCF_FILENAME);
        // Simulate "the published .tbi was bad" by deleting the one
        // ClinVarFixtureSupport already built, then proving rebuild()
        // produces a fresh, working one from scratch.
        Files.delete(Path.of(vcfGz + ".tbi"));

        ClinVarTabixIndexer indexer = new ClinVarTabixIndexer(new ClinVarDownloadClient());
        Path rebuiltTbi = indexer.rebuild(vcfGz);

        assertThat(Files.exists(rebuiltTbi)).isTrue();
        assertThat(Files.size(rebuiltTbi)).isGreaterThan(0);
    }

    private String baseUrl() {
        return "http://localhost:" + server.getAddress().getPort();
    }

    private static String md5Hex(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(Files.readAllBytes(file));
        return HexFormat.of().formatHex(digest.digest());
    }
}
