package com.adamastorx.workers.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end proof of services#25's ingestion path (ADR 0018): a fixture
 * VCF (real data, trimmed -- see {@code
 * workers/src/test/resources/clinvar/fixture-release-1.vcf}) served over
 * a local {@link HttpServer} standing in for NCBI, downloaded, indexed,
 * scanned into {@code clinvar_variant_index}, committed as an active
 * {@code clinvar_release} row, and pointed at by the filesystem
 * {@code current} symlink -- asserted against real Postgres
 * (Testcontainers) and a real temp-directory filesystem, not mocked.
 *
 * <p>No live NCBI access here or in CI (ADR 0018's own consequences
 * section calls for exactly this: "a small fixture slice for fast CI
 * runs"). The fixture's two records (rs80357906, rs80359550) are real
 * ClinVar data (BRCA1/BRCA2 founder pathogenic variants), fetched and
 * trimmed from a live download while building this fixture -- not
 * fabricated coordinates or classifications.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "clinvar.ingestion.completed")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class ClinVarIngestionServiceIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    private static HttpServer server;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        // workers carries no Flyway of its own (see
        // V2__create_clinvar_tables.sql's header comment) -- this test
        // still needs the clinvar_* schema to exist, so it migrates the
        // Testcontainers Postgres directly with Flyway rather than
        // standing up a second full api Spring context just for that.
        var flyway = org.flywaydb.core.Flyway.configure()
                .dataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();

        Path releaseSourceDir = Files.createTempDirectory("clinvar-fixture-source");
        ClinVarFixtureSupport.bgzipAndIndex("clinvar/fixture-release-1.vcf", releaseSourceDir);
        Path vcfGz = releaseSourceDir.resolve(ClinVarRefdataPaths.VCF_FILENAME);
        Path tbi = Path.of(vcfGz + ".tbi");
        String tbiMd5 = md5Hex(tbi);

        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/clinvar.vcf.gz", exchange -> serveFile(exchange, vcfGz));
        server.createContext("/clinvar.vcf.gz.tbi.md5", exchange -> serveText(exchange, tbiMd5 + "  clinvar.vcf.gz.tbi\n"));
        server.createContext("/clinvar.vcf.gz.tbi", exchange -> serveFile(exchange, tbi));
        server.start();
        int port = server.getAddress().getPort();

        registry.add("app.clinvar.source-vcf-url", () -> "http://localhost:" + port + "/clinvar.vcf.gz");
        registry.add("app.clinvar.source-tbi-url", () -> "http://localhost:" + port + "/clinvar.vcf.gz.tbi");

        Path refdataRoot = Files.createTempDirectory("clinvar-refdata-root");
        registry.add("app.clinvar.refdata-path", () -> refdataRoot.toString());
    }

    @AfterAll
    static void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Autowired
    private ClinVarIngestionService ingestionService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ClinVarRefdataPaths paths;

    @Test
    void ingestsFixtureReleaseEndToEnd() throws Exception {
        UUID releaseId = ingestionService.ingest();

        // 1. Postgres: exactly one active release, with header-derived
        // published_date (not file mtime) and a variant count matching
        // the fixture's two records.
        List<java.util.Map<String, Object>> releases =
                jdbcTemplate.queryForList("SELECT * FROM clinvar_release WHERE release_id = ?", releaseId);
        assertThat(releases).hasSize(1);
        assertThat(releases.get(0).get("is_active")).isEqualTo(true);
        assertThat(releases.get(0).get("published_date").toString()).isEqualTo("2026-07-20");
        assertThat(((Number) releases.get(0).get("variant_count")).longValue()).isEqualTo(2L);

        // 2. rsID -> coordinate index populated for both fixture records.
        Integer indexRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM clinvar_variant_index WHERE clinvar_release_id = ?", Integer.class, releaseId);
        assertThat(indexRows).isEqualTo(2);

        Integer brca1Rows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM clinvar_variant_index WHERE rsid = 'rs80357906' AND chrom = '17' AND pos = 43057062",
                Integer.class);
        assertThat(brca1Rows).isEqualTo(1);

        // 3. Filesystem: current points at a directory containing both
        // the VCF and its tabix index, only after the above committed.
        Path currentVcf = paths.currentVcfPath();
        assertThat(Files.exists(currentVcf)).isTrue();
        assertThat(Files.exists(Path.of(currentVcf + ".tbi"))).isTrue();
        assertThat(paths.currentReleaseIdOrNull()).isEqualTo(releaseId);
    }

    private static void serveFile(com.sun.net.httpserver.HttpExchange exchange, Path file) throws java.io.IOException {
        byte[] bytes = Files.readAllBytes(file);
        exchange.sendResponseHeaders(200, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void serveText(com.sun.net.httpserver.HttpExchange exchange, String text) throws java.io.IOException {
        byte[] bytes = text.getBytes();
        exchange.sendResponseHeaders(200, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String md5Hex(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        digest.update(Files.readAllBytes(file));
        return HexFormat.of().formatHex(digest.digest());
    }
}
