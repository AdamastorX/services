package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves services#24's AC end to end (ADR 0018): both lookup key styles
 * (rsID and coordinates), a not-found case, and the mutually-exclusive
 * parameter validation -- against a known real pathogenic variant,
 * rs80357906 (see {@code src/test/resources/clinvar/fixture-release-1.vcf}),
 * asserting the returned clinical significance is "Pathogenic" and
 * {@code clinvarReleaseId} matches the release this test seeds.
 *
 * <p>The filesystem {@code current} release and its
 * {@code clinvar_release}/{@code clinvar_variant_index} Postgres rows are
 * seeded directly here (a plain directory standing in for the {@code
 * current} symlink, a plain JDBC insert standing in for a real ingestion)
 * rather than by running {@code workers}' ingestion pipeline -- {@code
 * api} has no dependency on {@code workers} (separate Maven modules, ADR
 * 0007), and this test is about the lookup endpoint's own behaviour given
 * the filesystem/Postgres state an ingestion would have already produced,
 * not about re-proving ingestion itself (that's {@code
 * ClinVarIngestionServiceIntegrationTest}, in {@code workers}).
 *
 * <p>{@code @TestInstance(PER_CLASS)}: lets {@link #seedReleaseAndFixture}
 * run once as a non-static {@code @BeforeAll} with the Spring-managed
 * {@link JdbcTemplate} already injected -- the default PER_METHOD
 * lifecycle would require this to be static, which can't reach an
 * {@code @Autowired} field.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"work-items", "clinvar.ingestion.completed"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VariantLookupIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    private static final UUID RELEASE_ID = UUID.randomUUID();
    private static Path refdataRoot;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        refdataRoot = Files.createTempDirectory("clinvar-refdata-root");
        registry.add("app.clinvar.refdata-path", () -> refdataRoot.toString());
    }

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private RestTestClient client;

    @BeforeAll
    void seedReleaseAndFixture() throws Exception {
        Path currentDir = refdataRoot.resolve("current");
        ClinVarFixtureSupport.bgzipAndIndex("clinvar/fixture-release-1.vcf", currentDir);

        jdbcTemplate.update(
                "INSERT INTO clinvar_release "
                        + "(release_id, source_url, file_sha256, published_date, variant_count, is_active) "
                        + "VALUES (?, ?, ?, ?, ?, true)",
                RELEASE_ID,
                "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz",
                "0".repeat(64),
                java.sql.Date.valueOf("2026-07-20"),
                2L);
        jdbcTemplate.update(
                "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                "rs80357906",
                "17",
                43057062,
                "T",
                "TG",
                RELEASE_ID);
        jdbcTemplate.update(
                "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                "rs80359550",
                "13",
                32340300,
                "GT",
                "G",
                RELEASE_ID);
    }

    private RestTestClient client() {
        if (client == null) {
            client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        }
        return client;
    }

    @Test
    void lookupByRsidReturnsPathogenicBrca1Variant() {
        VariantAnnotation annotation = client()
                .get()
                .uri("/variants/lookup?rsid=rs80357906")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(VariantAnnotation.class)
                .returnResult()
                .getResponseBody();

        assertThat(annotation).isNotNull();
        assertThat(annotation.clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(annotation.clinvarReleaseId()).isEqualTo(RELEASE_ID.toString());
        assertThat(annotation.chrom()).isEqualTo("17");
        assertThat(annotation.pos()).isEqualTo(43057062);
    }

    @Test
    void lookupByCoordinatesReturnsSameVariant() {
        VariantAnnotation annotation = client()
                .get()
                .uri("/variants/lookup?chrom=13&pos=32340300&ref=GT&alt=G")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(VariantAnnotation.class)
                .returnResult()
                .getResponseBody();

        assertThat(annotation).isNotNull();
        assertThat(annotation.clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(annotation.rsid()).isEqualTo("rs80359550");
        assertThat(annotation.clinvarReleaseId()).isEqualTo(RELEASE_ID.toString());
    }

    @Test
    void unknownRsidReturnsNotFound() {
        client().get().uri("/variants/lookup?rsid=rs00000000").exchange().expectStatus().isNotFound();
    }

    @Test
    void bothKeyStylesTogetherIsBadRequest() {
        client()
                .get()
                .uri("/variants/lookup?rsid=rs80357906&chrom=17&pos=43057062&ref=T&alt=TG")
                .exchange()
                .expectStatus()
                .isBadRequest();
    }

    @Test
    void neitherKeyStyleIsBadRequest() {
        client().get().uri("/variants/lookup").exchange().expectStatus().isBadRequest();
    }
}
