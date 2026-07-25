package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
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
 * <p>Seeding uses a plain JDBC {@link Connection} built directly from the
 * {@code postgres} container's own coordinates in a static
 * {@code @BeforeAll}, not an {@code @Autowired JdbcTemplate} -- an earlier
 * version used {@code @TestInstance(PER_CLASS)} to reach an injected bean
 * from a non-static {@code @BeforeAll}, but that lifecycle changes when
 * JUnit must construct the test instance (and therefore run Spring's
 * {@code postProcessTestInstance}/context-loading callbacks, which is
 * where {@code @DynamicPropertySource} gets evaluated) relative to
 * {@code @Testcontainers}' own container-starting {@code beforeAll}
 * callback -- with PER_CLASS, context loading raced container startup and
 * {@code redis.getMappedPort(6379)} was called before {@code redis.start()}
 * had run, throwing {@code IllegalStateException: Mapped port can only be
 * obtained after the container is started}. Default PER_METHOD lifecycle
 * plus a plain JDBC connection sidesteps the whole ordering question --
 * nothing here needs the Spring context to exist yet.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"work-items", "clinvar.ingestion.completed"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class VariantLookupIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    private static final UUID RELEASE_ID = UUID.randomUUID();

    // Created eagerly at class-load time (a static field initializer, not a
    // @DynamicPropertySource side effect) so it exists before either
    // @BeforeAll or @DynamicPropertySource run -- JUnit's @BeforeAll fires
    // on pure JUnit lifecycle, independent of and *before* Spring's context
    // loading (where @DynamicPropertySource actually gets evaluated) for
    // the default PER_METHOD lifecycle. Stashing the directory as a side
    // effect of @DynamicPropertySource (as an earlier version of this test
    // did) left @BeforeAll reading a still-null field.
    private static final Path refdataRoot = createRefdataRoot();

    private static Path createRefdataRoot() {
        try {
            return Files.createTempDirectory("clinvar-refdata-root");
        } catch (java.io.IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add("app.clinvar.refdata-path", refdataRoot::toString);
    }

    @LocalServerPort
    private int port;

    private RestTestClient client;

    @BeforeAll
    static void seedReleaseAndFixture() throws Exception {
        Path currentDir = refdataRoot.resolve("current");
        ClinVarFixtureSupport.bgzipAndIndex("clinvar/fixture-release-1.vcf", currentDir);

        try (Connection connection = DriverManager.getConnection(
                        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO clinvar_release "
                            + "(release_id, source_url, file_sha256, published_date, variant_count, is_active) "
                            + "VALUES (?, ?, ?, ?, ?, true)")) {
                statement.setObject(1, RELEASE_ID);
                statement.setString(2, "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz");
                statement.setString(3, "0".repeat(64));
                statement.setDate(4, java.sql.Date.valueOf("2026-07-20"));
                statement.setLong(5, 2L);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                            + "VALUES (?, ?, ?, ?, ?, ?)")) {
                statement.setString(1, "rs80357906");
                statement.setString(2, "17");
                statement.setInt(3, 43057062);
                statement.setString(4, "T");
                statement.setString(5, "TG");
                statement.setObject(6, RELEASE_ID);
                statement.executeUpdate();

                statement.setString(1, "rs80359550");
                statement.setString(2, "13");
                statement.setInt(3, 32340300);
                statement.setString(4, "GT");
                statement.setString(5, "G");
                statement.setObject(6, RELEASE_ID);
                statement.executeUpdate();
            }
        }
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
