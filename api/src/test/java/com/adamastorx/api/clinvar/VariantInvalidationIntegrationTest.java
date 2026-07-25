package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
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
 * Proves services#26's AC end to end (ADR 0018): a seeded stale Redis
 * entry (release N's classification, "Uncertain_significance") is
 * evicted immediately after a real {@code clinvar.ingestion.completed}
 * Kafka event announces release N+1 (a deliberately constructed fixture
 * pair where the same coordinate's classification flips to "Pathogenic",
 * not real historical ClinVar data -- see {@code
 * fixture-invalidation-release-n.vcf}/{@code -n2.vcf}), verified via the
 * real {@code cache.invalidations} counter read off {@code
 * MeterRegistry} (the same registry backing {@code /actuator/prometheus},
 * not a mock), a live Redis check, and a subsequent lookup that
 * repopulates the cache with the new classification and new
 * {@code clinvarReleaseId}.
 *
 * <p>The event is sent over a real embedded Kafka broker in the exact
 * wire format {@code workers} actually produces (JSON, no type headers)
 * and consumed by the real {@code ClinVarCacheInvalidationListener} --
 * this test never calls {@code VariantInvalidationService} directly.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"work-items", "clinvar.ingestion.completed"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VariantInvalidationIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    private static final UUID RELEASE_N = UUID.randomUUID();
    private static final UUID RELEASE_N2 = UUID.randomUUID();
    private static final String CACHE_KEY = "variantAnnotation:7:117559600:C:T";

    private static Path refdataRoot;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) throws Exception {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));

        refdataRoot = Files.createTempDirectory("clinvar-invalidation-refdata");
        registry.add("app.clinvar.refdata-path", () -> refdataRoot.toString());
    }

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RedisTemplate<String, VariantAnnotation> variantAnnotationRedisTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private RestTestClient client;

    @BeforeAll
    void seedReleasesFixturesAndCache() throws Exception {
        // Both releases' files on disk, exactly as workers' retention
        // policy would leave them after ingesting N+1 (see
        // workers.clinvar.ClinVarRefdataPaths' pruneOtherThan javadoc).
        ClinVarFixtureSupport.bgzipAndIndex(
                "clinvar/fixture-invalidation-release-n.vcf", releaseDir(RELEASE_N));
        ClinVarFixtureSupport.bgzipAndIndex(
                "clinvar/fixture-invalidation-release-n2.vcf", releaseDir(RELEASE_N2));
        // `current` -> release N+1's directory, exactly as workers'
        // ClinVarRefdataPaths.flipCurrent would have already done by the
        // time this event is published (ADR 0018's ordering).
        copyDirectory(releaseDir(RELEASE_N2), refdataRoot.resolve("current"));

        insertRelease(RELEASE_N, "2026-06-01", false);
        insertRelease(RELEASE_N2, "2026-07-06", true);

        VariantAnnotation staleAnnotation = new VariantAnnotation(
                "7", 117559600, "C", "T", "rs900000001", "Uncertain_significance",
                "criteria_provided,_single_submitter", null, RELEASE_N.toString());
        variantAnnotationRedisTemplate.opsForValue().set(CACHE_KEY, staleAnnotation);
    }

    private Path releaseDir(UUID releaseId) {
        return refdataRoot.resolve("releases").resolve(releaseId.toString());
    }

    private static void copyDirectory(Path source, Path target) throws Exception {
        Files.createDirectories(target);
        try (var stream = Files.list(source)) {
            for (Path file : stream.toList()) {
                Files.copy(file, target.resolve(file.getFileName()));
            }
        }
    }

    private void insertRelease(UUID releaseId, String publishedDate, boolean active) {
        jdbcTemplate.update(
                "INSERT INTO clinvar_release "
                        + "(release_id, source_url, file_sha256, published_date, variant_count, is_active) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                releaseId,
                "https://example.invalid/fixture",
                "0".repeat(64),
                java.sql.Date.valueOf(publishedDate),
                1L,
                active);
    }

    private RestTestClient client() {
        if (client == null) {
            client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        }
        return client;
    }

    private double invalidationCounter() {
        var counter = meterRegistry
                .find("cache.invalidations")
                .tag("cache", "variant-annotation")
                .tag("reason", "release-changed")
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    @Test
    void staleCacheEntryIsEvictedAfterReleaseChangeAndRepopulatesWithNewClassification() throws Exception {
        double invalidationsBefore = invalidationCounter();
        assertThat(variantAnnotationRedisTemplate.hasKey(CACHE_KEY)).isTrue();

        publishIngestionCompletedEvent();

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(variantAnnotationRedisTemplate.hasKey(CACHE_KEY)).isFalse());

        assertThat(invalidationCounter()).isEqualTo(invalidationsBefore + 1);

        VariantAnnotation refreshed = client()
                .get()
                .uri("/variants/lookup?chrom=7&pos=117559600&ref=C&alt=T")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(VariantAnnotation.class)
                .returnResult()
                .getResponseBody();

        assertThat(refreshed).isNotNull();
        assertThat(refreshed.clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(refreshed.clinvarReleaseId()).isEqualTo(RELEASE_N2.toString());
    }

    private void publishIngestionCompletedEvent() throws Exception {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // Publish using this module's own event record (api's copy), same
        // wire shape as workers' -- see that class's javadoc.
        DefaultKafkaProducerFactory<String, ClinVarIngestionCompletedEvent> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        try {
            KafkaTemplate<String, ClinVarIngestionCompletedEvent> template = new KafkaTemplate<>(producerFactory);
            ClinVarIngestionCompletedEvent event = new ClinVarIngestionCompletedEvent(
                    RELEASE_N2.toString(), RELEASE_N.toString(), "2026-07-06", 1L, java.time.Instant.now().toString());
            template.send("clinvar.ingestion.completed", event).get(10, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            producerFactory.destroy();
        }
    }
}
