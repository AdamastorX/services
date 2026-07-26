package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.RedisTemplate;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves cache invalidation's AC end to end (ADR 0019): a seeded stale
 * Redis entry is evicted after a real {@code clinvar.ingestion.completed}
 * event names its key in {@code changedKeys}, verified via the real
 * {@code cache.invalidations} counter read off {@code MeterRegistry} (the
 * same registry backing {@code /actuator/prometheus}, not a mock) and a
 * live Redis check.
 *
 * <p>Drastically simpler than services#26's original version (ADR 0018):
 * that test seeded two whole bgzipped/tabix-indexed VCF fixture releases
 * on disk plus matching {@code clinvar_release} Postgres rows so {@code
 * VariantInvalidationService} could diff them itself. Under ADR 0019,
 * {@code clinvar-service} has already computed that diff by the time this
 * event is published -- {@code changedKeys} names the exact Redis key to
 * delete, so this test only needs to seed that one key directly in Redis
 * and publish the event; no fixture VCFs, no Postgres seeding, no
 * filesystem {@code current}/{@code releases} layout at all.
 *
 * <p>The event is sent over a real embedded Kafka broker in the exact
 * wire format {@code clinvar-service} produces (JSON, no type headers)
 * and consumed by the real {@code ClinVarCacheInvalidationListener} --
 * this test never calls {@code VariantInvalidationService} directly.
 *
 * <p>Seeding lives in {@code @BeforeEach}, not a non-static {@code
 * @TestInstance(PER_CLASS)} {@code @BeforeAll} -- that combination breaks
 * {@code @Testcontainers}/{@code @DynamicPropertySource} ordering (see
 * {@code VariantLookupIntegrationTest}'s javadoc for the full mechanism);
 * this test only has one {@code @Test} method, so {@code @BeforeEach}
 * running "once" is already the natural outcome, no idempotency guard
 * needed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"work-items", "clinvar.ingestion.completed"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
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

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @Autowired
    private RedisTemplate<String, VariantAnnotation> variantAnnotationRedisTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeEach
    void seedStaleCacheEntry() {
        VariantAnnotation staleAnnotation = new VariantAnnotation(
                "7", 117559600, "C", "T", "rs900000001", "Uncertain_significance",
                "criteria_provided,_single_submitter", null, RELEASE_N.toString());
        variantAnnotationRedisTemplate.opsForValue().set(CACHE_KEY, staleAnnotation);
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
    void changedKeyIsEvictedAfterIngestionCompletedEventAndInvalidationCounterIncrements() throws Exception {
        double invalidationsBefore = invalidationCounter();
        assertThat(variantAnnotationRedisTemplate.hasKey(CACHE_KEY)).isTrue();

        publishIngestionCompletedEvent();

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(variantAnnotationRedisTemplate.hasKey(CACHE_KEY))
                        .isFalse());

        assertThat(invalidationCounter()).isEqualTo(invalidationsBefore + 1);
    }

    private void publishIngestionCompletedEvent() throws Exception {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // Publish using this module's own event record -- same wire
        // shape clinvar-service actually produces (ADR 0019).
        DefaultKafkaProducerFactory<String, ClinVarIngestionCompletedEvent> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        try {
            KafkaTemplate<String, ClinVarIngestionCompletedEvent> template = new KafkaTemplate<>(producerFactory);
            ClinVarIngestionCompletedEvent event = new ClinVarIngestionCompletedEvent(
                    RELEASE_N2.toString(),
                    RELEASE_N.toString(),
                    "2026-07-06",
                    1L,
                    java.time.Instant.now().toString(),
                    List.of(CACHE_KEY));
            template.send("clinvar.ingestion.completed", event).get(10, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            producerFactory.destroy();
        }
    }
}
