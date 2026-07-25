package com.adamastorx.api.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
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
 * Proves the two AC halves ADR 0016 exists to satisfy that
 * {@code WorkItemControllerIntegrationTest} doesn't cover: the hit/miss
 * metric is real (services#5 AC: "not just implemented, but visible"),
 * and a Redis outage fails the read open to PostgreSQL rather than failing
 * the request (services#5 AC, explicit and tested — this is that test).
 *
 * <p>No official Testcontainers Redis module exists (ADR 0016, pom.xml
 * comment) — {@code redis} here is a plain {@link GenericContainer} wired
 * via {@link DynamicPropertySource}, not {@code @ServiceConnection}.
 *
 * <p>{@code @EmbeddedKafka} + the {@code spring.kafka.bootstrap-servers}
 * override are needed here too, same as
 * {@code WorkItemControllerIntegrationTest} — {@code create()} below calls
 * the real {@code POST /work-items}, which always publishes to Kafka
 * regardless of this test's actual subject. Without this, the producer
 * blocks the request thread on metadata resolution against the default
 * (unreachable in CI) {@code kafka.kafka.svc.cluster.local:9092} for
 * {@code max.block.ms} (60s) before failing the whole request with a 500 —
 * found exactly this way, not assumed, the first time this test ran in CI.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class WorkItemCacheIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @LocalServerPort
    private int port;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void firstReadIsAMissSecondReadIsAHit() {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        WorkItem created = create(client, "hello cache");

        double missesBefore = counter("miss");
        double hitsBefore = counter("hit");

        client.get()
                .uri("/work-items/{id}", created.id())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(WorkItem.class)
                .isEqualTo(created);
        client.get()
                .uri("/work-items/{id}", created.id())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(WorkItem.class)
                .isEqualTo(created);

        assertThat(counter("miss")).isEqualTo(missesBefore + 1);
        assertThat(counter("hit")).isEqualTo(hitsBefore + 1);
    }

    @Test
    void redisOutageFailsOpenToPostgresInsteadOfFailingTheRequest() {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        WorkItem created = create(client, "survives an outage");

        // Warm/confirm the normal path first.
        client.get().uri("/work-items/{id}", created.id()).exchange().expectStatus().isOk();

        double errorsBefore = counter("error");
        redis.stop();

        // The read must still succeed -- served from PostgreSQL, not from
        // a dead cache -- this is the actual AC, not a health check.
        WorkItem fetched = client.get()
                .uri("/work-items/{id}", created.id())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(WorkItem.class)
                .returnResult()
                .getResponseBody();

        assertThat(fetched).isEqualTo(created);
        assertThat(counter("error")).isGreaterThan(errorsBefore);
    }

    private static WorkItem create(RestTestClient client, String message) {
        return client.post()
                .uri("/work-items")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("message", message))
                .exchange()
                .expectStatus()
                .isEqualTo(202)
                .expectBody(WorkItem.class)
                .returnResult()
                .getResponseBody();
    }

    private double counter(String result) {
        var counter = meterRegistry.find("cache.gets").tag("cache", "work-items").tag("result", result).counter();
        return counter == null ? 0.0 : counter.count();
    }
}
