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
 * Proves the hit/miss half of services#5's AC (ADR 0016): a first
 * {@code GET /work-items/{id}} misses and fills the cache, a second hits —
 * against the real Micrometer counter, not the implementation's say-so.
 *
 * <p>The Redis-outage half lives in
 * {@link WorkItemCacheOutageIntegrationTest} instead, deliberately its own
 * test class with its own Testcontainers/Spring context rather than a
 * second {@code @Test} method here: that test calls {@code redis.stop()}
 * on its container, which — sharing a class-level static container and
 * context (the default {@code @DirtiesContext} class mode is
 * {@code AFTER_CLASS}, not {@code AFTER_EACH_TEST_METHOD}) — would leave
 * Redis dead for whichever other {@code @Test} method in the same class
 * happened to run after it. Found exactly this way in CI: JUnit's default
 * (unordered) method order ran the outage test first, and this test's own
 * hit/miss assertions then failed against a cache that was already down,
 * not a logic bug in the caching code itself. Splitting into two classes
 * removes the shared mutable state instead of pinning method order with
 * {@code @Order}, which would fix today's flake but not the next one this
 * shape of dependency would eventually cause.
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

    // No official Testcontainers module for Redis (ADR 0016, pom.xml
    // comment) -- plain GenericContainer + @DynamicPropertySource instead
    // of @ServiceConnection.
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
