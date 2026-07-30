package com.adamastorx.api.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
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
 * Proves backlog#45's distinguishing-signal AC against the real {@code
 * /actuator/prometheus} output, not the implementation's say-so: a request
 * carrying {@link SyntheticTrafficObservationConvention#SYNTHETIC_USER_AGENT_PREFIX}
 * as its {@code User-Agent} shows up tagged {@code
 * traffic_source="synthetic"} on {@code http_server_requests_seconds_count};
 * an ordinary request -- this test's own other {@code RestTestClient} calls
 * included -- shows up {@code traffic_source="real"}. Same
 * Postgres/Redis/Kafka scaffolding as {@code WorkItemCacheIntegrationTest}/
 * {@code VariantLookupIntegrationTest}, since {@code api}'s full context
 * still needs all three to start regardless of what this test exercises.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class SyntheticTrafficObservationConventionTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @LocalServerPort
    private int port;

    private RestTestClient client() {
        return RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void syntheticUserAgentIsTaggedSeparatelyFromRealTraffic() {
        // A "synthetic" call, shaped like services/workload-generator's own.
        client()
                .get()
                .uri("/work-items")
                .header("User-Agent", SyntheticTrafficObservationConvention.SYNTHETIC_USER_AGENT_PREFIX + "1.0")
                .exchange()
                .expectStatus()
                .isOk();

        // A plain call with whatever default User-Agent RestTestClient
        // sends (never the synthetic prefix) -- this is the "real manual
        // traffic" side of the AC.
        client().get().uri("/work-items/" + UUID.randomUUID()).exchange().expectStatus().isNotFound();

        String metrics = client()
                .get()
                .uri("/actuator/prometheus")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(metrics).contains("traffic_source=\"synthetic\"");
        assertThat(metrics).contains("traffic_source=\"real\"");
    }
}
