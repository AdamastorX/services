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
 * Proves the actual hard requirement in services#5's AC (ADR 0016): a
 * Redis outage fails the read <em>open</em> to PostgreSQL, not the
 * request. Its own test class, own Testcontainers/Spring context,
 * deliberately separate from {@link WorkItemCacheIntegrationTest} — this
 * test kills its Redis container on purpose (irreversibly, for the rest
 * of this class's lifetime) to prove the outage path, so it cannot safely
 * share a container/context with any test that expects a working cache
 * afterwards. See that class's javadoc for how sharing one class caused
 * exactly that failure in CI the first time this was written.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class WorkItemCacheOutageIntegrationTest {

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
