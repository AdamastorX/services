package com.adamastorx.aggregator.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.aggregator.AggregatorApplication;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Backlog #85(b): the full-stack proof the two unit-level tests above
 * cannot give -- that {@code management.endpoint.health.group.liveness.
 * include} in {@code application.yml} is actually wired correctly through
 * real Spring Boot autoconfiguration, not merely a property name typed
 * from memory and hoped to work (this backlog item's own AC language).
 * Boots the real {@link AggregatorApplication} context (real {@code
 * @EnableKafkaStreams}, real {@code KafkaStreamsLivenessHealthIndicator}
 * bean, real actuator autoconfiguration) against a real embedded KRaft
 * broker, then makes a real HTTP call to {@code /actuator/health/liveness}
 * (the exact path {@code platform/kubernetes/aggregator/deployment.yaml}'s
 * own liveness probe hits) and inspects the real response body.
 *
 * <p>Confirms, for real:
 * <ul>
 *   <li>{@code kafkaStreams} (this backlog item's new indicator) is a
 *   member of the {@code liveness} group -- proves the {@code
 *   management.endpoint.health.group.liveness.include} property actually
 *   took effect, not just compiled.</li>
 *   <li>{@code livenessState} (Boot's own default) is still a member too
 *   -- proves setting {@code include} explicitly did not silently drop it,
 *   the real replace-not-merge behavior {@code
 *   AvailabilityProbesHealthEndpointGroups} has (confirmed against the
 *   actual {@code spring-boot-health:4.1.0} source, see this module's
 *   {@code KafkaStreamsLivenessHealthIndicator}'s own javadoc).</li>
 *   <li>{@code readinessState} is absent -- proves this change stayed
 *   scoped to the {@code liveness} group only, and did not accidentally
 *   also gate {@code readiness} on Kafka Streams' state (which would
 *   reintroduce exactly the restart-flapping readiness was deliberately
 *   spared from -- see this module's README, "ADR 0011, resolved").</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = AggregatorApplication.class)
class AggregatorLivenessHealthGroupIntegrationTest {

    private static EmbeddedKafkaBroker broker;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) throws Exception {
        broker = new EmbeddedKafkaKraftBroker(1, 1, "stock.price.tick", "news.sentiment.scored");
        broker.afterPropertiesSet();
        Path stateDir = Files.createTempDirectory("aggregator-liveness-group-test-");
        registry.add("spring.kafka.bootstrap-servers", broker::getBrokersAsString);
        registry.add("spring.kafka.streams.application-id", () -> "aggregator-liveness-group-test-" + UUID.randomUUID());
        registry.add("spring.kafka.streams.state-dir", stateDir::toString);
        // Test-only overrides: production application.yml leaves show-
        // details/show-components at their default ("never"), which
        // returns only the aggregate status with no member breakdown --
        // this test needs the real per-component breakdown to confirm
        // *which* indicators are actually in the liveness group, not just
        // that the aggregate status happens to match.
        registry.add("management.endpoint.health.show-details", () -> "always");
        registry.add("management.endpoint.health.show-components", () -> "always");
    }

    @AfterAll
    static void tearDown() {
        if (broker != null) {
            broker.destroy();
        }
    }

    @Test
    void livenessGroupRealResponseIncludesKafkaStreamsAndLivenessStateButNotReadinessState() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health/liveness"))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // A real HTTP response from the real liveness probe path -- 200 or
        // 503 both mean the endpoint answered for real (503 would just
        // mean the real KafkaStreams instance hadn't reached a non-ERROR
        // state yet at the exact moment this request landed, itself real
        // signal, not a test failure by itself).
        assertThat(response.statusCode()).isIn(200, 503);

        JsonNode body = new ObjectMapper().readTree(response.body());
        JsonNode components = body.path("components");
        assertThat(components.has("kafkaStreams"))
                .as("liveness group's real response body: %s", body)
                .isTrue();
        assertThat(components.has("livenessState"))
                .as("liveness group's real response body: %s", body)
                .isTrue();
        assertThat(components.has("readinessState"))
                .as("liveness group must stay scoped to liveness only, real response body: %s", body)
                .isFalse();
    }
}
