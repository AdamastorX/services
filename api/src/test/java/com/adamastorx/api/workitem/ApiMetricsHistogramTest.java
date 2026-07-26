package com.adamastorx.api.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Proves ADR 0017's named, deliberately-deferred gap is actually closed for
 * {@code api} (observability#15, ADR 0020): {@code management.metrics.
 * distribution.percentiles-histogram.http.server.requests: true}
 * (application.yml) makes a real {@code _bucket} series appear on a live
 * instance's {@code /actuator/prometheus}, not just {@code _sum}/
 * {@code _count}/{@code _max} -- verified against api's actual running
 * context (real PostgreSQL via Flyway, embedded Kafka), the same mechanism
 * already proven for gateway ({@code GatewayMetricsHistogramTest}) and
 * workers ({@code WorkersMetricsHistogramTest}), not assumed to carry over
 * unverified just because it's "the same property".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class ApiMetricsHistogramTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    private int port;

    @Test
    void prometheusScrapeContainsRealHttpLatencyHistogramBuckets() {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();

        // A prior real request so http.server.requests has an actual
        // sample recorded before /actuator/prometheus itself is scraped.
        client.post()
                .uri("/work-items")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("message", "metrics histogram test"))
                .exchange()
                .expectStatus()
                .isEqualTo(202);

        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest scrapeRequest = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/prometheus"))
                .GET()
                .build();
        HttpResponse<String> scrape = httpClient.sendAsync(scrapeRequest, HttpResponse.BodyHandlers.ofString())
                .join();

        assertThat(scrape.statusCode()).isEqualTo(200);
        assertThat(scrape.body()).contains("http_server_requests_seconds_bucket");
    }
}
