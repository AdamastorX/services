package com.adamastorx.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * Proves ADR 0017's named, deliberately-deferred gap is actually closed
 * (observability#15, ADR 0020): {@code management.metrics.distribution.
 * percentiles-histogram.http.server.requests: true} (application.yml)
 * makes a real {@code _bucket} series appear on a live instance's
 * {@code /actuator/prometheus}, not just {@code _sum}/{@code _count}/
 * {@code _max} -- verified against an actually running Spring context, not
 * assumed from the Micrometer docs.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GatewayMetricsHistogramTest {

    @LocalServerPort
    private int port;

    @Test
    void prometheusScrapeContainsRealHttpLatencyHistogramBuckets() throws Exception {
        HttpClient httpClient = HttpClient.newHttpClient();
        String baseUrl = "http://localhost:" + port;

        // A prior HTTP request so http.server.requests has a real sample
        // recorded before /actuator/prometheus itself is scraped -- the
        // scrape request's own timer only completes after its response is
        // written, so it can never see itself.
        HttpResponse<String> health = httpClient.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/actuator/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);

        HttpResponse<String> scrape = httpClient.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/actuator/prometheus")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(scrape.statusCode()).isEqualTo(200);

        assertThat(scrape.body()).contains("http_server_requests_seconds_bucket");
    }
}
