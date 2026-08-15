package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
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
 * backlog #105: {@code ClinVarServiceClient} used to have no connect/read
 * timeout at all, so a hung or unreachable {@code clinvar-service} would
 * block a real {@code GET /variants/lookup} request indefinitely. Proves
 * the fix live rather than trusting the config: a fake upstream that
 * deliberately never responds within the real 3s read timeout, and an
 * assertion on real wall-clock elapsed time -- if the timeout weren't
 * wired up, this test would hang for as long as the fake server's own
 * artificial delay (10s), not fail fast.
 *
 * <p>Deliberately a separate test class from {@link VariantLookupIntegrationTest},
 * not a fourth {@code @Test} method there -- a fake upstream that never
 * responds is a fundamentally different double than one that responds with
 * canned data, the same "different fake, different class" split this
 * package's own {@code WorkItemCacheIntegrationTest}/{@code
 * WorkItemCacheOutageIntegrationTest} pair already established.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"work-items", "clinvar.ingestion.completed"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class ClinVarServiceTimeoutIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    // 10s: comfortably longer than the real 3s read timeout under test,
    // short enough this test doesn't itself hang for a long time if the
    // timeout is ever accidentally removed -- the assertion below fails
    // fast either way once this connection actually completes or the
    // client's own timeout fires, whichever comes first.
    private static final HttpServer NEVER_RESPONDS_CLINVAR_SERVICE = startNeverRespondingServer();

    private static HttpServer startNeverRespondingServer() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/internal/clinvar/lookup", exchange -> {
                try {
                    Thread.sleep(Duration.ofSeconds(10));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exchange.sendResponseHeaders(200, -1);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterAll
    static void stopServer() {
        NEVER_RESPONDS_CLINVAR_SERVICE.stop(0);
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add(
                "clinvar-service.base-url",
                () -> "http://localhost:" + NEVER_RESPONDS_CLINVAR_SERVICE.getAddress().getPort());
    }

    @LocalServerPort
    private int port;

    private RestTestClient client;

    private RestTestClient client() {
        if (client == null) {
            client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        }
        return client;
    }

    @Test
    void aHungClinVarServiceFailsFastInsteadOfHangingIndefinitely() {
        Instant before = Instant.now();

        client().get().uri("/variants/lookup?rsid=rs80357906").exchange().expectStatus().is5xxServerError();

        Duration elapsed = Duration.between(before, Instant.now());
        // Real read timeout is 3s; the fake server's own artificial delay is
        // 10s. A real timeout firing lands well under that -- generous
        // headroom (8s) for real CI scheduling jitter, but nowhere near the
        // full 10s the request would take if the timeout weren't wired up
        // at all.
        assertThat(elapsed).isLessThan(Duration.ofSeconds(8));
    }
}
