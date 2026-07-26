package com.adamastorx.api.clinvar;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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
 * Proves {@code GET /variants/lookup}'s AC end to end (ADR 0019): both
 * lookup key styles (rsID and coordinates), a not-found case, and the
 * mutually-exclusive parameter validation -- against a known real
 * pathogenic variant, rs80357906 (BRCA1), and rs80359550 (BRCA2), same
 * fixture data services#24's original test used.
 *
 * <p>Under ADR 0019, {@code api} no longer touches a tabix file or a
 * local Postgres table for this endpoint at all -- {@link
 * VariantLookupService} calls {@code clinvar-service} over HTTP. This
 * test stands in for that upstream with a tiny local {@link HttpServer}
 * (JDK built-in, no new test dependency -- same pattern {@code
 * workers.clinvar.ClinVarTabixIndexerTest}/{@code
 * ClinVarIngestionServiceIntegrationTest} already used to fake NCBI
 * before ADR 0019 removed those classes) rather than WireMock or a
 * Postgres/filesystem fixture -- {@code api} has no dependency on either
 * anymore, so there's nothing left to seed for this test but the fake
 * upstream's canned responses.
 *
 * <p>Postgres Testcontainers is still required here (unrelated to
 * ClinVar): {@code api}'s full Spring context still migrates/queries
 * {@code work_items} via Flyway/JPA regardless of what this test
 * exercises, exactly like every other full-context {@code api} test
 * (e.g. {@code WorkItemCacheIntegrationTest}).
 *
 * <p>Seeding lives in {@code @BeforeEach}, not {@code @BeforeAll} --
 * this test has no seeding to do at all beyond starting the fake HTTP
 * server (done once, eagerly, in a static field initializer, exactly
 * like {@code refdataRoot} in this class's ADR 0018-era predecessor) --
 * see that predecessor's javadoc (git history) for the full
 * {@code @TestInstance(PER_CLASS)}/{@code @Testcontainers} ordering
 * conflict this deliberately avoids by never introducing it in the first
 * place.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {"work-items", "clinvar.ingestion.completed"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class VariantLookupIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

    private static final UUID RELEASE_ID = UUID.randomUUID();
    private static final ObjectMapper JSON = new ObjectMapper();

    // Created eagerly at class-load time (a static field initializer),
    // exactly like refdataRoot in the ADR 0018-era version of this test
    // -- so it's up and listening before @DynamicPropertySource's
    // properties() runs (Spring's context loading, which happens after
    // Testcontainers start but is otherwise independent of this class's
    // own static initialization order).
    private static final HttpServer FAKE_CLINVAR_SERVICE = startFakeClinVarService();

    private static HttpServer startFakeClinVarService() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/internal/clinvar/lookup", VariantLookupIntegrationTest::handleLookup);
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterAll
    static void stopFakeClinVarService() {
        FAKE_CLINVAR_SERVICE.stop(0);
    }

    private static void handleLookup(HttpExchange exchange) throws IOException {
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        ClinVarLookupResponse response = resolve(query);
        if (response == null) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
        }
        byte[] body = JSON.writeValueAsBytes(response);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        try (var out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static ClinVarLookupResponse resolve(Map<String, String> query) {
        String rsid = query.get("rsid");
        if ("rs80357906".equals(rsid)
                || ("17".equals(query.get("chrom"))
                        && "43057062".equals(query.get("pos"))
                        && "T".equals(query.get("ref"))
                        && "TG".equals(query.get("alt")))) {
            return new ClinVarLookupResponse(
                    "17",
                    43057062,
                    "T",
                    "TG",
                    "rs80357906",
                    "Pathogenic",
                    "reviewed_by_expert_panel",
                    null,
                    RELEASE_ID.toString());
        }
        if ("rs80359550".equals(rsid)
                || ("13".equals(query.get("chrom"))
                        && "32340300".equals(query.get("pos"))
                        && "GT".equals(query.get("ref"))
                        && "G".equals(query.get("alt")))) {
            return new ClinVarLookupResponse(
                    "13",
                    32340300,
                    "GT",
                    "G",
                    "rs80359550",
                    "Pathogenic",
                    "criteria_provided,_single_submitter",
                    null,
                    RELEASE_ID.toString());
        }
        return null;
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> params = new HashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return params;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = eq >= 0 ? pair.substring(0, eq) : pair;
            String value = eq >= 0 ? pair.substring(eq + 1) : "";
            params.put(
                    URLDecoder.decode(key, StandardCharsets.UTF_8), URLDecoder.decode(value, StandardCharsets.UTF_8));
        }
        return params;
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        registry.add(
                "clinvar-service.base-url",
                () -> "http://localhost:" + FAKE_CLINVAR_SERVICE.getAddress().getPort());
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
    void lookupByRsidReturnsPathogenicBrca1Variant() {
        VariantAnnotation annotation = client()
                .get()
                .uri("/variants/lookup?rsid=rs80357906")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(VariantAnnotation.class)
                .returnResult()
                .getResponseBody();

        assertThat(annotation).isNotNull();
        assertThat(annotation.clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(annotation.clinvarReleaseId()).isEqualTo(RELEASE_ID.toString());
        assertThat(annotation.chrom()).isEqualTo("17");
        assertThat(annotation.pos()).isEqualTo(43057062);
    }

    @Test
    void lookupByCoordinatesReturnsSameVariant() {
        VariantAnnotation annotation = client()
                .get()
                .uri("/variants/lookup?chrom=13&pos=32340300&ref=GT&alt=G")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(VariantAnnotation.class)
                .returnResult()
                .getResponseBody();

        assertThat(annotation).isNotNull();
        assertThat(annotation.clinicalSignificance()).isEqualTo("Pathogenic");
        assertThat(annotation.rsid()).isEqualTo("rs80359550");
        assertThat(annotation.clinvarReleaseId()).isEqualTo(RELEASE_ID.toString());
    }

    @Test
    void unknownRsidReturnsNotFound() {
        client().get().uri("/variants/lookup?rsid=rs00000000").exchange().expectStatus().isNotFound();
    }

    @Test
    void bothKeyStylesTogetherIsBadRequest() {
        client()
                .get()
                .uri("/variants/lookup?rsid=rs80357906&chrom=17&pos=43057062&ref=T&alt=TG")
                .exchange()
                .expectStatus()
                .isBadRequest();
    }

    @Test
    void neitherKeyStyleIsBadRequest() {
        client().get().uri("/variants/lookup").exchange().expectStatus().isBadRequest();
    }
}
