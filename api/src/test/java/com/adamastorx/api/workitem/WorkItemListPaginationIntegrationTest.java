package com.adamastorx.api.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * backlog #130: {@code GET /work-items} used to be {@code
 * repository.findAll()} with no limit at all — confirmed live to return
 * ~14MB/108,000+ rows and, on real ordinary read traffic (not a
 * synthetic worst case), to OOM a correctly-sized pod (2026-08-14, 282
 * real restarts over 41h). Seeds rows directly via {@link
 * WorkItemJpaRepository} (package-private, same package as this test) —
 * real Postgres rows via the same JPA path the app itself uses, not
 * mocked, but skipping the Kafka outbox path {@code POST /work-items}
 * would also exercise, which this AC has nothing to do with.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
@DirtiesContext
class WorkItemListPaginationIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    private int port;

    @Autowired
    private WorkItemJpaRepository repository;

    // Both @Test methods share this class's one static @Container Postgres
    // (Testcontainers instance, not reset between methods by
    // @DirtiesContext -- that only resets the Spring context) -- found
    // live in CI without this: the second test's seeded rows landed on
    // top of the first test's, failing its own totalElements assertion
    // against the combined count instead of its own.
    @BeforeEach
    void cleanSeededRows() {
        repository.deleteAll();
    }

    @Test
    void defaultPageSizeCapsAResponseThatWouldOtherwiseBeMultiplePages() {
        seed(55);

        WorkItemPage page = client()
                .get()
                .uri("/work-items")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(WorkItemPage.class)
                .returnResult()
                .getResponseBody();

        assertThat(page).isNotNull();
        assertThat(page.items()).hasSize(50);
        assertThat(page.page()).isEqualTo(0);
        assertThat(page.size()).isEqualTo(50);
        assertThat(page.totalElements()).isEqualTo(55);
        assertThat(page.totalPages()).isEqualTo(2);
    }

    @Test
    void aHugeRequestedSizeIsClampedToTheRealHardCeilingNotHonoredAsIs() {
        // backlog #130's own AC: a caller can't opt back into the old
        // unbounded behavior by passing a huge ?size= -- 205 real rows,
        // well past both the 50 default and the 200 ceiling, proves the
        // ceiling actually bites rather than just being a higher default.
        seed(205);

        WorkItemPage page = client()
                .get()
                .uri("/work-items?size=10000")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(WorkItemPage.class)
                .returnResult()
                .getResponseBody();

        assertThat(page).isNotNull();
        assertThat(page.items()).hasSize(200);
        assertThat(page.size()).isEqualTo(200);
        assertThat(page.totalElements()).isEqualTo(205);
    }

    private void seed(int count) {
        Instant base = Instant.now();
        List<WorkItemEntity> rows = IntStream.range(0, count)
                .mapToObj(i -> new WorkItemEntity(UUID.randomUUID(), "seeded-" + i, base.minusSeconds(i)))
                .toList();
        repository.saveAll(rows);
    }

    private RestTestClient client() {
        return RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }
}
