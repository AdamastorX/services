package com.adamastorx.api.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.api.outbox.OutboxEventJpaRepository;
import com.adamastorx.api.outbox.OutboxRelay;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Backlog #16's own literal AC: "a reproducible failure test that forces a
 * publish failure after a committed save and proves no work item is
 * silently lost."
 *
 * <p>No embedded Kafka broker here at all -- {@code
 * spring.kafka.bootstrap-servers} points at an address nothing is
 * listening on ({@code localhost:1} -- a real port never listens there),
 * so every {@link OutboxRelay} tick's publish genuinely fails, over and
 * over, for as long as this test runs. The old code this replaced
 * (WorkItemController calling KafkaTemplate.send() directly, ADR 0012's
 * named gap) would have silently dropped the Kafka side right here with
 * no trace of the failure and no retry. The point of this test is that
 * the {@code work_items} row and the {@code outbox_events} row both
 * survive that -- durably, in Postgres, unaffected by Kafka being
 * unreachable -- rather than either being rolled back or silently
 * discarded, which is exactly what {@code WorkItemOutboxService}'s single
 * transaction and {@link OutboxRelay}'s retry-forever design guarantee.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(
        properties = {
            "spring.kafka.bootstrap-servers=localhost:1",
            "app.outbox.relay-interval-ms=200",
        })
@Testcontainers
class WorkItemOutboxFailureIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    private int port;

    @Autowired
    private WorkItemJpaRepository workItemRepository;

    @Autowired
    private OutboxEventJpaRepository outboxRepository;

    @Test
    void workItemAndOutboxRowSurviveAnUnreachableBroker() throws InterruptedException {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();

        WorkItem created = client.post()
                .uri("/work-items")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("message", "must not be lost"))
                .exchange()
                .expectStatus()
                .isEqualTo(202)
                .expectBody(WorkItem.class)
                .returnResult()
                .getResponseBody();

        assertThat(created).isNotNull();
        UUID id = UUID.fromString(created.id());

        // The DB write itself is proven durable immediately -- the whole
        // point of doing it in one transaction with the outbox insert,
        // regardless of Kafka's reachability.
        assertThat(workItemRepository.findById(id)).isPresent();

        // Give the relay several real ticks against the unreachable broker.
        Thread.sleep(1500);

        // Still there, still PENDING (never lost, never silently marked
        // published) -- this is the actual "no work item silently lost"
        // proof the AC asks for. It also never blocked the request thread:
        // the POST above already returned 202 well before this sleep.
        assertThat(workItemRepository.findById(id)).isPresent();
        var outboxRows = outboxRepository.findByStatusOrderByCreatedAtAsc(
                "PENDING", org.springframework.data.domain.PageRequest.of(0, 50));
        assertThat(outboxRows).anySatisfy(row -> assertThat(row.getPayload()).contains("must not be lost"));
    }
}
