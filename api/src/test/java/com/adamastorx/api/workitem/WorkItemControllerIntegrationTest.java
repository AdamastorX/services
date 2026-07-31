package com.adamastorx.api.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Proves both halves of the AC: POSTing to {@code /work-items} persists a
 * row to a real PostgreSQL (services#4, ADR 0012 -- Testcontainers, not
 * H2, to avoid a works-in-tests/fails-in-prod SQL-dialect gap; Flyway's
 * {@code V1__} migration runs for real against it) *and* publishes a JSON
 * record onto the {@code work-items} topic in the exact wire shape
 * {@code workers} expects (no key, no type header, decodes as
 * {@code WorkItem}) -- against an embedded broker, no live cluster in
 * this sandbox.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(
        properties = {
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
            // backlog #16/ADR 0026: publish is now async via OutboxRelay
            // (default 2s tick) rather than synchronous in the request
            // thread -- a fast tick here keeps this test quick and not
            // flaky against getSingleRecord's 10s wait below.
            "app.outbox.relay-interval-ms=200",
        })
@Testcontainers
@DirtiesContext
class WorkItemControllerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    private int port;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void postingAWorkItemPersistsItAndPublishesItToTheTopic() {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();

        WorkItem created = client.post()
                .uri("/work-items")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("message", "hello from test"))
                .exchange()
                .expectStatus()
                .isEqualTo(202)
                .expectBody(WorkItem.class)
                .returnResult()
                .getResponseBody();

        assertThat(created).isNotNull();
        assertThat(created.message()).isEqualTo("hello from test");

        WorkItem fetched = client.get()
                .uri("/work-items/{id}", created.id())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(WorkItem.class)
                .returnResult()
                .getResponseBody();

        assertThat(fetched).isEqualTo(created);

        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("verify-producer-format", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Fixed target type, headers ignored -- mirrors exactly how
        // workers' consumer is configured (spring.json.use.type.headers:
        // false / spring.json.value.default.type), proving api's producer
        // output actually satisfies that contract.
        try (Consumer<String, WorkItem> consumer = new KafkaConsumer<>(
                consumerProps, new StringDeserializer(), new JsonDeserializer<>(WorkItem.class, false))) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "work-items");

            ConsumerRecord<String, WorkItem> record =
                    KafkaTestUtils.getSingleRecord(consumer, "work-items", Duration.ofSeconds(10));

            assertThat(record.key()).isNull();
            assertThat(record.value()).isEqualTo(created);
        }
    }
}
