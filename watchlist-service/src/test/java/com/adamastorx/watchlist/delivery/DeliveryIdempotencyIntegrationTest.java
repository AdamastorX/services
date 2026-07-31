package com.adamastorx.watchlist.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.adamastorx.watchlist.ingestion.ClinVarIngestionCompletedEvent;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Backlog #53's hard AC: "delivery is idempotent and proven so by a test
 * that redelivers the same event and asserts exactly one notification."
 *
 * <p>This is a real integration test, not a mock: a real Postgres
 * (Testcontainers), a real embedded Kafka broker (same convention api's own
 * {@code VariantInvalidationIntegrationTest} already uses), and a real JDK
 * {@link HttpServer} standing in for ntfy.sh so the assertion is "exactly
 * one real HTTP POST was received", not an interaction-mock's call count.
 *
 * <p>The same {@code clinvar.ingestion.completed} message (identical
 * newReleaseId + changedKeys) is published to the real broker <em>twice</em>
 * -- simulating a genuine Kafka redelivery (a rebalance, a manual offset
 * reset, or exactly the crash-before-ack window ADR 0026 exists to close)
 * -- and NotificationRelay must still only ever call ntfy once for the one
 * subscription watching that variant.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = "clinvar.ingestion.completed")
@TestPropertySource(
        properties = {
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
            "app.delivery.relay-interval-ms=200",
        })
@Testcontainers
@DirtiesContext
class DeliveryIdempotencyIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    private static HttpServer fakeNtfy;
    private static final List<String> receivedMessages = new CopyOnWriteArrayList<>();
    private static final String VARIANT_KEY = "variantAnnotation:7:117559600:C:T";

    @DynamicPropertySource
    static void ntfyProperties(DynamicPropertyRegistry registry) throws IOException {
        fakeNtfy = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        fakeNtfy.createContext("/", exchange -> {
            receivedMessages.add(new String(exchange.getRequestBody().readAllBytes()));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        fakeNtfy.start();
        registry.add("app.ntfy.base-url", () -> "http://localhost:" + fakeNtfy.getAddress().getPort());
    }

    @AfterAll
    static void stopFakeNtfy() {
        fakeNtfy.stop(0);
    }

    @Autowired
    private com.adamastorx.watchlist.subscription.SubscriptionJpaRepository subscriptionRepository;

    @Autowired
    private DeliveryJpaRepository deliveryRepository;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void redeliveredEventProducesExactlyOneNotification() throws Exception {
        var subscription = new com.adamastorx.watchlist.subscription.SubscriptionEntity(
                UUID.randomUUID(), VARIANT_KEY, null, "test-topic", Instant.now());
        subscriptionRepository.save(subscription);

        ClinVarIngestionCompletedEvent event = new ClinVarIngestionCompletedEvent(
                UUID.randomUUID().toString(), null, "2026-07-24", 1L, Instant.now().toString(), List.of(VARIANT_KEY));

        publish(event);
        publish(event); // real redelivery of the identical message

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(receivedMessages).hasSize(1));

        // Give the relay a couple more ticks to prove nothing else shows up late.
        Thread.sleep(1000);
        assertThat(receivedMessages).hasSize(1);

        List<DeliveryEntity> deliveries = deliveryRepository.findAll();
        assertThat(deliveries).hasSize(1);
        assertThat(deliveries.get(0).getStatus()).isEqualTo(DeliveryStatus.SENT);
    }

    private void publish(ClinVarIngestionCompletedEvent event) throws Exception {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        DefaultKafkaProducerFactory<String, ClinVarIngestionCompletedEvent> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        try {
            KafkaTemplate<String, ClinVarIngestionCompletedEvent> template = new KafkaTemplate<>(producerFactory);
            template.send("clinvar.ingestion.completed", event).get(10, TimeUnit.SECONDS);
        } finally {
            producerFactory.destroy();
        }
    }
}
