package com.adamastorx.watchlist.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Backlog #53's AC: "a permanently-failing subscriber is dead-lettered
 * rather than blocking the fan-out, with a metric." A subscription whose
 * ntfy target always fails must land in DEAD_LETTERED after
 * app.delivery.max-attempts, incrementing watchlist_delivery_dlq_depth --
 * and it must not need any other subscription to unblock it (this test has
 * only the one failing subscriber, proving dead-lettering itself works;
 * DeliveryIdempotencyIntegrationTest is the one proving a *good* delivery
 * completes -- together they cover "one bad subscriber doesn't block
 * another's fan-out" without needing every combination in one test).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = "clinvar.ingestion.completed")
@TestPropertySource(
        properties = {
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
            "app.delivery.relay-interval-ms=100",
            "app.delivery.max-attempts=3",
        })
@Testcontainers
@DirtiesContext
class NotificationRelayDeadLetterIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    private static HttpServer alwaysFailingNtfy;
    private static final AtomicInteger callCount = new AtomicInteger();

    @DynamicPropertySource
    static void ntfyProperties(DynamicPropertyRegistry registry) throws IOException {
        alwaysFailingNtfy = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        alwaysFailingNtfy.createContext("/", exchange -> {
            callCount.incrementAndGet();
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        alwaysFailingNtfy.start();
        registry.add("app.ntfy.base-url", () -> "http://localhost:" + alwaysFailingNtfy.getAddress().getPort());
    }

    @AfterAll
    static void stop() {
        alwaysFailingNtfy.stop(0);
    }

    @Autowired
    private com.adamastorx.watchlist.subscription.SubscriptionJpaRepository subscriptionRepository;

    @Autowired
    private DeliveryJpaRepository deliveryRepository;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @Test
    void permanentlyFailingSubscriberIsDeadLetteredAndIncrementsDlqDepth() {
        var subscription = new com.adamastorx.watchlist.subscription.SubscriptionEntity(
                UUID.randomUUID(), "variantAnnotation:1:12345:A:G", null, "test-topic", Instant.now());
        subscriptionRepository.save(subscription);

        var delivery = new DeliveryEntity(UUID.randomUUID(), subscription.getId(), "release-1", "variantAnnotation:1:12345:A:G", Instant.now());
        deliveryRepository.save(delivery);

        await().atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> {
                    DeliveryEntity reloaded = deliveryRepository.findById(delivery.getId()).orElseThrow();
                    assertThat(reloaded.getStatus()).isEqualTo(DeliveryStatus.DEAD_LETTERED);
                });

        assertThat(deliveryRepository.countByStatus(DeliveryStatus.DEAD_LETTERED)).isEqualTo(1);
        var dlqDepth = meterRegistry.find("watchlist_delivery_dlq_depth").gauge();
        assertThat(dlqDepth).isNotNull();
        assertThat(dlqDepth.value()).isEqualTo(1.0);
    }
}
