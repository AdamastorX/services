package com.adamastorx.watchlist.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Proves backlog #53's own CRUD AC end to end against a real Postgres. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = "clinvar.ingestion.completed")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@Testcontainers
class SubscriptionControllerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @LocalServerPort
    private int port;

    @Test
    void createsReadsAndDeletesAVariantSubscription() {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();

        Subscription created = client.post()
                .uri("/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("variantKey", "variantAnnotation:7:117559600:C:T"))
                .exchange()
                .expectStatus()
                .isEqualTo(201)
                .expectBody(Subscription.class)
                .returnResult()
                .getResponseBody();

        assertThat(created).isNotNull();
        assertThat(created.variantKey()).isEqualTo("variantAnnotation:7:117559600:C:T");

        Subscription fetched = client.get()
                .uri("/subscriptions/{id}", created.id())
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(Subscription.class)
                .returnResult()
                .getResponseBody();

        assertThat(fetched).isEqualTo(created);

        client.delete().uri("/subscriptions/{id}", created.id()).exchange().expectStatus().isEqualTo(204);

        client.get().uri("/subscriptions/{id}", created.id()).exchange().expectStatus().isNotFound();
    }

    @Test
    void rejectsBothVariantKeyAndGeneSymbol() {
        RestTestClient client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();

        client.post()
                .uri("/subscriptions")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("variantKey", "variantAnnotation:7:117559600:C:T", "geneSymbol", "BRCA1"))
                .exchange()
                .expectStatus()
                .isEqualTo(400);
    }
}
