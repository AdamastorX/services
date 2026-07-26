package com.adamastorx.workers.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Proves ADR 0020 / observability#15's two workers-side ACs against a real
 * running instance, not by assuming the config property or the
 * {@link WorkItemConsumerConfig} wiring works from documentation alone:
 *
 * <ul>
 *   <li>a real message flows through {@code work-items} so the hand-built
 *       listener container factory's {@code Consumer} actually has fetch
 *       activity to report (an idle consumer with no assigned/consumed
 *       partitions may not populate every {@code records-lag} JMX metric);
 *   <li>a request is made so {@code http.server.requests} has at least one
 *       recorded sample;
 *   <li>{@code /actuator/prometheus} is then scraped for real and its body
 *       is asserted to contain both a real {@code _bucket} series for the
 *       HTTP timer and for the Kafka listener timer, plus a real
 *       {@code kafka_consumer_*} series from {@link
 *       io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics}
 *       bound against the actual hand-built {@code Consumer} -- not the
 *       thread-pool-usage proxy ADR 0017 shipped with.
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DirtiesContext
class WorkersMetricsHistogramTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private BlockingQueue<WorkItem> received;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @LocalServerPort
    private int port;

    @Test
    void prometheusScrapeContainsRealHistogramBucketsAndConsumerLag() throws Exception {
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }

        // Produce (and let workers actually consume) a real message so the
        // spring.kafka.listener timer and the bound Consumer's fetch
        // metrics both have real activity behind them, not an idle
        // registration.
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        DefaultKafkaProducerFactory<String, WorkItem> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        try {
            KafkaTemplate<String, WorkItem> template = new KafkaTemplate<>(producerFactory);
            template.send("work-items", new WorkItem("metrics-test-id", "hello from metrics test"))
                    .get(10, TimeUnit.SECONDS);
            assertThat(received.poll(10, TimeUnit.SECONDS)).isNotNull();
        } finally {
            producerFactory.destroy();
        }

        HttpClient httpClient = HttpClient.newHttpClient();
        String baseUrl = "http://localhost:" + port;

        // A prior HTTP request so http.server.requests has a real sample
        // recorded before /actuator/prometheus itself is scraped (the
        // scrape request's own timer only completes *after* its response
        // is written, so it can never see itself).
        HttpResponse<String> health = httpClient.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/actuator/health")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);

        HttpResponse<String> scrape = httpClient.send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/actuator/prometheus")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(scrape.statusCode()).isEqualTo(200);
        String body = scrape.body();

        // ADR 0017's named gap #1: real percentile buckets, not just
        // average/max, for the HTTP timer.
        assertThat(body).contains("http_server_requests_seconds_bucket");

        // ADR 0017's named gap #1, Kafka side: real percentile buckets for
        // the listener timer, proving the percentiles-histogram property
        // reaches an Observation-backed Timer even though
        // WorkItemConsumerConfig's container factory is hand-built.
        assertThat(body).contains("spring_kafka_listener_seconds_bucket");

        // ADR 0017's named gap #2: a real Kafka consumer metric (lag or a
        // sibling fetch-manager series), not the thread-pool-usage proxy --
        // proving KafkaClientMetrics is actually bound against the real
        // hand-built Consumer instance via WorkItemConsumerConfig's
        // ConsumerFactory.Listener, not merely constructed and discarded.
        assertThat(body).contains("kafka_consumer_");
        assertThat(body).containsPattern("kafka_consumer_[a-z_]*lag[a-z_]*");
    }

    @TestConfiguration
    static class CapturingHandlerConfig {

        @Bean
        BlockingQueue<WorkItem> received() {
            return new ArrayBlockingQueue<>(10);
        }

        @Bean
        @Primary
        WorkItemHandler capturingWorkItemHandler(BlockingQueue<WorkItem> received) {
            return received::add;
        }
    }
}
