package com.adamastorx.workers.workitem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
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
 * Proves the AC: a message produced (here, with the same wire format
 * {@code api} actually uses -- JSON value, no key, no type headers) is
 * consumed by {@code WorkItemListener} and acknowledged, against an
 * embedded broker (no live cluster in this sandbox; boring/minimal per
 * repo test conventions -- see workers/README.md for how the multi-replica
 * consumer-group behaviour is proven against a real cluster).
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "work-items")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DirtiesContext
class WorkItemListenerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private BlockingQueue<WorkItem> received;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void producedMessageIsConsumedAndAcknowledged() throws Exception {
        // The @KafkaListener container starts asynchronously on context
        // refresh; without this, sending immediately can race the
        // consumer's group join and land before it has partitions
        // assigned.
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        DefaultKafkaProducerFactory<String, WorkItem> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps);
        try {
            KafkaTemplate<String, WorkItem> template = new KafkaTemplate<>(producerFactory);
            WorkItem sent = new WorkItem("test-id", "hello from test");

            template.send("work-items", sent).get(10, TimeUnit.SECONDS);

            WorkItem consumed = received.poll(10, TimeUnit.SECONDS);
            assertThat(consumed).isEqualTo(sent);
        } finally {
            producerFactory.destroy();
        }
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
