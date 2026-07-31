package com.adamastorx.api.outbox;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Plain {@code String} value serializer -- {@link OutboxRelay} sends
 * already-serialized JSON text straight from {@code outbox_events.payload},
 * so it needs no Jackson/type-header machinery of its own (same producer
 * properties as {@code WorkItemProducerConfig}, different value type).
 */
@Configuration
public class OutboxKafkaConfig {

    @Bean
    public ProducerFactory<String, String> outboxProducerFactory(KafkaProperties kafkaProperties) {
        var props = kafkaProperties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new StringSerializer());
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(ProducerFactory<String, String> outboxProducerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(outboxProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }
}
