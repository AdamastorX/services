package com.adamastorx.newsingestor.publishing;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Typed {@code KafkaTemplate<String, ArticlePublishedEvent>} built
 * explicitly, same reasoning as {@code workers}' hand-built consumer
 * factory and {@code api}'s outbox producer: Boot's auto-configured
 * {@code kafkaTemplate} bean is untyped ({@code Object,Object}), and a
 * hand-built bean is what actually lets {@link KafkaTemplate#setObservationEnabled}
 * attach a trace span to each publish (ADR 0013) -- relying on Boot's
 * default here would silently produce untraced sends, the same gotcha
 * {@code docs/SESSION_STATE.md} already records for the Kafka producer/
 * listener observation property.
 *
 * <p>{@code JsonSerializer.ADD_TYPE_INFO_HEADERS=false}, matching {@code
 * workers}' producer convention (its {@code application.yml}'s {@code
 * spring.json.add.type.headers: false}) -- the wire contract is the JSON
 * shape, not a Java-class type header a non-JVM consumer (e.g. #80,
 * Python) would have no use for and shouldn't need to trust.
 */
@Configuration
public class NewsPublisherKafkaConfig {

    @Bean
    public ProducerFactory<String, ArticlePublishedEvent> articlePublishedProducerFactory(
            KafkaProperties kafkaProperties) {
        // buildProducerProperties() already carries application.yml's
        // spring.kafka.producer.properties["spring.json.add.type.headers"]
        // = false (workers' own established convention) -- Kafka's
        // KafkaProducer constructor calls configure() on the serializer
        // instances below with this exact map, so JsonSerializer picks the
        // property up without needing a second, code-level call to set it.
        var props = kafkaProperties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>());
    }

    @Bean
    public KafkaTemplate<String, ArticlePublishedEvent> articlePublishedKafkaTemplate(
            ProducerFactory<String, ArticlePublishedEvent> articlePublishedProducerFactory) {
        KafkaTemplate<String, ArticlePublishedEvent> template =
                new KafkaTemplate<>(articlePublishedProducerFactory);
        template.setObservationEnabled(true);
        return template;
    }
}
