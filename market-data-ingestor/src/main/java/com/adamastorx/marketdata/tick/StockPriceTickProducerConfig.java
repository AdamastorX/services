package com.adamastorx.marketdata.tick;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * A typed {@code KafkaTemplate<String, StockPriceTick>}, hand-built rather
 * than left to Boot's untyped {@code kafkaTemplate} auto-configuration --
 * same reasoning {@code api}'s now-removed {@code WorkItemProducerConfig}
 * and {@code workers}' {@code WorkItemConsumerConfig} both document: a
 * typed bean is what lets {@link StockPriceTickPublisher} call {@code
 * send()} without an unchecked cast, and matches this project's existing
 * convention of one hand-built {@code Configuration} per Kafka wire
 * contract.
 *
 * <p>Key = ticker symbol, not {@code null} (unlike {@code work-items},
 * which has no domain entity to key on yet) -- keeping every tick for a
 * given ticker on the same partition preserves per-ticker ordering if this
 * service is ever scaled to more than one producer instance, a real
 * property {@code aggregator} (#81)'s windowed correlation will care
 * about.
 *
 * <p>{@code spring.json.add.type.headers: false} (matches {@code
 * application.yml}): the JSON payload carries no Java-class type header,
 * same "agree on the JSON shape, not a shared Java type" contract
 * {@code workers/README.md} documents for {@code work-items} -- there is
 * no consumer of this topic in this repo yet (#81 will be the first), so
 * this is choosing the same convention up front rather than a new one
 * later.
 *
 * <p>Binds {@link KafkaClientMetrics} directly against the real {@link
 * Producer} instance(s) this hand-built factory creates (ADR 0020) -- the
 * same fix shape {@code WorkItemConsumerConfig} already established for
 * the consumer side, applied to a producer for the first time in this
 * repo: Boot's auto-configured Kafka metrics binder only registers
 * against Boot's own auto-configured {@code ProducerFactory} bean, which
 * this one is not. Without this, a real Prometheus scrape would carry no
 * {@code kafka_producer_*} series (send-rate, in-flight requests, error
 * rate) for this service at all.
 */
@Configuration
public class StockPriceTickProducerConfig {

    @Bean
    public ProducerFactory<String, StockPriceTick> stockPriceTickProducerFactory(
            KafkaProperties kafkaProperties, MeterRegistry meterRegistry) {
        // A plain, unconfigured JsonSerializer instance -- spring.json.add.type.headers:
        // false (application.yml, inside kafkaProperties.buildProducerProperties()'s
        // spring.kafka.producer.properties.*) is applied when the underlying
        // KafkaProducer constructor calls configure() on it. Calling a
        // setter (e.g. setAddTypeInfo(false)) here too, on top of that,
        // throws IllegalStateException ("must be configured with property
        // setters, or via configuration properties; not both") -- found via
        // this module's own test suite, not assumed from the javadoc.
        JsonSerializer<StockPriceTick> valueSerializer = new JsonSerializer<>();
        DefaultKafkaProducerFactory<String, StockPriceTick> factory = new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(), new StringSerializer(), valueSerializer);

        Map<String, KafkaClientMetrics> boundMetricsByProducerId = new ConcurrentHashMap<>();
        factory.addListener(new ProducerFactory.Listener<>() {
            @Override
            public void producerAdded(String id, Producer<String, StockPriceTick> producer) {
                KafkaClientMetrics metrics = new KafkaClientMetrics(producer);
                metrics.bindTo(meterRegistry);
                boundMetricsByProducerId.put(id, metrics);
            }

            @Override
            public void producerRemoved(String id, Producer<String, StockPriceTick> producer) {
                KafkaClientMetrics metrics = boundMetricsByProducerId.remove(id);
                if (metrics != null) {
                    metrics.close();
                }
            }
        });
        return factory;
    }

    @Bean
    public KafkaTemplate<String, StockPriceTick> stockPriceTickKafkaTemplate(
            ProducerFactory<String, StockPriceTick> stockPriceTickProducerFactory) {
        KafkaTemplate<String, StockPriceTick> template = new KafkaTemplate<>(stockPriceTickProducerFactory);
        // Observation-based tracing (observability#1, ADR 0013), same as
        // OutboxKafkaConfig's outboxKafkaTemplate -- without this, a
        // publish carries no OTel span, so a trace can't be followed from
        // "tick received" through to "record acknowledged by the broker".
        template.setObservationEnabled(true);
        return template;
    }
}
