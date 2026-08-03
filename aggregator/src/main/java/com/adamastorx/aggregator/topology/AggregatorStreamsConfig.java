package com.adamastorx.aggregator.topology;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.observability.StateRestoreMetrics;
import com.adamastorx.aggregator.tick.StockPriceTick;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;

/**
 * Wires backlog #81's real topology into Spring's own {@code
 * @EnableKafkaStreams} support (spring-kafka's documented extension
 * points, not a hand-rolled replacement of them -- unlike this project's
 * hand-built {@code KafkaTemplate}/consumer-factory configs elsewhere,
 * {@code @EnableKafkaStreams}'s {@code defaultKafkaStreamsConfig} bean
 * (auto-provided by {@code spring-boot-kafka}'s {@code
 * KafkaStreamsAnnotationDrivenConfiguration}, confirmed by inspecting the
 * actual jar) already reads {@code spring.kafka.streams.*}
 * (application.yml) the same way {@code spring.kafka.producer.*} feeds
 * every other module's hand-built producer factory -- there is no
 * "untyped auto-configuration" gap here to work around the way the
 * plain-Kafka modules' own configs document, so this class only adds what
 * Boot's own autoconfiguration doesn't: the actual topology, and the two
 * observability hooks (state-restore timing, consumer-lag metrics)
 * backlog #81's AC names).
 *
 * <p>{@code aggregatorTopology}'s {@code StreamsBuilder} parameter is the
 * documented spring-kafka pattern for registering a topology against
 * {@code @EnableKafkaStreams}'s auto-configured builder -- the bean's
 * return value is not otherwise used by this app; the real work
 * ({@link AggregatorTopology#build}) is a side effect on the injected
 * {@link StreamsBuilder}, which the {@code
 * StreamsBuilderFactoryBean} (also auto-configured) turns into a real
 * {@link org.apache.kafka.streams.Topology} and starts once the
 * application context finishes refreshing.
 */
@Configuration
@EnableKafkaStreams
public class AggregatorStreamsConfig {

    @Bean
    public KStream<String, StockPriceTick> aggregatorTopology(StreamsBuilder streamsBuilder, AggregatorProperties properties) {
        return AggregatorTopology.build(streamsBuilder, properties);
    }

    /**
     * Backlog #81's AC: "consumer lag and state-restoration duration are
     * metrics". {@link StreamsBuilderFactoryBeanConfigurer} is
     * spring-kafka's own documented hook for customizing the
     * auto-configured {@link StreamsBuilderFactoryBean} before it starts
     * the real {@link KafkaStreams} instance (confirmed present and
     * auto-applied by inspecting {@code
     * KafkaStreamsDefaultConfiguration#defaultKafkaStreamsBuilder}'s own
     * {@code ObjectProvider<StreamsBuilderFactoryBeanConfigurer>}
     * parameter in the actual jar) -- the same "bind real client metrics
     * once the real client instance exists" shape {@code
     * StockPriceTickProducerConfig}'s producer-factory listener already
     * established for a plain {@code Producer}, applied here to a
     * {@code KafkaStreams} instance via {@link KafkaStreamsMetrics}
     * (which surfaces the underlying consumer(s)' own {@code
     * records-lag}/{@code records-lag-max} metrics -- real consumer lag,
     * not a hand-rolled substitute) and {@link
     * StateRestoreMetrics} (this module's own real restore-duration
     * timer).
     */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer aggregatorStreamsConfigurer(MeterRegistry meterRegistry) {
        return factoryBean -> {
            factoryBean.setStateRestoreListener(new StateRestoreMetrics(meterRegistry));
            factoryBean.addListener(new StreamsBuilderFactoryBean.Listener() {
                @Override
                public void streamsAdded(String id, KafkaStreams kafkaStreams) {
                    new KafkaStreamsMetrics(kafkaStreams).bindTo(meterRegistry);
                }
            });
        };
    }
}
