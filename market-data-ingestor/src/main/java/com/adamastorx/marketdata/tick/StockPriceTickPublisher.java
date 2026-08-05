package com.adamastorx.marketdata.tick;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes a real trade tick onto {@code stock.price.tick}, keyed by
 * ticker (see {@link StockPriceTickProducerConfig}). Records the
 * receipt-to-publish latency backlog #78's AC states a bound on ("under
 * 2s") as a real Micrometer {@link Timer} -- {@code
 * market_data_tick_publish_latency_seconds} on {@code
 * /actuator/prometheus} -- rather than a bound this service only asserts
 * in a comment. Fire-and-forget on the {@link KafkaTemplate#send} future,
 * matching this project's existing "an unreachable broker shouldn't block
 * the caller" convention ({@code application.yml}'s OTLP endpoint comment
 * states the same principle for tracing) -- a send failure is logged, not
 * retried inline; a dropped tick is an acceptable v1 gap for a
 * best-effort market-data feed (unlike {@code work-items}, nothing here
 * has already told a caller "created", so there is no at-least-once
 * promise to keep).
 */
@Service
public class StockPriceTickPublisher {

    private static final Logger log = LoggerFactory.getLogger(StockPriceTickPublisher.class);

    private final KafkaTemplate<String, StockPriceTick> kafkaTemplate;
    private final String topic;
    private final Timer publishLatencyTimer;
    private final Counter publishedCounter;
    private final Counter publishFailedCounter;

    public StockPriceTickPublisher(
            KafkaTemplate<String, StockPriceTick> kafkaTemplate,
            MarketDataProperties properties,
            MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = properties.stockPriceTickTopic();
        this.publishLatencyTimer = Timer.builder("market_data_tick_publish_latency")
                .description("Time from receiving a real trade tick over the Finnhub websocket to the Kafka "
                        + "send() call completing (backlog #78 AC: under 2s)")
                .publishPercentileHistogram()
                .register(meterRegistry);
        this.publishedCounter = Counter.builder("market_data_ticks_published_total")
                .description("Real trade ticks successfully published to stock.price.tick")
                .register(meterRegistry);
        this.publishFailedCounter = Counter.builder("market_data_ticks_publish_failed_total")
                .description("Real trade ticks that failed to publish to stock.price.tick")
                .register(meterRegistry);
    }

    public void publish(StockPriceTick tick) {
        Instant publishStart = Instant.now();
        kafkaTemplate
                .send(topic, tick.ticker(), tick)
                .whenComplete((result, ex) -> {
                    Duration latency = Duration.between(tick.ingestionTimestamp(), Instant.now());
                    publishLatencyTimer.record(latency);
                    if (ex != null) {
                        publishFailedCounter.increment();
                        log.warn("Failed to publish stock.price.tick for {}", tick.ticker(), ex);
                    } else {
                        publishedCounter.increment();
                        log.debug(
                                "Published stock.price.tick for {} (receipt-to-publish latency {}ms)",
                                tick.ticker(),
                                Duration.between(publishStart, Instant.now()).toMillis());
                    }
                });
    }
}
