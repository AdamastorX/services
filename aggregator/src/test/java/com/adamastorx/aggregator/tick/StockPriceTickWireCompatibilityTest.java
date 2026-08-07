package com.adamastorx.aggregator.tick;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * backlog #91: a real, documented uncertainty already flagged elsewhere in
 * this same module ({@code SentimentScoredEvent}'s own javadoc, on why it
 * deliberately never parses a Java {@code Instant} field coming through a
 * Kafka {@link JsonSerde}) is exactly what adding {@code exchangeTimestamp}
 * to this class risked reintroducing -- and this test caught it for real,
 * not hypothetically: a first CI run of this exact test failed with
 * {@code InvalidDefinitionException: Java 8 date/time type java.time.Instant
 * not supported by default}. Root cause: {@code jackson-datatype-jsr310}
 * was simply missing from this module's own {@code pom.xml} (present in
 * {@code market-data-ingestor}'s and {@code news-ingestor}'s already, whose
 * own wire-shape records carry {@code java.time} fields) -- fixed there,
 * not with a custom {@code ObjectMapper} here, matching that dependency's
 * own comment: spring-kafka's {@link JsonSerde} auto-registers any Jackson
 * module found on the classpath via {@code ServiceLoader}, so the plain,
 * no-arg construction below (identical to {@code AggregatorTopology}'s own)
 * is enough once the dependency is actually present.
 *
 * <p>Separately, this module's own {@code README.md} ("Wire shapes
 * consumed") already recorded the real wire *format* empirically before
 * this item needed it: {@code stock.price.tick}'s {@code Instant} fields
 * serialize as a JSON **number** (epoch seconds with a fractional-nanosecond
 * component, e.g. {@code 1785767400.123456789}), not an ISO-8601 string --
 * a first draft of this test used a hardcoded ISO-8601 literal before that
 * finding was re-read, which would have been a second, independent way for
 * this test to fail for the right underlying reason (a real wire-shape
 * mismatch) had the missing-dependency issue above not been hit first.
 */
class StockPriceTickWireCompatibilityTest {

    @Test
    void deserializesTheRealMarketDataIngestorWireShape() {
        // A real market-data-ingestor StockPriceTick, JSON-encoded --
        // volume/ingestionTimestamp are fields this class deliberately
        // doesn't declare (see this class's own javadoc); @JsonIgnoreProperties
        // must tolerate them, not just the two fields this test cares about.
        String json =
                """
                {
                  "ticker": "AAPL",
                  "price": 123.45,
                  "volume": 10,
                  "exchangeTimestamp": 1785767400.123456789,
                  "ingestionTimestamp": 1785767401.5,
                  "source": "WEBSOCKET"
                }
                """;

        try (JsonSerde<StockPriceTick> serde =
                new JsonSerde<>(StockPriceTick.class).noTypeInfo().ignoreTypeHeaders()) {
            StockPriceTick tick = serde.deserializer().deserialize("stock.price.tick", json.getBytes(StandardCharsets.UTF_8));

            assertThat(tick.ticker()).isEqualTo("AAPL");
            assertThat(tick.price()).isEqualByComparingTo("123.45");
            assertThat(tick.exchangeTimestamp()).isEqualTo(Instant.ofEpochSecond(1785767400L, 123456789L));
            assertThat(tick.source()).isEqualTo("WEBSOCKET");
        }
    }

    @Test
    void deserializesAPollFallbackTick() {
        String json =
                """
                {
                  "ticker": "MSFT",
                  "price": 400.00,
                  "volume": 0,
                  "exchangeTimestamp": 1785767400,
                  "ingestionTimestamp": 1785767400.2,
                  "source": "POLL_FALLBACK"
                }
                """;

        try (JsonSerde<StockPriceTick> serde =
                new JsonSerde<>(StockPriceTick.class).noTypeInfo().ignoreTypeHeaders()) {
            StockPriceTick tick = serde.deserializer().deserialize("stock.price.tick", json.getBytes(StandardCharsets.UTF_8));

            assertThat(tick.source()).isEqualTo("POLL_FALLBACK");
            assertThat(tick.exchangeTimestamp()).isEqualTo(Instant.ofEpochSecond(1785767400L));
        }
    }
}
