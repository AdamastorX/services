package com.adamastorx.aggregator.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.sentiment.SentimentScoredEvent;
import com.adamastorx.aggregator.tick.StockPriceTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Proves backlog #81's AC ("a Kafka Streams topology ... computing real
 * windowed aggregates ... rolling average sentiment per ticker, and price
 * movement over the same window correlated against the sentiment
 * aggregate") against the real topology logic, via {@link
 * TopologyTestDriver} -- Kafka Streams' own standard tool for this exact
 * job: no live/embedded broker, but the real {@link
 * AggregatorTopology#build} code path, the real serdes, the real
 * windowing.
 *
 * <p><b>{@code ReadOnlyWindowStore.fetch(key, time)} requires {@code
 * time} to be the window's own exact start timestamp, not merely a point
 * that falls within it</b> -- a real finding from this implementation
 * session, verified twice independently before trusting it: first here
 * (an early draft queried at an arbitrary in-window instant and every
 * fetch came back {@code null}), then again against a real embedded
 * {@code KafkaStreams} instance (not just {@code TopologyTestDriver}) to
 * rule out a test-driver-only quirk -- both showed the identical
 * behavior. {@code AggregateQueryService} had exactly this bug in an
 * earlier draft (it computed the window start correctly but then called
 * {@code fetch(ticker, now)} instead of {@code fetch(ticker,
 * windowStartMs)}) -- caught and fixed because of this test, not glossed
 * over. Every {@code fetch(...)} call below uses a helper that floors to
 * the real window boundary, matching the fixed production code.
 *
 * <p>The correlation itself (combining {@code price-window-store} and
 * {@code sentiment-window-store} for the same ticker/window) happens at
 * query time in {@code AggregateQueryService}, not inside the topology
 * (see {@code AggregatorTopology}'s own javadoc) -- so this test proves
 * each store's own aggregation is correct, and a separate assertion below
 * proves both stores key the same ticker into the *same* window boundary
 * (the real property the query-time correlation depends on).
 */
class AggregatorTopologyTest {

    private static final Duration WINDOW = Duration.ofMinutes(15);

    private final AggregatorProperties properties = new AggregatorProperties(
            "stock.price.tick",
            "news.sentiment.scored",
            WINDOW,
            Duration.ZERO,
            "price-window-store",
            "sentiment-window-store",
            List.of("AAPL", "MSFT"));

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> tickTopic;
    private TestInputTopic<String, byte[]> sentimentTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        AggregatorTopology.build(builder, properties);
        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregator-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // No explicit timestamp extractor override -- the default
        // (FailOnInvalidTimestamp) uses each record's own given
        // timestamp, which is what every pipeInput(..., Instant) call
        // below relies on to control window assignment. An earlier draft
        // of this test set WallclockTimestampExtractor here to sanity-
        // check something else and every fetch() started returning null
        // -- found live, via this test itself, that doing so makes Kafka
        // Streams silently ignore pipeInput's given timestamps in favor
        // of real wall-clock time, so every record landed in "now"'s
        // window instead of the base Instants below, and fetch()'d
        // against those base Instants found nothing there.
        driver = new TopologyTestDriver(topology, props);
        tickTopic = driver.createInputTopic(
                properties.stockPriceTickTopic(), Serdes.String().serializer(), Serdes.ByteArray().serializer());
        sentimentTopic = driver.createInputTopic(
                properties.newsSentimentScoredTopic(), Serdes.String().serializer(), Serdes.ByteArray().serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    /** Floors {@code instant} to this test's window size -- see this class's own javadoc for why. */
    private static long windowStartMillis(Instant instant) {
        long windowMs = WINDOW.toMillis();
        return Math.floorDiv(instant.toEpochMilli(), windowMs) * windowMs;
    }

    private byte[] tick(String ticker, String price) throws Exception {
        try (JsonSerializer<StockPriceTick> serializer = new JsonSerializer<>(objectMapper)) {
            return serializer.serialize(
                    properties.stockPriceTickTopic(), new StockPriceTick(ticker, new BigDecimal(price)));
        }
    }

    private byte[] sentiment(String ticker, double score) throws Exception {
        try (JsonSerializer<SentimentScoredEvent> serializer = new JsonSerializer<>(objectMapper)) {
            return serializer.serialize(
                    properties.newsSentimentScoredTopic(), new SentimentScoredEvent(ticker, score));
        }
    }

    @Test
    void priceWindowAggregateTracksFirstLastMinMaxAcrossMultipleTicksForTheSameTicker() throws Exception {
        Instant base = Instant.parse("2026-08-03T14:00:00Z");
        tickTopic.pipeInput("AAPL", tick("AAPL", "200.00"), base);
        tickTopic.pipeInput("AAPL", tick("AAPL", "205.00"), base.plusSeconds(60));
        tickTopic.pipeInput("AAPL", tick("AAPL", "198.00"), base.plusSeconds(120));
        tickTopic.pipeInput("AAPL", tick("AAPL", "210.00"), base.plusSeconds(180));

        WindowStore<String, PriceWindowAggregate> store = driver.getWindowStore(properties.priceWindowStoreName());
        PriceWindowAggregate agg = store.fetch("AAPL", windowStartMillis(base));

        assertThat(agg).isNotNull();
        assertThat(agg.tickCount()).isEqualTo(4);
        assertThat(agg.firstPrice()).isEqualByComparingTo("200.00");
        assertThat(agg.lastPrice()).isEqualByComparingTo("210.00");
        assertThat(agg.minPrice()).isEqualByComparingTo("198.00");
        assertThat(agg.maxPrice()).isEqualByComparingTo("210.00");
        assertThat(agg.movement()).isEqualByComparingTo("10.00");
    }

    @Test
    void sentimentWindowAggregateComputesARealRollingAverage() throws Exception {
        Instant base = Instant.parse("2026-08-03T14:00:00Z");
        sentimentTopic.pipeInput("AAPL", sentiment("AAPL", 0.5), base);
        sentimentTopic.pipeInput("AAPL", sentiment("AAPL", -0.2), base.plusSeconds(60));
        sentimentTopic.pipeInput("AAPL", sentiment("AAPL", 0.9), base.plusSeconds(120));

        WindowStore<String, SentimentWindowAggregate> store =
                driver.getWindowStore(properties.sentimentWindowStoreName());
        SentimentWindowAggregate agg = store.fetch("AAPL", windowStartMillis(base));

        assertThat(agg).isNotNull();
        assertThat(agg.sampleCount()).isEqualTo(3);
        // (0.5 - 0.2 + 0.9) / 3 = 0.4
        assertThat(agg.averageScore()).isCloseTo(0.4, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void differentTickersAreAggregatedIndependently() throws Exception {
        Instant base = Instant.parse("2026-08-03T14:00:00Z");
        tickTopic.pipeInput("AAPL", tick("AAPL", "200.00"), base);
        tickTopic.pipeInput("MSFT", tick("MSFT", "400.00"), base.plusSeconds(30));
        tickTopic.pipeInput("AAPL", tick("AAPL", "202.00"), base.plusSeconds(60));

        WindowStore<String, PriceWindowAggregate> store = driver.getWindowStore(properties.priceWindowStoreName());

        PriceWindowAggregate aapl = store.fetch("AAPL", windowStartMillis(base));
        PriceWindowAggregate msft = store.fetch("MSFT", windowStartMillis(base));

        assertThat(aapl.tickCount()).isEqualTo(2);
        assertThat(aapl.lastPrice()).isEqualByComparingTo("202.00");
        assertThat(msft.tickCount()).isEqualTo(1);
        assertThat(msft.firstPrice()).isEqualByComparingTo("400.00");
    }

    @Test
    void aTickAndASentimentEventInTheSameWindowLandInTheSameWindowBoundary() throws Exception {
        // Both stores use the exact same TimeWindows definition
        // (AggregatorTopology.build) -- this is the real property
        // AggregateQueryService's query-time correlation depends on: for
        // the same ticker at the same real time, both stores' window
        // start/end must agree, or a query-time combine would silently
        // compare mismatched windows.
        Instant base = Instant.parse("2026-08-03T14:05:00Z"); // not aligned to a window boundary itself
        tickTopic.pipeInput("AAPL", tick("AAPL", "200.00"), base);
        sentimentTopic.pipeInput("AAPL", sentiment("AAPL", 0.3), base.plusSeconds(10));

        WindowStore<String, PriceWindowAggregate> priceStore =
                driver.getWindowStore(properties.priceWindowStoreName());
        WindowStore<String, SentimentWindowAggregate> sentimentStore =
                driver.getWindowStore(properties.sentimentWindowStoreName());

        long windowStart = windowStartMillis(base);
        PriceWindowAggregate priceAgg = priceStore.fetch("AAPL", windowStart);
        SentimentWindowAggregate sentimentAgg = sentimentStore.fetch("AAPL", windowStart);

        assertThat(priceAgg).isNotNull();
        assertThat(sentimentAgg).isNotNull();
        assertThat(sentimentAgg.averageScore()).isCloseTo(0.3, org.assertj.core.data.Offset.offset(1e-9));
    }

    @Test
    void aTickInANewWindowStartsAFreshAggregate() throws Exception {
        Instant firstWindow = Instant.parse("2026-08-03T14:00:00Z");
        Instant secondWindow = firstWindow.plus(WINDOW).plusSeconds(1);

        tickTopic.pipeInput("AAPL", tick("AAPL", "200.00"), firstWindow);
        tickTopic.pipeInput("AAPL", tick("AAPL", "500.00"), secondWindow);

        WindowStore<String, PriceWindowAggregate> store = driver.getWindowStore(properties.priceWindowStoreName());

        PriceWindowAggregate first = store.fetch("AAPL", windowStartMillis(firstWindow));
        PriceWindowAggregate second = store.fetch("AAPL", windowStartMillis(secondWindow));

        assertThat(first.tickCount()).isEqualTo(1);
        assertThat(first.lastPrice()).isEqualByComparingTo("200.00");
        assertThat(second.tickCount()).isEqualTo(1);
        assertThat(second.lastPrice()).isEqualByComparingTo("500.00");
    }
}
