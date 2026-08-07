package com.adamastorx.aggregator.topology;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.sentiment.SentimentScoredEvent;
import com.adamastorx.aggregator.tick.StockPriceTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
 *
 * <p><b>"Latest known state" tests (found live 2026-08-05, see this
 * module's README).</b> The tests near the bottom of this class prove
 * the topology-level plumbing {@code AggregateQueryService}'s
 * window-then-fallback logic depends on: an old tick still lands in the
 * non-windowed latest-known {@code KTable}s (not silently dropped), with
 * a real, correct timestamp attached via {@link ValueAndTimestamp}, and
 * price/sentiment staleness for one ticker really are tracked
 * independently. {@code AggregateQueryService}'s own fallback logic
 * itself needs a real, running {@code KafkaStreams} instance to exercise
 * end to end (it reads via {@code StreamsBuilderFactoryBean}, which
 * {@code TopologyTestDriver} does not provide) -- that is proven
 * separately by {@code AggregateQueryServiceTest}, a real-embedded-broker
 * test, not faked here by pretending this driver can simulate "wall-clock
 * time has passed since this tick, but it's still queryable."
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
            "latest-price-store",
            "latest-sentiment-store",
            List.of("AAPL", "MSFT"));

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    private TopologyTestDriver driver;
    private TestInputTopic<String, byte[]> tickTopic;
    private TestInputTopic<String, byte[]> sentimentTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        AggregatorTopology.build(builder, properties, new SimpleMeterRegistry());
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
                    properties.stockPriceTickTopic(),
                    new StockPriceTick(ticker, new BigDecimal(price), Instant.now(), "WEBSOCKET"));
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

    // --- "Latest known state" (backlog #81 follow-up, found live 2026-08-05) ---
    //
    // AggregateQueryService's own "prefer current window, else fall back to
    // latest known" logic needs a real, running KafkaStreams instance to
    // exercise end to end (it reads via StreamsBuilderFactoryBean, which
    // TopologyTestDriver does not provide) -- that fallback logic itself is
    // proven by AggregateQueryServiceTest, a real-embedded-broker test, not
    // faked here. What TopologyTestDriver *can* prove directly, and what
    // these tests prove: the topology-level plumbing the fallback depends
    // on actually works -- an old tick still lands in the latest-known
    // KTable (not silently dropped just because it's outside the current
    // window), with the correct real timestamp attached, and price/
    // sentiment staleness for the same ticker really are independent at
    // the store level.

    @Test
    void aTickOutsideTheCurrentWindowStillLandsInTheLatestKnownPriceTableWithItsRealTimestamp() throws Exception {
        // Three windows old relative to "now" (this test's own base
        // instant stands in for "now" the same way every other test here
        // does) -- deliberately outside the windowed store's retention
        // entirely, proving the latest-known KTable is not itself
        // windowed: an old tick still shows up there, undiminished.
        Instant old = Instant.parse("2026-08-03T14:00:00Z");
        Instant now = old.plus(WINDOW.multipliedBy(3));

        tickTopic.pipeInput("AAPL", tick("AAPL", "150.00"), old);

        // "now"'s own window slot for AAPL was never written -- a real
        // AggregateQueryService querying the current (now-anchored)
        // window for AAPL at this point would find nothing here, exactly
        // like the real "market's closed, no fresh tick in 3 windows"
        // scenario this whole change exists for.
        WindowStore<String, PriceWindowAggregate> priceWindowStore =
                driver.getWindowStore(properties.priceWindowStoreName());
        assertThat(priceWindowStore.fetch("AAPL", windowStartMillis(now)))
                .as("an old tick must not appear in a window it was never part of")
                .isNull();

        KeyValueStore<String, ValueAndTimestamp<StockPriceTick>> latestPriceStore =
                driver.getTimestampedKeyValueStore(properties.latestPriceStoreName());
        ValueAndTimestamp<StockPriceTick> latest = latestPriceStore.get("AAPL");

        assertThat(latest).as("the old tick must still surface via the latest-known fallback, not be dropped")
                .isNotNull();
        assertThat(latest.value().price()).isEqualByComparingTo("150.00");
        // The real, correct priceAsOf a caller would compute from this:
        // the old tick's own real timestamp, not "now" -- proving this
        // isn't fabricated as fresh.
        assertThat(latest.timestamp()).isEqualTo(old.toEpochMilli());
    }

    @Test
    void aCurrentWindowTickIsStillPreferredAndCorrectAlongsideTheLatestKnownTable() throws Exception {
        // No regression: a genuinely current-window tick must still be
        // found via the windowed store (AggregateQueryService prefers
        // this over the latest-known fallback), and the latest-known
        // table -- fed by the very same stream -- agrees with it.
        Instant base = Instant.parse("2026-08-03T14:00:00Z");
        tickTopic.pipeInput("AAPL", tick("AAPL", "200.00"), base);
        tickTopic.pipeInput("AAPL", tick("AAPL", "210.00"), base.plusSeconds(30));

        WindowStore<String, PriceWindowAggregate> priceWindowStore =
                driver.getWindowStore(properties.priceWindowStoreName());
        PriceWindowAggregate windowAgg = priceWindowStore.fetch("AAPL", windowStartMillis(base));

        assertThat(windowAgg).isNotNull();
        assertThat(windowAgg.tickCount()).isEqualTo(2);
        assertThat(windowAgg.lastPrice()).isEqualByComparingTo("210.00");

        KeyValueStore<String, ValueAndTimestamp<StockPriceTick>> latestPriceStore =
                driver.getTimestampedKeyValueStore(properties.latestPriceStoreName());
        ValueAndTimestamp<StockPriceTick> latest = latestPriceStore.get("AAPL");

        assertThat(latest).isNotNull();
        assertThat(latest.value().price()).isEqualByComparingTo("210.00");
        assertThat(latest.timestamp()).isEqualTo(base.plusSeconds(30).toEpochMilli());
    }

    @Test
    void priceAndSentimentCanIndependentlyBeFreshVsStaleKnownForTheSameTickerAtTheSameTime() throws Exception {
        // Real trade ticks and real news arrive on unrelated schedules --
        // this proves the two latest-known KTables really do track
        // per-field staleness independently for one ticker, not a single
        // shared "last seen" notion.
        Instant staleSentiment = Instant.parse("2026-08-03T13:00:00Z"); // one window+ old
        Instant freshPrice = staleSentiment.plus(WINDOW).plusSeconds(5); // safely in a later window

        sentimentTopic.pipeInput("AAPL", sentiment("AAPL", -0.4), staleSentiment);
        tickTopic.pipeInput("AAPL", tick("AAPL", "199.50"), freshPrice);

        KeyValueStore<String, ValueAndTimestamp<StockPriceTick>> latestPriceStore =
                driver.getTimestampedKeyValueStore(properties.latestPriceStoreName());
        KeyValueStore<String, ValueAndTimestamp<SentimentScoredEvent>> latestSentimentStore =
                driver.getTimestampedKeyValueStore(properties.latestSentimentStoreName());

        ValueAndTimestamp<StockPriceTick> latestPrice = latestPriceStore.get("AAPL");
        ValueAndTimestamp<SentimentScoredEvent> latestSentiment = latestSentimentStore.get("AAPL");

        assertThat(latestPrice).isNotNull();
        assertThat(latestPrice.timestamp()).isEqualTo(freshPrice.toEpochMilli());

        assertThat(latestSentiment).isNotNull();
        assertThat(latestSentiment.value().score()).isCloseTo(-0.4, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(latestSentiment.timestamp()).isEqualTo(staleSentiment.toEpochMilli());

        // The real, material gap between the two -- exactly what
        // priceAsOf/sentimentAsOf must be able to show independently.
        assertThat(latestPrice.timestamp()).isGreaterThan(latestSentiment.timestamp());

        // And the current window (anchored on freshPrice's own window)
        // has no sentiment at all -- confirming AggregateQueryService
        // would have to fall back to the latest-known table above to
        // surface this stale-but-real sentiment, not find it in-window.
        WindowStore<String, SentimentWindowAggregate> sentimentWindowStore =
                driver.getWindowStore(properties.sentimentWindowStoreName());
        assertThat(sentimentWindowStore.fetch("AAPL", windowStartMillis(freshPrice))).isNull();
    }
}
