package com.adamastorx.aggregator.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.sentiment.SentimentScoredEvent;
import com.adamastorx.aggregator.tick.StockPriceTick;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

/**
 * Proves {@link AggregateQueryService}'s real "prefer current window, else
 * fall back to latest known" logic end to end, against a real, running
 * {@link org.apache.kafka.streams.KafkaStreams} instance -- the fallback
 * this class exercises cannot be proven by {@code
 * AggregatorTopologyTest}'s {@code TopologyTestDriver}: {@code
 * AggregateQueryService} reads via {@code StreamsBuilderFactoryBean} /
 * {@code KafkaStreams.store(...)}, which {@code TopologyTestDriver} does
 * not provide, and {@code TopologyTestDriver} has no notion of "wall-clock
 * 'now' has moved on since this record was piped in, but it's still
 * queryable" the way {@code AggregateQueryService.latestKnownState}'s own
 * {@code System.currentTimeMillis()}-anchored window math needs -- so this
 * is a real {@code @SpringBootTest} against a real embedded broker (this
 * project's own established pattern, e.g. {@code
 * StockPriceTickPublisherIntegrationTest}), not a workaround.
 *
 * <p>Uses the real 15-minute default window from {@code application.yml}
 * (not shrunk for this test): "old" records below are produced 45-60
 * minutes in the past, safely outside the current window regardless of
 * exactly when this test runs, and "fresh" records use real {@code
 * Instant.now()} timestamps -- avoids the window-boundary race a
 * deliberately-shrunk window would risk without needing one.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"stock.price.tick", "news.sentiment.scored"})
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DirtiesContext
class AggregateQueryServiceTest {

    // A fresh, unique application-id + state-dir per test run -- avoids
    // colliding with a leftover /tmp/kafka-streams directory (application.yml's
    // own default) from a previous local run or another test class, the
    // same isolation StateStoreRecoveryTest's own per-test temp dirs give
    // its two KafkaStreams instances.
    private static final String APPLICATION_ID = "aggregator-query-service-test-" + UUID.randomUUID();
    private static Path stateDir;

    @DynamicPropertySource
    static void streamsIsolation(DynamicPropertyRegistry registry) throws Exception {
        stateDir = Files.createTempDirectory("aggregator-query-service-test-");
        registry.add("spring.kafka.streams.application-id", () -> APPLICATION_ID);
        registry.add("spring.kafka.streams.state-dir", () -> stateDir.toString());
    }

    @Autowired
    private AggregateQueryService queryService;

    @Autowired
    private AggregatorProperties properties;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    // Lazily instantiated on first use (needs embeddedKafkaBroker, only
    // available once the context is up) -- see producer() helper below.
    private static KafkaProducer<String, byte[]> producer;

    @AfterAll
    static void classTearDown() {
        if (producer != null) {
            producer.close();
        }
    }

    private KafkaProducer<String, byte[]> producer() {
        if (producer == null) {
            Properties producerProps = new Properties();
            producerProps.put("bootstrap.servers", embeddedKafkaBroker.getBrokersAsString());
            producerProps.put("key.serializer", StringSerializer.class.getName());
            producerProps.put("value.serializer", ByteArraySerializer.class.getName());
            producer = new KafkaProducer<>(producerProps);
        }
        return producer;
    }

    private void produceTick(String ticker, String price, Instant timestamp) throws Exception {
        try (JsonSerializer<StockPriceTick> serializer = new JsonSerializer<>(objectMapper)) {
            byte[] value = serializer.serialize(
                    properties.stockPriceTickTopic(),
                    new StockPriceTick(ticker, new BigDecimal(price), timestamp, "WEBSOCKET"));
            producer()
                    .send(new ProducerRecord<>(
                            properties.stockPriceTickTopic(), null, timestamp.toEpochMilli(), ticker, value))
                    .get();
        }
    }

    private void produceSentiment(String ticker, double score, Instant timestamp) throws Exception {
        try (JsonSerializer<SentimentScoredEvent> serializer = new JsonSerializer<>(objectMapper)) {
            byte[] value = serializer.serialize(
                    properties.newsSentimentScoredTopic(), new SentimentScoredEvent(ticker, score));
            producer()
                    .send(new ProducerRecord<>(
                            properties.newsSentimentScoredTopic(), null, timestamp.toEpochMilli(), ticker, value))
                    .get();
        }
    }

    /** Polls {@link AggregateQueryService#latestKnownState} until {@code condition} holds or 30s pass. */
    private TickerAggregateResponse await(String ticker, Predicate<Optional<TickerAggregateResponse>> condition)
            throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(30);
        Optional<TickerAggregateResponse> last = Optional.empty();
        while (Instant.now().isBefore(deadline)) {
            last = queryService.latestKnownState(ticker);
            if (condition.test(last)) {
                return last.orElse(null);
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for " + ticker + "'s latestKnownState to satisfy the expected "
                + "condition; last observed value: " + last);
    }

    private void awaitReady() throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(30);
        while (!queryService.isReady() && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        assertThat(queryService.isReady()).as("KafkaStreams never reached RUNNING within 30s").isTrue();
    }

    @Test
    void oldTickOutsideTheCurrentWindowStillSurfacesViaTheLatestKnownFallbackWithAnHonestPriceAsOf() throws Exception {
        awaitReady();

        Instant old = Instant.now().minus(Duration.ofMinutes(60));
        produceTick("AAPL", "150.00", old);

        TickerAggregateResponse response = await("AAPL", Optional::isPresent);

        // Not silently dropped -- a real response, sourced from the
        // latest-known fallback (tickCount == 1, first == last == the one
        // known price, no fabricated movement).
        assertThat(response.tickCount()).isEqualTo(1);
        assertThat(response.firstPrice()).isEqualByComparingTo("150.00");
        assertThat(response.lastPrice()).isEqualByComparingTo("150.00");
        assertThat(response.priceMovement()).isEqualByComparingTo("0");

        // The real, correct priceAsOf: the old tick's own timestamp, not
        // "now" -- the entire point of this change (no fabricated
        // freshness). Kafka preserves the producer-supplied record
        // timestamp exactly, so this is an exact match, not "close to".
        assertThat(response.priceAsOf()).isEqualTo(old.truncatedTo(ChronoUnit.MILLIS));

        // No sentiment ever produced for AAPL in this test -- still the
        // real, honest "no sentiment" case, independent of price.
        assertThat(response.sentimentSampleCount()).isNull();
        assertThat(response.avgSentiment()).isNull();
        assertThat(response.sentimentAsOf()).isNull();
    }

    @Test
    void aGenuinelyCurrentWindowTickIsStillPreferredAndComputedCorrectlyNoRegression() throws Exception {
        awaitReady();

        produceTick("MSFT", "400.00", Instant.now());
        produceTick("MSFT", "410.00", Instant.now());
        produceTick("MSFT", "405.00", Instant.now());

        TickerAggregateResponse response = await("MSFT", opt -> opt.isPresent() && opt.get().tickCount() == 3);

        // Sourced from the real windowed aggregate (tickCount == 3), not
        // collapsed to the fallback's single-tick synthesis (which would
        // show tickCount == 1) -- the current window is genuinely
        // preferred when it has real data.
        assertThat(response.tickCount()).isEqualTo(3);
        assertThat(response.firstPrice()).isEqualByComparingTo("400.00");
        assertThat(response.lastPrice()).isEqualByComparingTo("405.00");

        // Genuinely fresh -- priceAsOf must be close to real "now", not
        // merely "not null". A generous bound accounts for real CI/test
        // scheduling slack, while still being far tighter than the
        // hour-old case above.
        assertThat(Duration.between(response.priceAsOf(), Instant.now()).abs())
                .isLessThan(Duration.ofSeconds(30));
    }

    @Test
    void priceAndSentimentAreIndependentlyFreshVsStaleKnownForTheSameTickerAtTheSameTime() throws Exception {
        awaitReady();

        Instant staleSentimentAt = Instant.now().minus(Duration.ofMinutes(45));
        produceSentiment("GOOGL", -0.6, staleSentimentAt);
        produceTick("GOOGL", "140.00", Instant.now());

        TickerAggregateResponse response =
                await("GOOGL", opt -> opt.isPresent() && opt.get().avgSentiment() != null);

        // Price: fresh, from the current window.
        assertThat(Duration.between(response.priceAsOf(), Instant.now()).abs())
                .isLessThan(Duration.ofSeconds(30));

        // Sentiment: stale, via the latest-known fallback -- one real
        // known sample, its real (old) score, not fabricated as fresh.
        assertThat(response.sentimentSampleCount()).isEqualTo(1L);
        assertThat(response.avgSentiment()).isCloseTo(-0.6, org.assertj.core.data.Offset.offset(1e-9));
        assertThat(response.sentimentAsOf()).isEqualTo(staleSentimentAt.truncatedTo(ChronoUnit.MILLIS));

        // The real, material gap this whole change exists to surface
        // honestly: this ticker's price and sentiment are not equally
        // fresh, and the response says so.
        assertThat(Duration.between(response.sentimentAsOf(), response.priceAsOf()))
                .isGreaterThan(Duration.ofMinutes(40));
    }

    @Test
    void aTickerNeverSeenAtAllStillReturnsTheRealEmptyCase() throws Exception {
        awaitReady();

        // TSLA is watchlisted (application.yml) but never produced to in
        // this test -- the one real "no data yet" case that must still
        // return empty, not a fabricated response.
        Optional<TickerAggregateResponse> response = queryService.latestKnownState("TSLA");

        assertThat(response).isEmpty();
    }
}
