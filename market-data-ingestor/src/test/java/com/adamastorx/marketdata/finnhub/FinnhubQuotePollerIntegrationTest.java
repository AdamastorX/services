package com.adamastorx.marketdata.finnhub;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.marketdata.tick.StockPriceTick;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

/**
 * Proves the REST-poll fallback ({@link FinnhubQuotePoller}'s own javadoc
 * has the real problem this solves) end to end against a real local HTTP
 * server -- the same {@code com.sun.net.httpserver.HttpServer} pattern
 * {@code news-ingestor}'s own {@code FeedPollerIntegrationTest} uses, not a
 * mocked {@link FinnhubQuoteClient} -- and a real (embedded) Kafka broker:
 *
 * <ol>
 *   <li>{@link #realQuoteResponseProducesAPublishedTickForEveryReachableTicker()}
 *       -- a real quote response for each reachable watchlisted ticker
 *       produces a real published {@link StockPriceTick} on {@code
 *       stock.price.tick}, with the exact wire shape (ticker, price,
 *       volume, exchange timestamp, ingestion timestamp) the websocket
 *       path already uses.
 *   <li>{@link #perTickerFailureIsSkippedNotCrashed()} -- a real HTTP 500
 *       for exactly one ticker (simulating a Finnhub rate-limit/error
 *       response) doesn't throw out of {@code pollAllTickers()}; the other
 *       four watchlisted tickers in the same cycle are still processed --
 *       this service's own version of {@code
 *       FeedPollerIntegrationTest#unreachableFeedIsSkippedNotCrashed()}.
 * </ol>
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "stock.price.tick")
@TestPropertySource(
        properties = {
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
            "app.finnhub.token=test-token-not-real",
            "app.finnhub.auto-connect=false"
        })
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FinnhubQuotePollerIntegrationTest {

    private static HttpServer server;
    private static int serverPort;
    private static final AtomicInteger tslaRequestCount = new AtomicInteger();

    @BeforeAll
    static void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/quote", exchange -> {
            String symbol = symbolFrom(exchange.getRequestURI().getQuery());
            if ("TSLA".equals(symbol)) {
                // Simulates a real Finnhub error/rate-limit response for
                // exactly one watchlisted ticker -- proves the other four
                // in the same poll cycle are unaffected.
                tslaRequestCount.incrementAndGet();
                exchange.sendResponseHeaders(500, -1);
                exchange.close();
                return;
            }
            String body = "{\"c\":" + priceFor(symbol)
                    + ",\"d\":0,\"dp\":0,\"h\":0,\"l\":0,\"o\":0,\"pc\":0,\"t\":1690000000}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            // A real Content-Type header, same as Finnhub's own real
            // response -- without this, RestClient has no message
            // converter to pick and throws UnknownContentTypeException
            // (found live via this test failing, not assumed).
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        serverPort = server.getAddress().getPort();
    }

    @AfterAll
    static void stopServer() {
        server.stop(0);
    }

    private static String symbolFrom(String query) {
        if (query == null) {
            return "";
        }
        for (String param : query.split("&")) {
            if (param.startsWith("symbol=")) {
                return param.substring("symbol=".length());
            }
        }
        return "";
    }

    private static String priceFor(String symbol) {
        return switch (symbol) {
            case "AAPL" -> "213.14";
            case "MSFT" -> "410.50";
            case "GOOGL" -> "178.20";
            case "AMZN" -> "185.90";
            default -> "1.00";
        };
    }

    @DynamicPropertySource
    static void quotePollProperties(DynamicPropertyRegistry registry) {
        registry.add("app.finnhub-quote-poll.quote-uri", () -> "http://localhost:" + serverPort + "/quote");
        // Scheduling stays real (@EnableScheduling), but the interval is
        // set far longer than this test's runtime -- pollAllTickers() is
        // invoked directly below for deterministic control, matching
        // FeedPollerIntegrationTest's own comment on the same shape.
        registry.add("app.finnhub-quote-poll.interval-ms", () -> "600000");
        registry.add("app.finnhub-quote-poll.initial-delay-ms", () -> "600000");
    }

    @Autowired
    private FinnhubQuotePoller quotePoller;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    void realQuoteResponseProducesAPublishedTickForEveryReachableTicker() throws Exception {
        var consumer = newConsumer();
        try {
            quotePoller.pollAllTickers();

            ConsumerRecords<String, StockPriceTick> records =
                    KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

            // 5 watchlisted tickers (app.market-data.tickers), TSLA's real
            // quote call fails (500) -- 4 real ticks published, not a
            // crashed cycle producing 0.
            assertThat(records.count()).isEqualTo(4);
            Map<String, StockPriceTick> byTicker = new HashMap<>();
            records.forEach(record -> byTicker.put(record.key(), record.value()));
            assertThat(byTicker.keySet()).containsExactlyInAnyOrder("AAPL", "MSFT", "GOOGL", "AMZN");

            StockPriceTick aaplTick = byTicker.get("AAPL");
            assertThat(aaplTick.price()).isEqualByComparingTo(new BigDecimal("213.14"));
            // No real per-quote volume from this REST endpoint -- see
            // FinnhubQuotePoller's own javadoc on why BigDecimal.ZERO, not
            // null, was chosen.
            assertThat(aaplTick.volume()).isEqualByComparingTo(BigDecimal.ZERO);
            assertThat(aaplTick.exchangeTimestamp()).isEqualTo(Instant.ofEpochSecond(1690000000L));
        } finally {
            consumer.close();
        }
    }

    @Test
    void perTickerFailureIsSkippedNotCrashed() {
        int tslaRequestsBefore = tslaRequestCount.get();

        // Must not throw -- this is the real behavior under test (a real
        // HTTP 500 from Finnhub for exactly one ticker), not just a
        // try/catch that looks right on inspection.
        quotePoller.pollAllTickers();
        quotePoller.pollAllTickers();

        assertThat(tslaRequestCount.get() - tslaRequestsBefore).isGreaterThanOrEqualTo(2);

        // The unreachable outcome is a real, observable metric, not only a
        // log line.
        double failedCount = meterRegistry
                .get("market_data_quote_poll_failed_total")
                .counter()
                .count();
        assertThat(failedCount).isGreaterThanOrEqualTo(2.0);

        // The other four reachable tickers in the same two-cycle run still
        // get processed and counted as succeeded -- one ticker's failure
        // doesn't take the whole poll cycle down with it.
        double succeededCount = meterRegistry
                .get("market_data_quote_poll_succeeded_total")
                .counter()
                .count();
        assertThat(succeededCount).isGreaterThanOrEqualTo(8.0);
    }

    private Consumer<String, StockPriceTick> newConsumer() {
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("test-group-" + System.nanoTime(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        JsonDeserializer<StockPriceTick> valueDeserializer = new JsonDeserializer<>(StockPriceTick.class, false);
        valueDeserializer.addTrustedPackages("com.adamastorx.marketdata.tick");
        var factory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), valueDeserializer);
        var consumer = factory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "stock.price.tick");
        return consumer;
    }
}
