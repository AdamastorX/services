package com.adamastorx.newsingestor.feed;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.newsingestor.publishing.ArticlePublishedEvent;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
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

/**
 * Proves the AC end to end against a real HTTP server (not a mocked
 * RssFeedClient) and a real (embedded) Kafka broker:
 *
 * <ol>
 *   <li>{@link #matchedArticleProducesEventWithinOnePollCycle()} -- a real
 *       matching article produces a real {@code news.article.published}
 *       event.
 *   <li>{@link #rePollOfUnchangedFeedNeverRepublishes()} -- polling the
 *       same feed state twice publishes exactly once (dedup by guid).
 *   <li>{@link #unreachableFeedIsSkippedNotCrashed()} -- a feed pointed at
 *       a real closed port doesn't throw out of {@code pollAllFeeds()};
 *       the other, reachable feed in the same cycle still gets processed.
 * </ol>
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "news.article.published")
// AFTER_EACH_TEST_METHOD, not the class-level default (AFTER_CLASS): each
// test method needs its own fresh ArticleDedupService state -- sharing
// one context/bean across all three methods would make an earlier test's
// poll silently "dedup away" a later test's first poll of the same guid,
// a false pass/fail unrelated to what each test actually asserts.
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class FeedPollerIntegrationTest {

    private static HttpServer server;
    private static int serverPort;
    private static int closedPort;

    private static volatile String currentGoodFeedBody = firstPollBody();

    @BeforeAll
    static void startServers() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/good", exchange -> {
            byte[] body = currentGoodFeedBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        serverPort = server.getAddress().getPort();

        // A real port nothing is listening on -- bind then immediately
        // close, so the OS won't hand this port to anything else for the
        // duration of this test but a connect attempt gets a real
        // "connection refused", the same shape a genuinely offline feed
        // (or the real CNBC/Reuters/Yahoo failures ADR 0029 hit live)
        // produces.
        try (ServerSocket probe = new ServerSocket(0)) {
            closedPort = probe.getLocalPort();
        }
    }

    @AfterAll
    static void stopServer() {
        server.stop(0);
    }

    @DynamicPropertySource
    static void feeds(DynamicPropertyRegistry registry) {
        registry.add("app.feeds[0].name", () -> "good-feed");
        registry.add("app.feeds[0].url", () -> "http://localhost:" + serverPort + "/good");
        registry.add("app.feeds[1].name", () -> "unreachable-feed");
        registry.add("app.feeds[1].url", () -> "http://localhost:" + closedPort + "/bad");
        // Scheduling stays real (@EnableScheduling), but the interval is
        // set far longer than this test's runtime -- pollAllFeeds() is
        // invoked directly below for deterministic control, the scheduled
        // trigger is just not allowed to race it.
        registry.add("app.poll-interval-ms", () -> "600000");
    }

    @Autowired
    private FeedPoller feedPoller;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private static String firstPollBody() {
        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <rss version="2.0"><channel>
                <item>
                <guid isPermaLink="false">WP-LIVE-TEST-0001</guid>
                <title>Apple Falls, But Amazon Push Nasdaq Higher</title>
                <description>A real market movement story matching the watchlist.</description>
                <link>https://example.com/story/live-1</link>
                <pubDate>Sun, 02 Aug 2026 18:50:26 GMT</pubDate>
                </item>
                <item>
                <guid isPermaLink="false">WP-LIVE-TEST-0002</guid>
                <title>Local Weather Roundup</title>
                <description>Nothing about any watchlist ticker here.</description>
                <link>https://example.com/story/live-2</link>
                <pubDate>Sun, 02 Aug 2026 17:00:00 GMT</pubDate>
                </item>
                </channel></rss>
                """;
    }

    @Test
    void matchedArticleProducesEventWithinOnePollCycle() throws Exception {
        var consumer = newConsumer();
        try {
            feedPoller.pollAllFeeds();

            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
            ArticlePublishedEvent event = records.iterator().next().value();
            assertThat(event.tickers()).containsExactlyInAnyOrder("AAPL", "AMZN");
            assertThat(event.headline()).isEqualTo("Apple Falls, But Amazon Push Nasdaq Higher");
            assertThat(event.source()).isEqualTo("good-feed");
            assertThat(event.link()).isEqualTo("https://example.com/story/live-1");
            assertThat(event.guid()).isEqualTo("WP-LIVE-TEST-0001");
        } finally {
            consumer.close();
        }
    }

    @Test
    void rePollOfUnchangedFeedNeverRepublishes() throws Exception {
        var consumer = newConsumer();
        try {
            // Poll once (publishes the matching article, proven above),
            // then poll again against the exact same feed body -- a
            // genuine re-poll of an unchanged feed, not a mocked "already
            // seen" flag.
            feedPoller.pollAllFeeds();
            KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10)); // drain the first poll's event

            feedPoller.pollAllFeeds();

            var secondPollRecords = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(5));
            assertThat(secondPollRecords.count())
                    .as("second poll of the unchanged feed must not republish the same guid")
                    .isZero();
        } finally {
            consumer.close();
        }
    }

    @Test
    void unreachableFeedIsSkippedNotCrashed() {
        // Must not throw -- this is the real behavior under test (a
        // connection-refused fetch against a real closed port), not just a
        // try/catch that looks right on inspection.
        feedPoller.pollAllFeeds();
        feedPoller.pollAllFeeds();

        // The scheduler thread survives (proven by the second call above
        // completing normally, in the same JVM/context) and the unreachable
        // outcome is a real, observable metric, not only a log line.
        double unreachableCount = meterRegistry
                .get("news_ingestor_feed_poll_total")
                .tag("outcome", "unreachable")
                .counter()
                .count();
        assertThat(unreachableCount).isGreaterThanOrEqualTo(2.0);

        // The other, reachable feed in the same two-feed cycle still gets
        // processed and counted as succeeded -- one feed's failure doesn't
        // take the whole poll cycle down with it.
        double succeededCount = meterRegistry
                .get("news_ingestor_feed_poll_total")
                .tag("outcome", "succeeded")
                .counter()
                .count();
        assertThat(succeededCount).isGreaterThanOrEqualTo(2.0);
    }

    private org.apache.kafka.clients.consumer.Consumer<String, ArticlePublishedEvent> newConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group-" + System.nanoTime(), "true", embeddedKafkaBroker);
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        JsonDeserializer<ArticlePublishedEvent> valueDeserializer = new JsonDeserializer<>(ArticlePublishedEvent.class, false);
        valueDeserializer.addTrustedPackages("com.adamastorx.newsingestor.publishing");
        var factory = new DefaultKafkaConsumerFactory<>(
                consumerProps, new org.apache.kafka.common.serialization.StringDeserializer(), valueDeserializer);
        var consumer = factory.createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "news.article.published");
        return consumer;
    }
}
