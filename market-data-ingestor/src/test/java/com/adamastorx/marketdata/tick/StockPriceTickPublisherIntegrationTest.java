package com.adamastorx.marketdata.tick;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Proves backlog #78's AC end to end for the Kafka-producing half of this
 * service, against an embedded broker (no live cluster in this sandbox --
 * see {@code README.md} for how the real Finnhub-to-Kafka path is
 * verified live against the real cluster): a {@link StockPriceTick}
 * handed to {@link StockPriceTickPublisher} really lands on {@code
 * stock.price.tick} with the exact wire shape (ticker, price, volume,
 * exchange timestamp, ingestion timestamp) the AC names, keyed by ticker,
 * and the AC's own "under 2s receipt-to-publish" bound holds -- measured
 * from the real {@code market_data_tick_publish_latency} Timer {@link
 * StockPriceTickPublisher} itself records, not from wall-clock time that
 * also includes this test's own consumer-side poll/group-join overhead
 * (found live: an early version of this test measured across that overhead
 * too and flaked past 2s on nothing but embedded-broker consumer startup
 * cost, not a real publish delay).
 *
 * <p>{@code app.finnhub.auto-connect=false}: see {@link
 * com.adamastorx.marketdata.finnhub.FinnhubWebSocketClient}'s own javadoc
 * on its {@code autoConnect} field -- this is a full {@code
 * @SpringBootTest} context (this repo's existing convention), so without
 * this flag the app would also try to open a real outbound connection to
 * Finnhub using the dummy token below.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "stock.price.tick")
@TestPropertySource(
        properties = {
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
            "app.finnhub.token=test-token-not-real",
            "app.finnhub.auto-connect=false"
        })
@DirtiesContext
class StockPriceTickPublisherIntegrationTest {

    @Autowired
    private StockPriceTickPublisher publisher;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void publishedTickLandsOnTheRealTopicWithinTheLatencyBound() throws Exception {
        Instant ingestionTimestamp = Instant.now();
        StockPriceTick tick = new StockPriceTick(
                "AAPL",
                new BigDecimal("123.45"),
                new BigDecimal("10"),
                ingestionTimestamp.minusSeconds(1),
                ingestionTimestamp);

        publisher.publish(tick);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // KafkaTestUtils.consumerProps defaults key.deserializer to
        // IntegerDeserializer (found live via this test failing, not
        // assumed) -- StockPriceTickProducerConfig keys by ticker
        // (a String), so this must be overridden explicitly.
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, StockPriceTick.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.adamastorx.marketdata.tick");
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        DefaultKafkaConsumerFactory<String, StockPriceTick> consumerFactory =
                new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, StockPriceTick> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(List.of("stock.price.tick"));
            ConsumerRecords<String, StockPriceTick> records =
                    KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10));

            assertThat(records.count()).isEqualTo(1);
            ConsumerRecord<String, StockPriceTick> record = records.iterator().next();

            // Keyed by ticker (StockPriceTickProducerConfig's own javadoc).
            assertThat(record.key()).isEqualTo("AAPL");
            assertThat(record.value()).isEqualTo(tick);
        }

        // AC: "under 2s receipt-to-publish", measured via the exact Timer
        // StockPriceTickPublisher itself records for every real publish.
        // By the time the consumer above has already read the record back
        // from the (embedded) broker, the producer's own send() future has
        // long since completed, so this Timer has exactly one real sample.
        Timer publishLatencyTimer = meterRegistry.find("market_data_tick_publish_latency").timer();
        assertThat(publishLatencyTimer).isNotNull();
        assertThat(publishLatencyTimer.count()).isEqualTo(1);
        assertThat(publishLatencyTimer.max(java.util.concurrent.TimeUnit.MILLISECONDS)).isLessThan(2000);
    }
}
