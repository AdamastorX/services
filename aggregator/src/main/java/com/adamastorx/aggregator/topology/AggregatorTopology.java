package com.adamastorx.aggregator.topology;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.sentiment.SentimentScoredEvent;
import com.adamastorx.aggregator.tick.StockPriceTick;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Backlog #81's real topology: two independent tumbling-window
 * aggregations, one per input topic, each keyed by ticker.
 *
 * <p><b>Windowing on Kafka's own record timestamp, not a JSON payload
 * field.</b> Both {@code stock.price.tick} and {@code
 * news.sentiment.scored} carry their own business timestamps
 * ({@code exchangeTimestamp}/{@code ingestionTimestamp},
 * {@code articlePublishedAt}/{@code scoredAt}), but this topology
 * deliberately does not parse any of them for window assignment --
 * Kafka Streams' default {@link
 * org.apache.kafka.streams.processor.FailOnInvalidTimestamp} extractor
 * uses the record's own Kafka-level timestamp (producer {@code
 * CreateTime}, i.e. roughly "when this service's Kafka client sent it"),
 * which is both simpler ("boring, well-understood tools", ADR 0021 /
 * ADR 0029 decision 4) and sidesteps {@code sentiment-analyzer}'s own
 * documented uncertainty about one Jackson {@code Instant} wire encoding
 * (see {@code SentimentScoredEvent}'s javadoc here) entirely, rather than
 * building fragile parsing against a field this service doesn't actually
 * need.
 *
 * <p><b>No cross-stream DSL join.</b> An earlier design considered a
 * {@code KTable<Windowed<String>, X>.join(...)} between the two windowed
 * aggregations directly in the topology. Rejected: {@code
 * stock.price.tick} has 3 partitions, {@code news.sentiment.scored} has
 * 1 (see {@code platform/argocd/apps/kafka.yaml}) -- a real KTable-KTable
 * join requires co-partitioned inputs, which would mean introducing a
 * repartition step (and its own topic/changelog) purely to satisfy the
 * join, for a milestone whose real traffic is a handful of events per
 * ticker per window on both sides. Simpler and just as correct: this
 * class materializes two independent, queryable windowed stores; {@code
 * api.AggregateQueryService} performs the "correlate sentiment against
 * price movement" step at query time by reading both stores for the same
 * ticker and window. This also means an all-price/no-sentiment window
 * (the common case -- news is sparse, price ticks are continuous) still
 * produces a real, queryable result instead of an inner join silently
 * withholding it. See this module's README for the full reasoning.
 *
 * <p><b>Window size bounds changelog-rebuild cost -- the real ADR 0011
 * resolution.</b> {@link TimeWindows#ofSizeAndGrace} sets each store's
 * changelog retention to window size + grace (Kafka Streams' own
 * mechanism, not something this class implements itself) -- see {@code
 * AggregatorProperties#window()}'s javadoc and this module's README for
 * the real measured restoration time this bound was chosen against.
 *
 * <p><b>A second, parallel, non-windowed "latest known state" per
 * ticker.</b> Found live 2026-08-05: real trade ticks only flow during US
 * market hours and real news is naturally sparse, so most of the time
 * (including whenever a human opens {@code visualizer}) the *current*
 * 15-minute window has no data for a ticker at all, even though a real
 * price/sentiment was seen recently. {@link KStream#toTable()} on each
 * already-ticker-keyed source stream materializes a plain {@code
 * KTable<String, StockPriceTick>} / {@code KTable<String,
 * SentimentScoredEvent>} -- ordinary KTable semantics (latest record per
 * key wins), not a windowed aggregate, so {@code
 * api.AggregateQueryService} can fall back to "the most recent known
 * value, whatever its age" instead of returning nothing. No explicit
 * {@code groupByKey()} before {@code toTable()}: both source streams are
 * already correctly partitioned by ticker straight from {@code
 * builder.stream(topic)} (same key the topic itself is produced on), so
 * the DSL does not insert a repartition step -- confirmed by this
 * topology's own {@code Topology#describe()} output having no repartition
 * node for either new sub-topology.
 *
 * <p><b>This does not reopen ADR 0011's resolution above -- it is a
 * separately-bounded, trivially small addition.</b> A KTable's changelog
 * is bounded by key-space, not by time: at most one record per key is
 * ever retained (each new value for a ticker overwrites, does not append
 * to, the changelog's compacted view), and this topology's key-space is
 * exactly the 5-ticker watchlist. A full rebuild of either latest-known
 * store after a broker/topic loss replays at most 5 changelog records --
 * smaller than even the windowed stores' own already-small measured
 * replay volume (see README's "ADR 0011, resolved"). Stated explicitly
 * here, not merely assumed: this reasoning does not depend on real
 * traffic volume or frequency the way the windowed stores' bound does
 * (window size), so it holds even as {@code market-data-ingestor}'s
 * REST-poll fallback and real news frequency both stay low.
 */
public final class AggregatorTopology {

    private AggregatorTopology() {}

    public static KStream<String, StockPriceTick> build(StreamsBuilder builder, AggregatorProperties props) {
        Serde<String> keySerde = Serdes.String();
        Serde<StockPriceTick> tickSerde =
                new JsonSerde<>(StockPriceTick.class).noTypeInfo().ignoreTypeHeaders();
        Serde<SentimentScoredEvent> sentimentSerde =
                new JsonSerde<>(SentimentScoredEvent.class).noTypeInfo().ignoreTypeHeaders();
        Serde<PriceWindowAggregate> priceAggSerde =
                new JsonSerde<>(PriceWindowAggregate.class).noTypeInfo().ignoreTypeHeaders();
        Serde<SentimentWindowAggregate> sentimentAggSerde =
                new JsonSerde<>(SentimentWindowAggregate.class).noTypeInfo().ignoreTypeHeaders();

        // No grace by default (AggregatorProperties#grace() -- see its own
        // javadoc): a real-time trade-tick/sentiment feed has no expected
        // late-arrival case worth extending changelog retention for.
        TimeWindows windows = TimeWindows.ofSizeAndGrace(props.window(), props.grace());

        KStream<String, StockPriceTick> ticks =
                builder.stream(props.stockPriceTickTopic(), Consumed.with(keySerde, tickSerde));
        ticks.groupByKey(Grouped.with(keySerde, tickSerde))
                .windowedBy(windows)
                .aggregate(
                        PriceWindowAggregate::empty,
                        (ticker, tick, agg) -> agg.accumulate(tick.price()),
                        Materialized.<String, PriceWindowAggregate, WindowStore<Bytes, byte[]>>as(
                                        props.priceWindowStoreName())
                                .withKeySerde(keySerde)
                                .withValueSerde(priceAggSerde));
        // The "latest known price" fallback -- see this class's own javadoc
        // ("A second, parallel, non-windowed 'latest known state'").
        // KStream.toTable() keeps ordinary KTable semantics (the latest
        // record per key wins), materialized with Kafka Streams' own
        // default TimestampedKeyValueStore -- api.AggregateQueryService
        // reads that store via QueryableStoreTypes.timestampedKeyValueStore()
        // specifically so it gets each ticker's real last-update time
        // (ValueAndTimestamp#timestamp(), Kafka's own record timestamp,
        // same source this whole topology already trusts -- see "Windowing
        // on Kafka's own record timestamp" above) for the honest
        // priceAsOf field, not a fabricated one.
        ticks.toTable(Materialized.<String, StockPriceTick, KeyValueStore<Bytes, byte[]>>as(props.latestPriceStoreName())
                .withKeySerde(keySerde)
                .withValueSerde(tickSerde));

        KStream<String, SentimentScoredEvent> sentiments =
                builder.stream(props.newsSentimentScoredTopic(), Consumed.with(keySerde, sentimentSerde));
        sentiments
                .groupByKey(Grouped.with(keySerde, sentimentSerde))
                .windowedBy(windows)
                .aggregate(
                        SentimentWindowAggregate::empty,
                        (ticker, evt, agg) -> agg.accumulate(evt.score()),
                        Materialized.<String, SentimentWindowAggregate, WindowStore<Bytes, byte[]>>as(
                                        props.sentimentWindowStoreName())
                                .withKeySerde(keySerde)
                                .withValueSerde(sentimentAggSerde));
        // Same "latest known" fallback, independent of the price one above
        // -- a ticker's price and sentiment can each be fresh or stale on
        // their own (real news arrives on its own schedule, unrelated to
        // when the last trade tick landed).
        sentiments.toTable(Materialized.<String, SentimentScoredEvent, KeyValueStore<Bytes, byte[]>>as(
                        props.latestSentimentStoreName())
                .withKeySerde(keySerde)
                .withValueSerde(sentimentSerde));

        return ticks;
    }
}
