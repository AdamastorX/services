package com.adamastorx.aggregator.api;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.sentiment.SentimentScoredEvent;
import com.adamastorx.aggregator.tick.StockPriceTick;
import com.adamastorx.aggregator.topology.PriceWindowAggregate;
import com.adamastorx.aggregator.topology.SentimentWindowAggregate;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

/**
 * Interactive Query (IQ) reads against this topology's stores, plus the
 * query-time "correlate sentiment against price movement" combine step
 * (see {@code AggregatorTopology}'s own javadoc for why this is a
 * query-time combine of independent stores, not a topology-level join).
 *
 * <p><b>Real problem found live 2026-08-05: "current window only" was the
 * wrong model.</b> The original v1 of this class only ever looked at the
 * current 15-minute tumbling window -- if no {@code stock.price.tick}/
 * {@code news.sentiment.scored} event had landed in that exact window,
 * {@link #latestKnownState} returned nothing at all. Real trade ticks
 * only flow during actual US market hours and real news is naturally
 * sparse, so most of the time -- including whenever a human actually
 * opens {@code visualizer} to look -- the current window has no data,
 * even though a real price/sentiment was seen recently. A companion
 * change adds a low-frequency REST-poll fallback to {@code
 * market-data-ingestor} (every 30 minutes) so a real price is always
 * available at least that often even outside market hours; real news
 * stays as infrequent as real news actually is. Given both inputs can now
 * be tens of minutes old at any given moment, this class now answers "the
 * most recent known value per ticker, with its real age shown honestly,"
 * not "only what's in the window right now."
 *
 * <p><b>Two independent state-store pairs per field, resolved
 * independently.</b> For each of price and sentiment: prefer the current
 * (still-open) window's own aggregate if it has data (more precise --
 * real movement/rolling-average within an active window); if it doesn't,
 * fall back to {@code AggregatorTopology}'s non-windowed "latest known
 * state" {@code KTable} for that same input (see that class's own
 * javadoc for the store, and its bounded-changelog reasoning). Price and
 * sentiment are resolved independently -- a ticker's price and sentiment
 * ages can differ (real trade ticks and real news arrive on unrelated
 * schedules).
 *
 * <p><b>{@code priceAsOf}/{@code sentimentAsOf} come from Kafka's own
 * real record timestamp, via {@link ValueAndTimestamp}, not a fabricated
 * "now."</b> Both this class's windowed stores and {@code
 * AggregatorTopology}'s new latest-known {@code KTable}s are, by default,
 * backed by Kafka Streams' own {@code Timestamped*Store} implementations
 * (a Kafka Streams built-in since KIP-258, not something this class
 * implements) -- queried here via {@link QueryableStoreTypes#timestampedWindowStore()}
 * / {@link QueryableStoreTypes#timestampedKeyValueStore()} instead of the
 * plain (untimestamped) store types the v1 of this class used. {@link
 * ValueAndTimestamp#timestamp()} is the real timestamp of whichever
 * record most recently updated that key/window -- the same Kafka-level
 * record timestamp this whole topology already trusts for windowing (see
 * {@code AggregatorTopology}'s "Windowing on Kafka's own record
 * timestamp"), not a new, separate notion of time this class invents.
 *
 * <p><b>"No data at all" still means something real.</b> If the
 * latest-known price {@code KTable} itself has no entry for a ticker,
 * that ticker has genuinely never had a price tick land since this
 * process started -- {@link #latestKnownState} still returns {@link
 * Optional#empty()} for exactly that case (an unwatched/misconfigured
 * ticker, or the first few minutes after a fresh deploy/restart before
 * the first real tick or the first 30-minute REST-poll fallback lands).
 * Sentiment has no equivalent "gates the whole response" role -- a
 * ticker with a real, known price but zero sentiment ever (a real,
 * common case, news is sparse) still returns a real response with {@code
 * sentimentSampleCount}/{@code avgSentiment}/{@code sentimentAsOf} all
 * {@code null}, unchanged from before.
 *
 * <p><b>Single replica, so every partition is always local.</b> This
 * service runs {@code replicas: 1} (platform's own {@code
 * deployment.yaml}, same M13 convention every other service in this
 * milestone uses) -- a real distributed Kafka Streams app would need
 * {@link KafkaStreams#streamsMetadataForStore} to route a query for a
 * given key to whichever instance actually holds its partition; with one
 * replica holding all partitions of every topic, that whole "which
 * instance has this key" concern does not apply. Stated here explicitly
 * as a scale-specific simplification, not an oversight -- revisit if
 * this service is ever scaled beyond one replica.
 *
 * <p><b>{@code fetch(key, time)} requires {@code time} to be the
 * window's own exact start timestamp.</b> A real finding from this
 * module's original implementation session, verified against a live
 * embedded {@link KafkaStreams} instance (not merely {@code
 * TopologyTestDriver}, to rule out a test-only quirk): {@code
 * store.fetch(key, System.currentTimeMillis())} (an arbitrary point
 * inside the current window) silently returns {@code null} even when
 * {@code store.all()} shows the exact same key/window holds real data --
 * only {@code fetch(key, windowStartMs)} (the floored window boundary)
 * finds it. Still applies to the current-window lookups below.
 */
@Service
public class AggregateQueryService {

    private final StreamsBuilderFactoryBean factoryBean;
    private final AggregatorProperties properties;

    public AggregateQueryService(StreamsBuilderFactoryBean factoryBean, AggregatorProperties properties) {
        this.factoryBean = factoryBean;
        this.properties = properties;
    }

    /** True once the underlying {@link KafkaStreams} instance is RUNNING -- see {@link #latestKnownState}'s javadoc. */
    public boolean isReady() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        return streams != null && streams.state() == KafkaStreams.State.RUNNING;
    }

    /**
     * One ticker's best-known price/sentiment: the current (still-open)
     * tumbling window's aggregate if it has data, otherwise the most
     * recently known value regardless of age (see this class's own
     * javadoc). Empty only if either: the app isn't RUNNING yet (most
     * likely mid state-store-restoration after a restart -- backlog #81's
     * own AC scenario, see {@code StateRestoreMetrics}), or this ticker
     * has never had a price tick land since this process started (an
     * unwatched/misconfigured ticker, or the first few minutes after a
     * fresh deploy -- both real, both simply "no data yet," not an
     * error).
     */
    public Optional<TickerAggregateResponse> latestKnownState(String ticker) {
        if (!isReady()) {
            return Optional.empty();
        }
        KafkaStreams streams = factoryBean.getKafkaStreams();

        long windowSizeMs = properties.window().toMillis();
        long now = System.currentTimeMillis();
        long windowStartMs = Math.floorDiv(now, windowSizeMs) * windowSizeMs;
        long windowEndMs = windowStartMs + windowSizeMs;

        PriceSnapshot price = resolvePrice(streams, ticker, windowStartMs);
        if (price == null) {
            // Genuinely never seen a price tick for this ticker -- the
            // real "no data at all" case, see this method's own javadoc.
            return Optional.empty();
        }
        SentimentSnapshot sentiment = resolveSentiment(streams, ticker, windowStartMs);

        return Optional.of(new TickerAggregateResponse(
                ticker,
                Instant.ofEpochMilli(windowStartMs),
                Instant.ofEpochMilli(windowEndMs),
                price.agg().tickCount(),
                price.agg().firstPrice(),
                price.agg().lastPrice(),
                price.agg().movement(),
                price.agg().movementPct(),
                price.asOf(),
                sentiment == null ? null : sentiment.agg().sampleCount(),
                sentiment == null ? null : sentiment.agg().averageScore(),
                sentiment == null ? null : sentiment.asOf()));
    }

    /** {@code GET /aggregates} (all watchlisted tickers) -- the same per-ticker lookup, batched. */
    public List<TickerAggregateResponse> latestKnownStateForWatchlist() {
        return properties.watchlist().stream()
                .map(this::latestKnownState)
                .flatMap(Optional::stream)
                .toList();
    }

    /**
     * Current window's price aggregate if it has one; otherwise the
     * "latest known" {@code KTable}'s single most recent tick,
     * synthesized into a one-tick {@link PriceWindowAggregate} via {@link
     * PriceWindowAggregate#accumulate} -- the exact same fold the real
     * windowed aggregation uses, so {@code firstPrice == lastPrice ==}
     * that one known price and {@code movement == 0}: an honest "this is
     * the only data point we have," not a fabricated multi-tick
     * movement. {@code null} only when this ticker has never had a price
     * tick at all.
     */
    private PriceSnapshot resolvePrice(KafkaStreams streams, String ticker, long windowStartMs) {
        try {
            ReadOnlyWindowStore<String, ValueAndTimestamp<PriceWindowAggregate>> priceStore = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            properties.priceWindowStoreName(),
                            QueryableStoreTypes.<String, PriceWindowAggregate>timestampedWindowStore()));
            ValueAndTimestamp<PriceWindowAggregate> windowed = priceStore.fetch(ticker, windowStartMs);
            if (windowed != null) {
                return new PriceSnapshot(windowed.value(), Instant.ofEpochMilli(windowed.timestamp()));
            }
        } catch (InvalidStateStoreException e) {
            // Store exists in the topology but isn't queryable right this
            // instant (e.g. a rebalance/restore just started after
            // isReady()'s check above passed) -- a real, transient state,
            // not a bug; fall through to the latest-known fallback below
            // rather than failing the whole request.
        }

        try {
            ReadOnlyKeyValueStore<String, ValueAndTimestamp<StockPriceTick>> latestStore = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            properties.latestPriceStoreName(),
                            QueryableStoreTypes.<String, StockPriceTick>timestampedKeyValueStore()));
            ValueAndTimestamp<StockPriceTick> latest = latestStore.get(ticker);
            if (latest == null) {
                return null; // never seen this ticker at all
            }
            PriceWindowAggregate synthetic = PriceWindowAggregate.empty().accumulate(latest.value().price());
            return new PriceSnapshot(synthetic, Instant.ofEpochMilli(latest.timestamp()));
        } catch (InvalidStateStoreException e) {
            // Same transient reasoning as above; unlike the sentiment
            // side, price gates the whole response (see this class's own
            // javadoc), so this genuinely means "can't answer right now,"
            // not "no data" -- both collapse to the same caller-visible
            // Optional.empty() either way.
            return null;
        }
    }

    /**
     * Current window's sentiment aggregate if it has one; otherwise the
     * "latest known" {@code KTable}'s single most recent score,
     * synthesized the same way {@link #resolvePrice} does. {@code null}
     * when this ticker has never had a sentiment event at all -- a real,
     * common state (news is sparse), not an error; unlike price, this
     * does not gate the overall response.
     */
    private SentimentSnapshot resolveSentiment(KafkaStreams streams, String ticker, long windowStartMs) {
        try {
            ReadOnlyWindowStore<String, ValueAndTimestamp<SentimentWindowAggregate>> sentimentStore = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            properties.sentimentWindowStoreName(),
                            QueryableStoreTypes.<String, SentimentWindowAggregate>timestampedWindowStore()));
            ValueAndTimestamp<SentimentWindowAggregate> windowed = sentimentStore.fetch(ticker, windowStartMs);
            if (windowed != null) {
                return new SentimentSnapshot(windowed.value(), Instant.ofEpochMilli(windowed.timestamp()));
            }
        } catch (InvalidStateStoreException e) {
            // Same reasoning as resolvePrice's own try block above.
        }

        try {
            ReadOnlyKeyValueStore<String, ValueAndTimestamp<SentimentScoredEvent>> latestStore = streams.store(
                    StoreQueryParameters.fromNameAndType(
                            properties.latestSentimentStoreName(),
                            QueryableStoreTypes.<String, SentimentScoredEvent>timestampedKeyValueStore()));
            ValueAndTimestamp<SentimentScoredEvent> latest = latestStore.get(ticker);
            if (latest == null) {
                return null; // no sentiment ever for this ticker -- real, common, not an error
            }
            SentimentWindowAggregate synthetic = SentimentWindowAggregate.empty().accumulate(latest.value().score());
            return new SentimentSnapshot(synthetic, Instant.ofEpochMilli(latest.timestamp()));
        } catch (InvalidStateStoreException e) {
            // Sentiment is optional in the response (see this class's own
            // javadoc) -- this degrades to "no sentiment for now" rather
            // than failing the whole request, same as before.
            return null;
        }
    }

    private record PriceSnapshot(PriceWindowAggregate agg, Instant asOf) {}

    private record SentimentSnapshot(SentimentWindowAggregate agg, Instant asOf) {}
}
