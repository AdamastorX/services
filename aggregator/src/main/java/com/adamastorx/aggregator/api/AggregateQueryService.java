package com.adamastorx.aggregator.api;

import com.adamastorx.aggregator.config.AggregatorProperties;
import com.adamastorx.aggregator.topology.PriceWindowAggregate;
import com.adamastorx.aggregator.topology.SentimentWindowAggregate;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

/**
 * Interactive Query (IQ) reads against this topology's two windowed state
 * stores, plus the query-time "correlate sentiment against price
 * movement" combine step (see {@code AggregatorTopology}'s own javadoc
 * for why this is a query-time combine of two independent stores, not a
 * topology-level join).
 *
 * <p><b>Single replica, so every partition is always local.</b> This
 * service runs {@code replicas: 1} (platform's own {@code
 * deployment.yaml}, same M13 convention every other service in this
 * milestone uses) -- a real distributed Kafka Streams app would need
 * {@link KafkaStreams#streamsMetadataForStore} to route a query for a
 * given key to whichever instance actually holds its partition; with one
 * replica holding all partitions of both topics, that whole "which
 * instance has this key" concern does not apply. Stated here explicitly
 * as a scale-specific simplification, not an oversight -- revisit if
 * this service is ever scaled beyond one replica.
 *
 * <p><b>{@code fetch(key, time)} requires {@code time} to be the
 * window's own exact start timestamp.</b> A real finding from this
 * implementation session, verified against a live embedded {@link
 * KafkaStreams} instance (not merely {@code TopologyTestDriver}, to rule
 * out a test-only quirk): {@code store.fetch(key, System.currentTimeMillis())}
 * (an arbitrary point inside the current window) silently returns {@code
 * null} even when {@code store.all()} shows the exact same key/window
 * holds real data -- only {@code fetch(key, windowStartMs)} (the
 * floored window boundary) finds it. An earlier draft of this method
 * computed {@code windowStartMs} correctly but then called {@code
 * fetch(ticker, now)} anyway -- caught by {@code AggregatorTopologyTest},
 * fixed here, not shipped.
 */
@Service
public class AggregateQueryService {

    private final StreamsBuilderFactoryBean factoryBean;
    private final AggregatorProperties properties;

    public AggregateQueryService(StreamsBuilderFactoryBean factoryBean, AggregatorProperties properties) {
        this.factoryBean = factoryBean;
        this.properties = properties;
    }

    /** True once the underlying {@link KafkaStreams} instance is RUNNING -- see {@link #currentWindow}'s javadoc. */
    public boolean isReady() {
        KafkaStreams streams = factoryBean.getKafkaStreams();
        return streams != null && streams.state() == KafkaStreams.State.RUNNING;
    }

    /**
     * The current (still-open) tumbling window's aggregate for one
     * ticker. Empty if either: the app isn't RUNNING yet (most likely
     * mid state-store-restoration after a restart -- exactly backlog
     * #81's own AC scenario, see {@code StateRestoreMetrics}), or no
     * price tick for this ticker has landed in the current window at all
     * (an unwatched/unknown ticker, or the window just rolled over and no
     * tick has arrived yet -- both real, both simply "no data yet," not
     * an error).
     */
    public Optional<TickerAggregateResponse> currentWindow(String ticker) {
        if (!isReady()) {
            return Optional.empty();
        }
        KafkaStreams streams = factoryBean.getKafkaStreams();

        long windowSizeMs = properties.window().toMillis();
        long now = System.currentTimeMillis();
        long windowStartMs = Math.floorDiv(now, windowSizeMs) * windowSizeMs;
        long windowEndMs = windowStartMs + windowSizeMs;

        PriceWindowAggregate priceAgg;
        try {
            ReadOnlyWindowStore<String, PriceWindowAggregate> priceStore = streams.store(StoreQueryParameters
                    .fromNameAndType(properties.priceWindowStoreName(), QueryableStoreTypes.<String, PriceWindowAggregate>windowStore()));
            // fetch(key, time) requires "time" to be the window's own
            // exact start timestamp, not merely a point that falls
            // within it -- verified for real against a live embedded
            // KafkaStreams instance (not just TopologyTestDriver): an
            // earlier draft called fetch(ticker, now) directly and every
            // query silently returned empty except in the one
            // millisecond a window opened. windowStartMs (computed
            // above), not now, is the correct argument.
            priceAgg = priceStore.fetch(ticker, windowStartMs);
        } catch (InvalidStateStoreException e) {
            // Store exists in the topology but isn't queryable right this
            // instant (e.g. a rebalance/restore just started after
            // isReady()'s check above passed) -- a real, transient state,
            // not a bug; treated the same as "no data yet."
            return Optional.empty();
        }
        if (priceAgg == null) {
            return Optional.empty();
        }

        Long sentimentSampleCount = null;
        Double avgSentiment = null;
        try {
            ReadOnlyWindowStore<String, SentimentWindowAggregate> sentimentStore =
                    streams.store(StoreQueryParameters.fromNameAndType(
                            properties.sentimentWindowStoreName(),
                            QueryableStoreTypes.<String, SentimentWindowAggregate>windowStore()));
            SentimentWindowAggregate sentimentAgg = sentimentStore.fetch(ticker, windowStartMs);
            if (sentimentAgg != null) {
                sentimentSampleCount = sentimentAgg.sampleCount();
                avgSentiment = sentimentAgg.averageScore();
            }
        } catch (InvalidStateStoreException e) {
            // Same reasoning as above -- sentiment data is optional in
            // this response anyway, so this degrades to "no sentiment
            // this window" rather than failing the whole request.
        }

        return Optional.of(new TickerAggregateResponse(
                ticker,
                Instant.ofEpochMilli(windowStartMs),
                Instant.ofEpochMilli(windowEndMs),
                priceAgg.tickCount(),
                priceAgg.firstPrice(),
                priceAgg.lastPrice(),
                priceAgg.movement(),
                priceAgg.movementPct(),
                sentimentSampleCount,
                avgSentiment));
    }

    /** {@code GET /aggregates} (all watchlisted tickers) -- the same per-ticker lookup, batched. */
    public List<TickerAggregateResponse> currentWindowForWatchlist() {
        return properties.watchlist().stream()
                .map(this::currentWindow)
                .flatMap(Optional::stream)
                .toList();
    }
}
