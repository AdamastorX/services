package com.adamastorx.aggregator.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Backlog #81's own tunables. {@code app.aggregator.*}, same
 * relaxed-binding-prefix convention every other module's own
 * {@code *Properties} class uses.
 *
 * @param stockPriceTickTopic input topic 1 (backlog #78's output)
 * @param newsSentimentScoredTopic input topic 2 (backlog #80's output)
 * @param window the tumbling window size. **15 minutes**, the AC's own
 *     suggested default, kept as the real default: see {@code
 *     README.md}'s "ADR 0011, resolved" section for the real, measured
 *     state-store-recovery numbers this choice is justified against, not
 *     a guess. Also directly bounds each windowed store's changelog
 *     retention (retention = window + grace, {@link
 *     org.apache.kafka.streams.kstream.TimeWindows#ofSizeAndGrace}) --
 *     the actual mechanism that turns "a full changelog rebuild after a
 *     broker/topic loss" from an open-ended cost into a bounded, accepted
 *     one (ADR 0029/ADR 0011's real conflict, backlog #81's own AC).
 * @param grace how long after a window closes a late-arriving record is
 *     still accepted into it. Zero -- see {@code AggregatorTopology}'s
 *     javadoc for why a real-time price/sentiment feed has no accepted
 *     late-arrival case worth extending changelog retention for.
 * @param latestPriceStoreName the non-windowed {@code KTable<String,
 *     StockPriceTick>} store name -- the "latest known price per ticker,
 *     however old" fallback {@code AggregateQueryService} reads when the
 *     current window has none. See {@code AggregatorTopology}'s javadoc
 *     and this module's README for why this store's changelog is bounded
 *     by ticker count, not time, and does not reopen ADR 0011's resolution
 *     for the windowed stores above.
 * @param latestSentimentStoreName the same "latest known, however old"
 *     store for {@code news.sentiment.scored}, independent of {@code
 *     latestPriceStoreName} -- a ticker's price and sentiment can each be
 *     fresh or stale independently of the other.
 * @param watchlist the fixed ticker list {@code GET /aggregates} (all
 *     tickers) iterates -- same 5 large-cap tickers {@code
 *     market-data-ingestor}/{@code news-ingestor} already use (not an
 *     independent guess, the same precedent {@code news-ingestor}'s own
 *     {@code application.yml} comment states for its own watchlist
 *     default). This service has no dependency on those two modules'
 *     config at runtime (ADR 0007: no shared Java types/config across
 *     modules) so the list is duplicated here deliberately, the same way
 *     every M13 service's watchlist already is.
 */
@ConfigurationProperties(prefix = "app.aggregator")
public record AggregatorProperties(
        String stockPriceTickTopic,
        String newsSentimentScoredTopic,
        Duration window,
        Duration grace,
        String priceWindowStoreName,
        String sentimentWindowStoreName,
        String latestPriceStoreName,
        String latestSentimentStoreName,
        List<String> watchlist) {}
