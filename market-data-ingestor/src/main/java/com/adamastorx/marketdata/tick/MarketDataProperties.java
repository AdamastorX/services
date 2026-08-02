package com.adamastorx.marketdata.tick;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The fixed watchlist and this service's own topic/staleness config
 * (backlog #78's AC: "a fixed, small watchlist ... finalized at
 * implementation"). Bound from {@code app.market-data.*}
 * (application.yml) rather than a database row or a runtime-mutable
 * admin API -- there is no CRUD surface for the watchlist, deliberately:
 * changing it is a one-line config change plus a redeploy, exactly the
 * "no gold-plating" bar ADR 0021 sets for a component with no stated need
 * for a dynamic watchlist yet.
 *
 * @param tickers the fixed watchlist, Finnhub's own symbol format (e.g.
 *     {@code AAPL}) -- 5 large-cap US tickers, finalized per the AC's own
 *     suggested list: Apple, Microsoft, Alphabet, Amazon, Tesla. All five
 *     trade on Nasdaq/NYSE during the same US market-hours window {@link
 *     com.adamastorx.marketdata.observability.MarketHoursService} checks,
 *     so one market-hours definition covers the whole watchlist.
 * @param stockPriceTickTopic the {@code stock.price.tick} Kafka topic
 * @param staleThreshold how long a watchlisted ticker can go without a
 *     trade tick during real US market hours before {@link
 *     com.adamastorx.marketdata.observability.StaleFeedMetrics} reports it
 *     stale. 5 minutes: each of these five tickers is among the most
 *     heavily-traded US equities, typically producing several trades per
 *     *second* during regular hours -- 5 minutes of total silence from any
 *     one of them during market hours is already a strong signal something
 *     is wrong (the websocket subscription silently dropped, Finnhub's
 *     feed itself is degraded), not ordinary quiet trading. A stated,
 *     reasonable v1 pick, not a number derived from a real observed
 *     distribution this service hasn't run long enough to have yet.
 */
@ConfigurationProperties("app.market-data")
public record MarketDataProperties(
        List<String> tickers, String stockPriceTickTopic, Duration staleThreshold) {}
