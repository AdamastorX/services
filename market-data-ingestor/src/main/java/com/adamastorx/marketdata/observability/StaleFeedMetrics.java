package com.adamastorx.marketdata.observability;

import com.adamastorx.marketdata.tick.MarketDataProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

/**
 * The real, observable stale-feed condition backlog #78's AC asks for:
 * "no tick for a watchlisted ticker within N minutes during real US
 * market hours ... not a silent gap -- and after-hours/weekend silence is
 * explicitly NOT treated as that condition."
 *
 * <p>Exposes two gauges per watchlisted ticker on {@code
 * /actuator/prometheus}:
 *
 * <ul>
 *   <li>{@code market_data_seconds_since_last_tick{ticker="AAPL"}} -- a
 *       real-time value, always meaningful, not gated on market hours (so
 *       it's still useful to look at outside market hours -- "how long
 *       since the last trade before close").
 *   <li>{@code market_data_stale_feed{ticker="AAPL"}} -- 1 or 0. This is
 *       the gauge {@code platform/argocd/apps/prometheus.yaml}'s
 *       {@code MarketDataStaleFeed} alert rule actually keys on. The
 *       market-hours check ({@link MarketHoursService}) is baked into the
 *       gauge's own value here, application-side, rather than left for
 *       the PromQL alert expression to re-derive (the same "the
 *       application computes its own real state, the alert just reads a
 *       threshold" shape {@code watchlist_delivery_dlq_depth} already
 *       uses) -- so a plain {@code > 0} in the alert rule is already
 *       correct, no timezone-aware PromQL needed there.
 * </ul>
 *
 * <p>Backed by a plain in-memory map, not persisted -- this service holds
 * no database (see {@code pom.xml}'s own comment): a restart resets every
 * ticker's last-tick time to "now" (constructor), the same accepted
 * "state doesn't survive a restart" tradeoff the rest of this service
 * already makes. That deliberately avoids a false stale reading for the
 * first {@code staleThreshold} after a fresh deploy, when no real tick has
 * arrived yet.
 *
 * <p>Takes a {@link Clock} rather than calling {@code Instant.now()}
 * directly -- the one piece of test seam this class needs to be unit-
 * tested deterministically (staleness is a function of elapsed time and
 * market hours, both of which need a fixed "now" to assert against
 * without a flaky, when-does-CI-happen-to-run dependency).
 */
@Component
public class StaleFeedMetrics {

    private final MarketDataProperties properties;
    private final MarketHoursService marketHoursService;
    private final Clock clock;
    private final Map<String, AtomicReference<Instant>> lastTickByTicker = new ConcurrentHashMap<>();

    public StaleFeedMetrics(
            MarketDataProperties properties,
            MarketHoursService marketHoursService,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.properties = properties;
        this.marketHoursService = marketHoursService;
        this.clock = clock;
        Instant startupInstant = clock.instant();
        for (String ticker : properties.tickers()) {
            AtomicReference<Instant> lastTick = new AtomicReference<>(startupInstant);
            lastTickByTicker.put(ticker, lastTick);

            Gauge.builder("market_data_seconds_since_last_tick", lastTick, ref -> secondsSince(ref.get()))
                    .description("Seconds since the last real trade tick received for this watchlisted ticker")
                    .tag("ticker", ticker)
                    .register(meterRegistry);

            Gauge.builder("market_data_stale_feed", lastTick, ref -> isStale(ticker, ref.get()) ? 1.0 : 0.0)
                    .description("1 if no tick has been received for this ticker within the stale threshold "
                            + "during real US market hours, 0 otherwise (after-hours/weekend silence is never "
                            + "reported stale)")
                    .tag("ticker", ticker)
                    .register(meterRegistry);
        }
    }

    /** Called from {@code FinnhubWebSocketClient} on every real trade tick received. */
    public void recordTick(String ticker, Instant tickInstant) {
        AtomicReference<Instant> lastTick = lastTickByTicker.get(ticker);
        if (lastTick != null) {
            lastTick.set(tickInstant);
        }
    }

    boolean isStale(String ticker, Instant lastTickInstant) {
        if (!marketHoursService.isUsMarketHours(clock.instant())) {
            return false;
        }
        return secondsSince(lastTickInstant) > properties.staleThreshold().toSeconds();
    }

    private double secondsSince(Instant instant) {
        return Duration.between(instant, clock.instant()).toSeconds();
    }
}
