package com.adamastorx.marketdata.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.adamastorx.marketdata.tick.MarketDataProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Proves backlog #78's AC directly: a watchlisted ticker with no tick
 * inside the stale threshold is reported stale during real US market
 * hours, and explicitly NOT stale outside them (after-hours/weekend
 * silence must not page).
 */
class StaleFeedMetricsTest {

    private static final Instant DURING_MARKET_HOURS =
            ZonedDateTime.of(2026, 1, 6, 10, 0, 0, 0, ZoneId.of("America/New_York")).toInstant();
    private static final Instant AFTER_HOURS =
            ZonedDateTime.of(2026, 1, 6, 20, 0, 0, 0, ZoneId.of("America/New_York")).toInstant();

    private final MarketDataProperties properties =
            new MarketDataProperties(List.of("AAPL"), "stock.price.tick", Duration.ofMinutes(5));
    private final MarketHoursService marketHoursService = new MarketHoursService();

    @Test
    void tickerWithNoRecentTickDuringMarketHoursIsStale() {
        Clock clock = Clock.fixed(DURING_MARKET_HOURS, ZoneId.of("UTC"));
        StaleFeedMetrics metrics =
                new StaleFeedMetrics(properties, marketHoursService, clock, new SimpleMeterRegistry());

        Instant sixMinutesAgo = DURING_MARKET_HOURS.minus(Duration.ofMinutes(6));
        metrics.recordTick("AAPL", sixMinutesAgo);

        assertThat(metrics.isStale("AAPL", sixMinutesAgo)).isTrue();
    }

    @Test
    void tickerWithARecentTickDuringMarketHoursIsNotStale() {
        Clock clock = Clock.fixed(DURING_MARKET_HOURS, ZoneId.of("UTC"));
        StaleFeedMetrics metrics =
                new StaleFeedMetrics(properties, marketHoursService, clock, new SimpleMeterRegistry());

        Instant oneMinuteAgo = DURING_MARKET_HOURS.minus(Duration.ofMinutes(1));
        metrics.recordTick("AAPL", oneMinuteAgo);

        assertThat(metrics.isStale("AAPL", oneMinuteAgo)).isFalse();
    }

    @Test
    void sameSilenceAfterHoursIsNeverStale() {
        // The AC's own explicit negative case: after-hours silence, even
        // well past the stale threshold, must not be reported stale.
        Clock clock = Clock.fixed(AFTER_HOURS, ZoneId.of("UTC"));
        StaleFeedMetrics metrics =
                new StaleFeedMetrics(properties, marketHoursService, clock, new SimpleMeterRegistry());

        Instant hoursAgo = AFTER_HOURS.minus(Duration.ofHours(3));
        metrics.recordTick("AAPL", hoursAgo);

        assertThat(metrics.isStale("AAPL", hoursAgo)).isFalse();
    }

    @Test
    void freshlyStartedServiceIsNotStaleBeforeAnyTickArrives() {
        // Constructor seeds lastTickByTicker at "now" (the class's own
        // javadoc) -- a fresh deploy must not immediately read as stale.
        Clock clock = Clock.fixed(DURING_MARKET_HOURS, ZoneId.of("UTC"));
        StaleFeedMetrics metrics =
                new StaleFeedMetrics(properties, marketHoursService, clock, new SimpleMeterRegistry());

        assertThat(metrics.isStale("AAPL", DURING_MARKET_HOURS)).isFalse();
    }
}
