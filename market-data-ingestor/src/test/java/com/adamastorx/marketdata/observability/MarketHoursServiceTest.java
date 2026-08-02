package com.adamastorx.marketdata.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

/**
 * Proves backlog #78's AC in the negative and positive direction: a
 * stale-feed condition must fire during real US market hours, and must
 * NOT fire outside them (weekend/after-hours silence isn't a real
 * incident). Fixed instants, not {@code Instant.now()} -- deterministic
 * regardless of when this test actually runs.
 */
class MarketHoursServiceTest {

    private final MarketHoursService marketHoursService = new MarketHoursService();

    @Test
    void weekdayDuringRegularHoursIsMarketHours() {
        // Tuesday 2026-01-06, 10:00 America/New_York -- well inside 09:30-16:00.
        Instant instant = atNewYork(2026, 1, 6, 10, 0);
        assertThat(marketHoursService.isUsMarketHours(instant)).isTrue();
    }

    @Test
    void exactlyAtMarketOpenIsMarketHours() {
        Instant instant = atNewYork(2026, 1, 6, 9, 30);
        assertThat(marketHoursService.isUsMarketHours(instant)).isTrue();
    }

    @Test
    void exactlyAtMarketCloseIsNotMarketHours() {
        // 16:00 is the close boundary -- the AC's window is [09:30, 16:00).
        Instant instant = atNewYork(2026, 1, 6, 16, 0);
        assertThat(marketHoursService.isUsMarketHours(instant)).isFalse();
    }

    @Test
    void beforeMarketOpenIsNotMarketHours() {
        Instant instant = atNewYork(2026, 1, 6, 9, 0);
        assertThat(marketHoursService.isUsMarketHours(instant)).isFalse();
    }

    @Test
    void weekendIsNeverMarketHours() {
        // Saturday 2026-01-10, 12:00 -- squarely inside the weekday window's
        // clock time, but a Saturday.
        Instant saturday = atNewYork(2026, 1, 10, 12, 0);
        Instant sunday = atNewYork(2026, 1, 11, 12, 0);
        assertThat(marketHoursService.isUsMarketHours(saturday)).isFalse();
        assertThat(marketHoursService.isUsMarketHours(sunday)).isFalse();
    }

    @Test
    void weekdayEveningAfterCloseIsNotMarketHours() {
        Instant instant = atNewYork(2026, 1, 6, 20, 0);
        assertThat(marketHoursService.isUsMarketHours(instant)).isFalse();
    }

    private static Instant atNewYork(int year, int month, int day, int hour, int minute) {
        return ZonedDateTime.of(year, month, day, hour, minute, 0, 0, ZoneId.of("America/New_York"))
                .toInstant();
    }
}
