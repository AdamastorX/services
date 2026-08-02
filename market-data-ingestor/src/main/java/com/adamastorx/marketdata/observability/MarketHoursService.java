package com.adamastorx.marketdata.observability;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.springframework.stereotype.Service;

/**
 * A deliberately simple US/Eastern business-hours check (backlog #78's
 * AC: "state the real market-hours logic you use, e.g. a simple
 * US/Eastern business-hours check, not full holiday-calendar precision --
 * that would be gold-plating for a portfolio project").
 *
 * <p><strong>What this checks:</strong> Monday-Friday, 09:30-16:00
 * America/New_York (regular NYSE/Nasdaq trading hours -- all 5 watchlist
 * tickers trade on one of those two exchanges). {@link ZoneId} handles the
 * EST/EDT daylight-saving transition automatically -- the boundary is
 * always wall-clock 09:30/16:00 in New York, not a fixed UTC offset.
 *
 * <p><strong>What this deliberately does NOT check</strong> (the AC's own
 * named, accepted v1 gap): US market holidays (New Year's Day,
 * Thanksgiving, Christmas, etc.) and early-close days (the day after
 * Thanksgiving, Christmas Eve). On a real market holiday that falls on a
 * weekday, this method still returns {@code true} for the normal
 * 09:30-16:00 window even though no trading is actually happening -- a
 * watchlisted ticker going silent that whole session would be reported
 * stale by {@link StaleFeedMetrics}. A full holiday calendar is real,
 * ongoing maintenance burden (it needs a new entry every year) for a
 * failure mode this project's stated discipline (ADR 0021, no
 * gold-plating) doesn't justify carrying for a personal portfolio
 * project's alerting -- a stale-feed page landing on Thanksgiving morning
 * is an acceptable, honestly-stated false positive, not a silent gap.
 */
@Service
public class MarketHoursService {

    private static final ZoneId US_EASTERN = ZoneId.of("America/New_York");
    private static final LocalTime MARKET_OPEN = LocalTime.of(9, 30);
    private static final LocalTime MARKET_CLOSE = LocalTime.of(16, 0);

    public boolean isUsMarketHours(Instant instant) {
        ZonedDateTime easternTime = instant.atZone(US_EASTERN);
        DayOfWeek dayOfWeek = easternTime.getDayOfWeek();
        if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime timeOfDay = easternTime.toLocalTime();
        return !timeOfDay.isBefore(MARKET_OPEN) && timeOfDay.isBefore(MARKET_CLOSE);
    }
}
