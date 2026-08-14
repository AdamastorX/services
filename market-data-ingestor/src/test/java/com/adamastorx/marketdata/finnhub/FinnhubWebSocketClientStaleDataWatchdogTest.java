package com.adamastorx.marketdata.finnhub;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * backlog #133: real, live incident -- the watchdog's prior liveness
 * check only tracked "any frame received" (Finnhub's own protocol-level
 * ping frames included), which stayed satisfied for ~29 real hours while
 * Finnhub silently stopped delivering real trade data for all 5
 * watchlisted tickers during real US market hours. A human noticed the
 * real, live {@code MarketDataStaleFeed} alert and manually triggered
 * {@link FinnhubWebSocketClient#forceReconnect()}, which fixed it within
 * seconds -- proving a reconnect is the real fix, just one nothing was
 * triggering automatically. {@link
 * FinnhubWebSocketClient#shouldForceReconnectForStaleData} is the pure
 * rule that now does that automatically; tested directly against
 * concrete inputs, the same "extract the real rule, test it as a static
 * method" shape {@code FinnhubWebSocketClientReconnectBackoffTest}
 * already uses for this class's other watchdog/backoff decisions.
 */
class FinnhubWebSocketClientStaleDataWatchdogTest {

    private static final Duration STALE_THRESHOLD = Duration.ofMinutes(5);

    @Test
    void doesNotReconnectWhenNoTickerIsStale() {
        Instant now = Instant.parse("2026-08-14T14:00:00Z");
        assertThat(FinnhubWebSocketClient.shouldForceReconnectForStaleData(
                        false, Instant.EPOCH, now, STALE_THRESHOLD))
                .isFalse();
    }

    @Test
    void reconnectsOnTheFirstRealStaleConditionEvenWithNoPriorReconnect() {
        // lastStaleTriggeredReconnectAt starts at Instant.EPOCH (the
        // field's own real initial value) -- the very first real stale
        // condition must trigger immediately, not wait out a cooldown
        // against a reconnect that never happened.
        Instant now = Instant.parse("2026-08-14T14:00:00Z");
        assertThat(FinnhubWebSocketClient.shouldForceReconnectForStaleData(
                        true, Instant.EPOCH, now, STALE_THRESHOLD))
                .isTrue();
    }

    @Test
    void doesNotReconnectAgainWithinTheCooldownAfterAStaleTriggeredReconnect() {
        // backlog #133's own real-world reasoning: a healthy reconnect
        // took 0-12s to receive a fresh real trade per ticker, so
        // anyTickerStale can legitimately still read true on the very
        // next 10s watchdog tick -- must not reconnect-storm.
        Instant lastReconnect = Instant.parse("2026-08-14T14:00:00Z");
        Instant tenSecondsLater = lastReconnect.plusSeconds(10);
        assertThat(FinnhubWebSocketClient.shouldForceReconnectForStaleData(
                        true, lastReconnect, tenSecondsLater, STALE_THRESHOLD))
                .isFalse();
    }

    @Test
    void reconnectsAgainOnceTheCooldownHasFullyElapsed() {
        // A real, persistent condition (e.g. a genuine upstream Finnhub
        // outage, not just this connection's own state) must still be
        // retried periodically, not backed off forever.
        Instant lastReconnect = Instant.parse("2026-08-14T14:00:00Z");
        Instant afterCooldown = lastReconnect.plus(STALE_THRESHOLD);
        assertThat(FinnhubWebSocketClient.shouldForceReconnectForStaleData(
                        true, lastReconnect, afterCooldown, STALE_THRESHOLD))
                .isTrue();
    }
}
