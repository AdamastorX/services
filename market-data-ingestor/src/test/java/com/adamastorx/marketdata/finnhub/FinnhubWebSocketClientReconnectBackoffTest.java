package com.adamastorx.marketdata.finnhub;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * backlog #115: proves the real decision rule directly, the same way
 * {@code KafkaStreamsLivenessHealthIndicatorTest} exercises its own
 * static rule against real inputs rather than a live client.
 *
 * <p>{@link FinnhubWebSocketClient#isRateLimited} is tested against a
 * reconstructed cause chain shaped like the real incident this backlog
 * item is named for: a {@code CompletionException} wrapping a deeper
 * cause whose message contains "429". The JDK's own real exception for
 * this ({@code jdk.internal.net.http.websocket.CheckFailedException})
 * lives in a {@code jdk.internal.*} package, not catchable by type from
 * application code -- exactly why {@code isRateLimited} walks the cause
 * chain by message text instead of by type, and why this test does the
 * same rather than trying to fake the real JDK type.
 */
class FinnhubWebSocketClientReconnectBackoffTest {

    @Test
    void detectsARealHttp429BuriedInTheCauseChain() {
        Throwable real429 = new CompletionException(
                new RuntimeException("java.net.http.WebSocketHandshakeException",
                        new RuntimeException("Unexpected HTTP response status code 429")));
        assertThat(FinnhubWebSocketClient.isRateLimited(real429)).isTrue();
    }

    @Test
    void anOrdinaryConnectionFailureIsNotMisdetectedAs429() {
        Throwable ordinary = new CompletionException(new java.net.ConnectException("Connection refused"));
        assertThat(FinnhubWebSocketClient.isRateLimited(ordinary)).isFalse();
    }

    @Test
    void backoffGrowsExponentiallyFromTheConfiguredBase() {
        Duration base = Duration.ofSeconds(2);
        assertThat(FinnhubWebSocketClient.nextReconnectDelay(base, 0)).isEqualTo(Duration.ofSeconds(2));
        assertThat(FinnhubWebSocketClient.nextReconnectDelay(base, 1)).isEqualTo(Duration.ofSeconds(4));
        assertThat(FinnhubWebSocketClient.nextReconnectDelay(base, 2)).isEqualTo(Duration.ofSeconds(8));
        assertThat(FinnhubWebSocketClient.nextReconnectDelay(base, 3)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void backoffIsCappedAtTheRealMaximum() {
        Duration base = Duration.ofSeconds(2);
        // 2s * 2^8 = 512s, well past the real 5-minute (300s) cap.
        Duration delay = FinnhubWebSocketClient.nextReconnectDelay(base, 8);
        assertThat(delay).isEqualTo(Duration.ofMinutes(5));
        // Failures beyond the exponent cap must not overflow or exceed the same real maximum.
        Duration delayFarBeyond = FinnhubWebSocketClient.nextReconnectDelay(base, 1000);
        assertThat(delayFarBeyond).isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void aRealRateLimitReachesTheCapFasterThanOrdinaryFailures() {
        // backlog #115's own real incident: 285+ ordinary-shaped retries
        // never grew past the flat base delay under the old design. The
        // real fix's whole point is that a *rate-limited* failure reaches
        // a real, meaningful backoff far sooner than that -- proven here
        // by comparing the delay after a small, realistic number of
        // rate-limited rounds against the same round count of ordinary
        // failures.
        Duration base = Duration.ofSeconds(2);
        Duration afterTwoRateLimitedRounds =
                FinnhubWebSocketClient.nextReconnectDelay(base, 2 * FinnhubWebSocketClient.RATE_LIMIT_FAILURE_PENALTY);
        Duration afterTwoOrdinaryFailures = FinnhubWebSocketClient.nextReconnectDelay(base, 2);
        assertThat(afterTwoRateLimitedRounds).isGreaterThan(afterTwoOrdinaryFailures);
    }
}
