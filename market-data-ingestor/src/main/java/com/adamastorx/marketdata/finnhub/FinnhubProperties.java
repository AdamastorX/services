package com.adamastorx.marketdata.finnhub;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Connection config for Finnhub's free real-time trade websocket (ADR
 * 0029). {@code token} is never logged or included in any exception
 * message this module throws -- {@link FinnhubWebSocketClient} builds the
 * full connect URI (token included, Finnhub's own auth mechanism: a query
 * parameter, not a header) but keeps that URI itself out of log lines.
 *
 * @param token the real Finnhub API key (backlog #78's AC: "stored as a
 *     real Kubernetes Secret ... never committed to git") -- sourced from
 *     the {@code FINNHUB_API_KEY} env var (application.yml), which the
 *     Deployment populates via {@code secretKeyRef} against the
 *     already-provisioned {@code finnhub-api-key} Secret
 *     (platform/kubernetes/market-data-ingestor/deployment.yaml)
 * @param websocketUri Finnhub's real websocket base URI, {@code
 *     wss://ws.finnhub.io} -- the token is appended as a query parameter
 *     at connect time, not baked into this value, so it never appears in
 *     application.yml's own default
 * @param reconnectDelay fixed delay before retrying a dropped/failed
 *     connection (backlog #78's AC: "reconnects automatically on a
 *     dropped connection"). A plain fixed delay, not exponential backoff
 *     with jitter -- Finnhub's free tier has no documented reconnect-rate
 *     limit this project found during ADR 0029's research, and a 5-ticker
 *     watchlist reconnecting every few seconds during a real outage is a
 *     negligible request rate; exponential backoff would be defensive
 *     engineering for a failure mode (Finnhub actively rate-limiting
 *     reconnect attempts) this project has no evidence of, the same
 *     no-gold-plating bar ADR 0021 applies elsewhere
 * @param idleTimeout if no frame (trade, ping, or pong) is received for
 *     this long, the connection is presumed dead and force-reconnected --
 *     the backstop for a silent, half-open TCP connection that never
 *     completes a close handshake (the case {@code onClose}/{@code
 *     onError} alone cannot catch)
 * @param pingInterval how often this client sends a WebSocket-protocol
 *     ping while otherwise idle, to detect that silent-half-open case
 *     promptly rather than waiting the full {@code idleTimeout} on a
 *     connection that would otherwise look merely quiet
 */
@ConfigurationProperties("app.finnhub")
public record FinnhubProperties(
        String token, String websocketUri, Duration reconnectDelay, Duration idleTimeout, Duration pingInterval) {}
