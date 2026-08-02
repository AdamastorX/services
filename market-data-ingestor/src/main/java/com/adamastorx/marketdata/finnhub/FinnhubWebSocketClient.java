package com.adamastorx.marketdata.finnhub;

import com.adamastorx.marketdata.observability.StaleFeedMetrics;
import com.adamastorx.marketdata.tick.MarketDataProperties;
import com.adamastorx.marketdata.tick.StockPriceTick;
import com.adamastorx.marketdata.tick.StockPriceTickPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Connects to Finnhub's free real-time trade websocket ({@code
 * wss://ws.finnhub.io}), subscribes to every watchlisted ticker, and
 * republishes each real trade as a {@link StockPriceTick} via {@link
 * StockPriceTickPublisher}.
 *
 * <p>Built on {@code java.net.http.WebSocket} (the JDK's own built-in
 * client, available since Java 11) -- deliberately not a third-party
 * websocket-client library; see {@code pom.xml}'s own comment.
 *
 * <p><strong>Reconnect-and-resume (backlog #78's AC):</strong> three
 * independent paths all funnel into the same {@link #connect()}:
 *
 * <ol>
 *   <li>{@code onClose}/{@code onError} -- the peer (or the JDK's own
 *       transport) reports the connection is gone.
 *   <li>{@link #watchdog()} -- a 10s-scheduled backstop for a silent,
 *       half-open connection that never completes a close handshake: if
 *       idle past {@code app.finnhub.ping-interval} it sends an RFC 6455
 *       protocol ping (any compliant peer must pong); if idle past {@code
 *       app.finnhub.idle-timeout} with nothing back at all, the
 *       connection is presumed dead and force-reconnected without
 *       waiting for the JDK to notice on its own.
 *   <li>{@link #forceReconnect()} -- the operational/test hook {@link
 *       com.adamastorx.marketdata.admin.ReconnectController} exposes,
 *       used to prove this AC live (see that class's own javadoc for why
 *       an admin-triggered abort, not a NetworkPolicy or an in-pod {@code
 *       ss --kill}, is the real mechanism used here).
 * </ol>
 *
 * <p>Every path re-subscribes to the full watchlist on the new
 * connection ({@link #subscribeAll}) -- "resumes subscriptions" per the
 * AC, not just a bare reconnect that would leave the feed silent even
 * once the socket itself is back up.
 */
@Component
public class FinnhubWebSocketClient {

    private static final Logger log = LoggerFactory.getLogger(FinnhubWebSocketClient.class);

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "finnhub-ws-client");
        thread.setDaemon(true);
        return thread;
    });

    private final FinnhubProperties finnhubProperties;
    private final MarketDataProperties marketDataProperties;
    private final StockPriceTickPublisher publisher;
    private final StaleFeedMetrics staleFeedMetrics;
    private final Counter reconnectCounter;
    private final Counter connectFailureCounter;

    private final AtomicReference<WebSocket> activeSocket = new AtomicReference<>();
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final AtomicReference<Instant> lastFrameReceivedAt = new AtomicReference<>(Instant.now());
    private final StringBuilder messageBuffer = new StringBuilder();

    /**
     * Test/CI seam, not a documented operational knob (kept out of {@link
     * FinnhubProperties}, which documents real domain config): this
     * project's existing test convention is a full {@code @SpringBootTest}
     * context (see e.g. {@code workers}' {@code
     * WorkersMetricsHistogramTest}), which would otherwise make every
     * test run of this module open a real outbound connection to Finnhub
     * with whatever token the test happens to set -- non-deterministic
     * and a real network dependency neither this project's CI nor its
     * "verify against real, live behavior" discipline wants smuggled into
     * a unit/integration test. Defaults to {@code true} for every real
     * deployment; set {@code false} only in test properties.
     */
    @Value("${app.finnhub.auto-connect:true}")
    private boolean autoConnect = true;

    public FinnhubWebSocketClient(
            FinnhubProperties finnhubProperties,
            MarketDataProperties marketDataProperties,
            StockPriceTickPublisher publisher,
            StaleFeedMetrics staleFeedMetrics,
            MeterRegistry meterRegistry) {
        this.finnhubProperties = finnhubProperties;
        this.marketDataProperties = marketDataProperties;
        this.publisher = publisher;
        this.staleFeedMetrics = staleFeedMetrics;
        this.reconnectCounter = Counter.builder("market_data_websocket_reconnects_total")
                .description("Successful (re)connections to the Finnhub websocket, including the first connect "
                        + "at startup -- a real, observable signal that backlog #78's reconnect-and-resume path "
                        + "actually ran, not just that it exists in code")
                .register(meterRegistry);
        this.connectFailureCounter = Counter.builder("market_data_websocket_connect_failures_total")
                .description("Failed attempts to establish the Finnhub websocket connection")
                .register(meterRegistry);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!autoConnect) {
            log.info("app.finnhub.auto-connect=false -- not connecting to Finnhub (test/CI context)");
            return;
        }
        connect();
        scheduler.scheduleAtFixedRate(this::watchdog, 10, 10, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        shuttingDown.set(true);
        scheduler.shutdownNow();
        WebSocket webSocket = activeSocket.getAndSet(null);
        if (webSocket != null) {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "service shutting down");
        }
    }

    /** Exposed for {@link com.adamastorx.marketdata.admin.ReconnectController}; see this class's own javadoc. */
    public void forceReconnect() {
        WebSocket webSocket = activeSocket.getAndSet(null);
        if (webSocket == null) {
            log.info("force-reconnect requested but no active websocket -- a reconnect is likely already scheduled");
            return;
        }
        log.warn("force-reconnect requested -- aborting the live Finnhub websocket connection");
        webSocket.abort();
        scheduleReconnect();
    }

    private void connect() {
        if (shuttingDown.get() || !connecting.compareAndSet(false, true)) {
            return;
        }
        URI uri = URI.create(finnhubProperties.websocketUri() + "?token=" + finnhubProperties.token());
        log.info("Connecting to Finnhub websocket ({} watchlisted tickers)", marketDataProperties.tickers().size());
        httpClient
                .newWebSocketBuilder()
                .buildAsync(uri, new FinnhubListener())
                .whenComplete((webSocket, error) -> {
                    connecting.set(false);
                    if (error != null) {
                        connectFailureCounter.increment();
                        log.warn("Finnhub websocket connect failed: {}", error.toString());
                        scheduleReconnect();
                        return;
                    }
                    lastFrameReceivedAt.set(Instant.now());
                    activeSocket.set(webSocket);
                    reconnectCounter.increment();
                    subscribeAll(webSocket);
                    log.info("Finnhub websocket connected and subscribed to {}", marketDataProperties.tickers());
                });
    }

    private void subscribeAll(WebSocket webSocket) {
        for (String ticker : marketDataProperties.tickers()) {
            webSocket.sendText("{\"type\":\"subscribe\",\"symbol\":\"" + ticker + "\"}", true);
        }
    }

    private void scheduleReconnect() {
        if (shuttingDown.get()) {
            return;
        }
        scheduler.schedule(this::connect, finnhubProperties.reconnectDelay().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void watchdog() {
        if (shuttingDown.get()) {
            return;
        }
        WebSocket webSocket = activeSocket.get();
        if (webSocket == null) {
            // Already mid-reconnect via onClose/onError/connect-failure -- nothing to watch.
            return;
        }
        Duration idle = Duration.between(lastFrameReceivedAt.get(), Instant.now());
        if (idle.compareTo(finnhubProperties.idleTimeout()) >= 0) {
            log.warn(
                    "No frame from Finnhub websocket in {}s -- treating connection as dead, forcing reconnect",
                    idle.toSeconds());
            activeSocket.compareAndSet(webSocket, null);
            webSocket.abort();
            scheduleReconnect();
        } else if (idle.compareTo(finnhubProperties.pingInterval()) >= 0) {
            webSocket.sendPing(ByteBuffer.allocate(0));
        }
    }

    private void handleMessage(String rawMessage) {
        FinnhubMessage message;
        try {
            message = objectMapper.readValue(rawMessage, FinnhubMessage.class);
        } catch (Exception e) {
            log.warn("Failed to parse Finnhub websocket message: {}", e.getMessage());
            return;
        }
        if (message.type() == null) {
            return;
        }
        switch (message.type()) {
            case "trade" -> handleTrade(message);
            case "ping" -> {
                // Finnhub's own application-level keep-alive -- already
                // counted as activity via lastFrameReceivedAt (set by
                // onText before this method runs), nothing else to do.
            }
            case "error" -> log.warn("Finnhub reported an error on this connection: {}", message.msg());
            default -> log.debug("Ignoring unrecognized Finnhub message type: {}", message.type());
        }
    }

    private void handleTrade(FinnhubMessage message) {
        if (message.data() == null) {
            return;
        }
        Instant ingestionTimestamp = Instant.now();
        for (FinnhubTrade trade : message.data()) {
            Instant exchangeTimestamp = Instant.ofEpochMilli(trade.epochMillis());
            StockPriceTick tick = new StockPriceTick(
                    trade.symbol(), trade.price(), trade.volume(), exchangeTimestamp, ingestionTimestamp);
            staleFeedMetrics.recordTick(trade.symbol(), ingestionTimestamp);
            publisher.publish(tick);
        }
    }

    private final class FinnhubListener implements WebSocket.Listener {

        @Override
        public void onOpen(WebSocket webSocket) {
            lastFrameReceivedAt.set(Instant.now());
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            lastFrameReceivedAt.set(Instant.now());
            messageBuffer.append(data);
            webSocket.request(1);
            if (last) {
                String message = messageBuffer.toString();
                messageBuffer.setLength(0);
                handleMessage(message);
            }
            return null;
        }

        @Override
        public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
            lastFrameReceivedAt.set(Instant.now());
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            log.warn("Finnhub websocket closed by peer: {} {}", statusCode, reason);
            activeSocket.compareAndSet(webSocket, null);
            scheduleReconnect();
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            log.warn("Finnhub websocket error: {}", error.toString());
            activeSocket.compareAndSet(webSocket, null);
            scheduleReconnect();
        }
    }
}
