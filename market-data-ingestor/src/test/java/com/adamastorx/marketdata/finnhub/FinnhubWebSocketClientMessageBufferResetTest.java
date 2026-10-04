package com.adamastorx.marketdata.finnhub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.adamastorx.marketdata.observability.MarketHoursService;
import com.adamastorx.marketdata.observability.StaleFeedMetrics;
import com.adamastorx.marketdata.tick.MarketDataProperties;
import com.adamastorx.marketdata.tick.StockPriceTick;
import com.adamastorx.marketdata.tick.StockPriceTickPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.net.http.WebSocket;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Backlog #156: {@code messageBuffer} lives on {@link FinnhubWebSocketClient}
 * itself, not on the per-connection {@link
 * FinnhubWebSocketClient.FinnhubListener} -- a partial frame left buffered
 * by a connection that dies before ever sending its closing {@code
 * last=true} fragment used to survive into the next connection and
 * silently corrupt its first message, one lost, self-recovering message
 * per reconnect.
 *
 * <p>Reproduces the real sequence: a first connection buffers a partial,
 * never-completed frame, then a second (reconnected) connection's {@code
 * onOpen} fires followed by its own complete first frame. Before the fix,
 * the leftover partial frame text is silently prepended to the new
 * connection's first message, producing invalid JSON that {@link
 * FinnhubWebSocketClient#handleMessage} can only log and drop -- so {@link
 * StockPriceTickPublisher#publish} is never called for a real trade that
 * did arrive. After the fix, {@code onOpen} clears the buffer, so the new
 * connection's first message parses cleanly on its own.
 */
class FinnhubWebSocketClientMessageBufferResetTest {

    @Test
    void messageBufferIsResetOnReconnectSoALeftoverPartialFrameDoesNotCorruptTheNextConnectionsFirstMessage() {
        MarketDataProperties marketDataProperties =
                new MarketDataProperties(List.of("AAPL"), "stock.price.tick", Duration.ofMinutes(5));
        FinnhubProperties finnhubProperties = new FinnhubProperties(
                "test-token", "wss://ws.finnhub.io", Duration.ofSeconds(1), Duration.ofMinutes(1), Duration.ofSeconds(30));
        StockPriceTickPublisher publisher = mock(StockPriceTickPublisher.class);
        Clock clock = Clock.fixed(Instant.parse("2026-08-24T14:00:00Z"), ZoneId.of("UTC"));
        StaleFeedMetrics staleFeedMetrics = new StaleFeedMetrics(
                marketDataProperties, new MarketHoursService(), clock, new SimpleMeterRegistry());

        FinnhubWebSocketClient client = new FinnhubWebSocketClient(
                finnhubProperties, marketDataProperties, publisher, staleFeedMetrics, new SimpleMeterRegistry());

        // First (doomed) connection: a real trade frame arrives split
        // across TCP segments, and the connection dies mid-frame -- the
        // final `last=true` fragment that would normally flush and clear
        // messageBuffer never arrives.
        WebSocket deadSocket = mock(WebSocket.class);
        FinnhubWebSocketClient.FinnhubListener firstConnectionListener = client.new FinnhubListener();
        firstConnectionListener.onOpen(deadSocket);
        firstConnectionListener.onText(
                deadSocket, "{\"data\":[{\"p\":100.0,\"s\":\"MSFT\",\"t\":1690000000000,\"v\":5}],\"typ", false);

        // Reconnect: a brand new connection (its own fresh listener, the
        // same shape `connect()` builds via `new FinnhubListener()`)
        // opens, then immediately delivers one complete, well-formed
        // trade message as its first frame.
        WebSocket newSocket = mock(WebSocket.class);
        FinnhubWebSocketClient.FinnhubListener secondConnectionListener = client.new FinnhubListener();
        secondConnectionListener.onOpen(newSocket);
        String realFirstMessageOnNewConnection =
                "{\"data\":[{\"p\":123.45,\"s\":\"AAPL\",\"t\":1690000000001,\"v\":10}],\"type\":\"trade\"}";
        secondConnectionListener.onText(newSocket, realFirstMessageOnNewConnection, true);

        // Proves the new connection's first message parsed cleanly on its
        // own, not corrupted by the dead connection's leftover partial
        // frame: without the reset, the concatenated text is invalid JSON,
        // handleMessage logs a parse failure and returns, and publish() is
        // never called at all -- verify()'s default times(1) is exactly
        // the assertion that catches the regression.
        ArgumentCaptor<StockPriceTick> tickCaptor = ArgumentCaptor.forClass(StockPriceTick.class);
        verify(publisher).publish(tickCaptor.capture());
        StockPriceTick tick = tickCaptor.getValue();
        assertThat(tick.ticker()).isEqualTo("AAPL");
        assertThat(tick.price()).isEqualByComparingTo(new BigDecimal("123.45"));
        assertThat(tick.volume()).isEqualByComparingTo(new BigDecimal("10"));
    }
}
