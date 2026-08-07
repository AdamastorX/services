package com.adamastorx.marketdata.tick;

/**
 * Which real path produced a {@link StockPriceTick} -- backlog #91's real
 * trap, found during roadmap review before this item was built: {@code
 * FinnhubQuotePoller}'s 30-minute REST-poll fallback publishes onto the
 * same {@code stock.price.tick} topic in the same wire shape as {@code
 * FinnhubWebSocketClient}, specifically to cover off-hours/gaps. A
 * freshness SLI that doesn't distinguish the two would see a real, by-design
 * 30-minute lag on every fallback tick and either alert constantly (if
 * tight) or stay meaningless during real market hours (if loosened to
 * cover it) -- this field is what lets a downstream consumer (here,
 * {@code aggregator}) measure freshness against {@link #WEBSOCKET} ticks
 * only.
 */
public enum TickSource {
    WEBSOCKET,
    POLL_FALLBACK
}
