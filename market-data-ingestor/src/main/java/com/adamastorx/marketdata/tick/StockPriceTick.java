package com.adamastorx.marketdata.tick;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The {@code stock.price.tick} Kafka event's wire shape (backlog #78's AC:
 * "ticker, price, volume, exchange timestamp, ingestion timestamp"). A
 * plain record, deliberately not shared with any other module's compiled
 * class (ADR 0007) -- this topic has exactly one producer (this service)
 * and, until #81 (`aggregator`) exists, no consumer in this repo at all.
 *
 * @param ticker the watchlisted symbol, e.g. {@code "AAPL"} (Finnhub's own
 *     {@code s} field, matched verbatim -- no normalization needed, the
 *     watchlist is already expressed in Finnhub's own symbol format)
 * @param price the last trade price (Finnhub's {@code p}, a real decimal
 *     price -- {@link BigDecimal}, not {@code double}, for the same reason
 *     any money-shaped value should avoid binary floating-point error)
 * @param volume the trade's size (Finnhub's {@code v} -- number of shares,
 *     which Finnhub itself represents as a JSON number, not necessarily an
 *     integer for fractional-share trades)
 * @param exchangeTimestamp when the trade actually happened on the
 *     exchange (Finnhub's {@code t}, epoch milliseconds)
 * @param ingestionTimestamp when this service received the trade over the
 *     websocket -- the two timestamps together are what let a downstream
 *     consumer (or a human during this item's own live verification) prove
 *     the AC's "under 2s receipt-to-publish" bound without trusting a
 *     single number alone
 */
public record StockPriceTick(
        String ticker, BigDecimal price, BigDecimal volume, Instant exchangeTimestamp, Instant ingestionTimestamp) {}
