package com.adamastorx.marketdata.finnhub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/**
 * Finnhub's real, documented REST {@code GET /api/v1/quote} response shape:
 * {@code {"c":<current/last price>,"d":<change>,"dp":<percent change>,
 * "h":<high>,"l":<low>,"o":<open>,"pc":<previous close>,"t":<unix
 * timestamp>}}. This sandbox has no live {@code FINNHUB_API_KEY} to verify
 * the real response against at build time ({@link FinnhubQuotePoller}'s own
 * javadoc), so this is the documented shape, stated explicitly here rather
 * than silently assumed correct.
 *
 * <p>Only {@code c} and {@code t} are modeled -- {@code d}/{@code dp}/
 * {@code h}/{@code l}/{@code o}/{@code pc} have no use in this service, the
 * same "model only the fields this service actually needs, ignore the
 * rest" discipline {@link FinnhubTrade} already applies to the websocket's
 * own {@code c} trade-conditions field.
 *
 * @param currentPrice Finnhub's {@code c} -- the current/last price. A
 *     real quote for a symbol Finnhub has no data for comes back as {@code
 *     0} for every field (not an HTTP error) -- {@link FinnhubQuotePoller}
 *     treats {@code 0} (or a missing value) the same as a failed poll for
 *     that ticker, never as a real $0.00 price for a large-cap watchlisted
 *     ticker.
 * @param epochSeconds Finnhub's {@code t} -- unlike the trade websocket's
 *     own {@code t} ({@link FinnhubTrade#epochMillis()}, epoch
 *     *milliseconds*), this REST endpoint's {@code t} is epoch *seconds*.
 *     A real, documented difference between these two Finnhub APIs, not a
 *     typo.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FinnhubQuote(@JsonProperty("c") BigDecimal currentPrice, @JsonProperty("t") long epochSeconds) {}
