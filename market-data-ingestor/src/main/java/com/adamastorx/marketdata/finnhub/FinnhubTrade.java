package com.adamastorx.marketdata.finnhub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/**
 * One entry of a Finnhub {@code "type":"trade"} message's {@code data}
 * array -- Finnhub's real, documented field names, single-letter and
 * terse by their own design (verified against finnhub.io's live websocket
 * docs during ADR 0029's research): {@code s} symbol, {@code p} last
 * price, {@code v} volume, {@code t} UNIX ms timestamp, {@code c} trade
 * conditions (ignored here -- this service has no use for them).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FinnhubTrade(
        @JsonProperty("s") String symbol,
        @JsonProperty("p") BigDecimal price,
        @JsonProperty("v") BigDecimal volume,
        @JsonProperty("t") long epochMillis) {}
