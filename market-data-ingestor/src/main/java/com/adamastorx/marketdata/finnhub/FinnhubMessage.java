package com.adamastorx.marketdata.finnhub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * The envelope every Finnhub websocket frame arrives in. {@code type} is
 * one of {@code "trade"} (the only kind this service acts on), {@code
 * "ping"} (a keep-alive Finnhub sends periodically at the application
 * layer, separate from RFC 6455 protocol-level ping/pong frames), or
 * {@code "error"} (e.g. an invalid symbol or a free-tier limit hit on
 * subscribe) -- all three are real, observed message types; anything else
 * is logged and otherwise ignored rather than assumed impossible.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FinnhubMessage(String type, List<FinnhubTrade> data, String msg) {}
