package com.adamastorx.aggregator.tick;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * This service's own consumer-side view of {@code stock.price.tick}'s wire
 * shape (backlog #78's real producer:
 * {@code market-data-ingestor/src/main/java/com/adamastorx/marketdata/tick/StockPriceTick.java}).
 * Deliberately narrower than the producer's own record -- that record also
 * carries {@code ingestionTimestamp}, which this one still does not parse
 * (this topology windows on Kafka's own record timestamp, see {@code
 * AggregatorTopology}'s javadoc, not a business-payload timestamp field, so
 * there is no need to reproduce {@code market-data-ingestor}'s
 * serializer-format question for that one). {@code exchangeTimestamp} and
 * {@code source} were added for backlog #91: the real end-to-end freshness
 * SLI this item exists to build needs the actual Finnhub trade timestamp
 * (not Kafka's own record timestamp, which is closer to ingestion time),
 * and needs to tell a real-time websocket tick apart from {@code
 * FinnhubQuotePoller}'s 30-minute REST-poll fallback -- see {@code
 * AggregatorTopology}'s own javadoc on where this is used.
 *
 * <p>Not a shared Java type with the producer's own record (ADR 0007) --
 * "agree on the JSON shape, not a shared Java type" is this project's
 * existing convention for every Kafka wire contract with a cross-module
 * consumer (see {@code StockPriceTickProducerConfig}'s own javadoc).
 * {@code source} is deserialized as a plain {@code String} (matching the
 * producer's enum's own JSON encoding, {@code "WEBSOCKET"}/{@code
 * "POLL_FALLBACK"}), not a shared Java enum -- same reasoning. {@link
 * com.fasterxml.jackson.annotation.JsonIgnoreProperties} tolerates the
 * extra fields the real payload carries ({@code volume},
 * {@code ingestionTimestamp}) -- this is a deliberate narrowing, not a
 * mismatch to be fixed.
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public record StockPriceTick(String ticker, BigDecimal price, Instant exchangeTimestamp, String source) {}
