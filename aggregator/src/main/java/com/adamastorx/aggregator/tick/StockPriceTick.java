package com.adamastorx.aggregator.tick;

import java.math.BigDecimal;

/**
 * This service's own consumer-side view of {@code stock.price.tick}'s wire
 * shape (backlog #78's real producer:
 * {@code market-data-ingestor/src/main/java/com/adamastorx/marketdata/tick/StockPriceTick.java}).
 * Deliberately narrower than the producer's own record -- that record
 * carries {@code ticker, price, volume, exchangeTimestamp,
 * ingestionTimestamp}; this one only deserializes {@code ticker} and
 * {@code price}, the two fields backlog #81's windowed price-movement
 * aggregate actually needs. {@code volume} has no AC-named use here; the
 * two {@code Instant} fields are deliberately not parsed at all -- this
 * topology windows on Kafka's own record timestamp (see {@code
 * AggregatorTopology}'s javadoc), not a business-payload timestamp field,
 * so there is no need to reproduce
 * {@code market-data-ingestor}'s serializer-format question here.
 *
 * <p>Not a shared Java type with the producer's own record (ADR 0007) --
 * "agree on the JSON shape, not a shared Java type" is this project's
 * existing convention for every Kafka wire contract with a cross-module
 * consumer (see {@code StockPriceTickProducerConfig}'s own javadoc).
 * {@link com.fasterxml.jackson.annotation.JsonIgnoreProperties} tolerates
 * the extra fields the real payload carries -- this is a deliberate
 * narrowing, not a mismatch to be fixed.
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public record StockPriceTick(String ticker, BigDecimal price) {}
