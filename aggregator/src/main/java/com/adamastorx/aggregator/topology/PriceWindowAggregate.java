package com.adamastorx.aggregator.topology;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * The running aggregate this topology's {@code price-window-store} keeps
 * per (ticker, window): tick count, first/last/min/max price seen in the
 * window so far. {@code firstPrice}/{@code lastPrice} are "first/last in
 * this app's own processing order," not exchange-timestamp order -- an
 * accepted v1 approximation, not a gap glossed over: {@code
 * market-data-ingestor} is a single replica with one producer thread
 * keyed by ticker (its own {@code StockPriceTickProducerConfig} javadoc),
 * so in-partition order already matches real receive order for a given
 * ticker in practice; a genuine out-of-order guarantee would need
 * per-record exchange-timestamp comparison this v1 doesn't do (ADR 0021).
 *
 * <p>Immutable, one new instance per {@code accumulate} call -- matches
 * this project's existing record-based aggregate-state convention (see
 * {@code SentimentWindowAggregate} for the same shape on the other
 * stream).
 */
public record PriceWindowAggregate(
        long tickCount, BigDecimal firstPrice, BigDecimal lastPrice, BigDecimal minPrice, BigDecimal maxPrice) {

    public static PriceWindowAggregate empty() {
        return new PriceWindowAggregate(0, null, null, null, null);
    }

    public PriceWindowAggregate accumulate(BigDecimal price) {
        if (tickCount == 0) {
            return new PriceWindowAggregate(1, price, price, price, price);
        }
        BigDecimal newMin = price.compareTo(minPrice) < 0 ? price : minPrice;
        BigDecimal newMax = price.compareTo(maxPrice) > 0 ? price : maxPrice;
        return new PriceWindowAggregate(tickCount + 1, firstPrice, price, newMin, newMax);
    }

    /** Absolute price movement over the window so far: last minus first. */
    public BigDecimal movement() {
        if (firstPrice == null || lastPrice == null) {
            return BigDecimal.ZERO;
        }
        return lastPrice.subtract(firstPrice);
    }

    /** Percent price movement over the window so far, relative to the first price seen. */
    public BigDecimal movementPct() {
        if (firstPrice == null || firstPrice.signum() == 0) {
            return BigDecimal.ZERO;
        }
        return movement().divide(firstPrice, 6, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));
    }
}
