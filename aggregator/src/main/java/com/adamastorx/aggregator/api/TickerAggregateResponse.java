package com.adamastorx.aggregator.api;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * {@code GET /aggregates/{ticker}}'s response shape (backlog #81's AC: "a
 * small, plain REST query API; no gold-plated streaming/GraphQL API for
 * v1", ADR 0021). One JSON object, current tumbling window only -- no
 * history, no pagination, matching {@code api}'s own simplest lookup
 * endpoints ({@code VariantLookupController}).
 *
 * <p><b>This is a query-time correlation, not a topology-level join</b>
 * (see {@code AggregatorTopology}'s own javadoc for why): {@code
 * sentimentSampleCount}/{@code avgSentiment} are {@code null} when no
 * {@code news.sentiment.scored} event landed for this ticker in the
 * current window -- a real, expected, common state (news is sparse,
 * price ticks are continuous), not an error. A consumer (backlog #82's
 * {@code visualizer}) correlates the two by reading both fields together
 * for the same ticker/window, which is the entire "correlated against"
 * requirement this v1 satisfies -- a real Pearson-style correlation
 * coefficient across many windows is out of scope (would need a history
 * of past windows this store doesn't retain beyond the current one, real
 * added scope with no stated v1 need, ADR 0021).
 */
public record TickerAggregateResponse(
        String ticker,
        Instant windowStart,
        Instant windowEnd,
        long tickCount,
        BigDecimal firstPrice,
        BigDecimal lastPrice,
        BigDecimal priceMovement,
        BigDecimal priceMovementPct,
        Long sentimentSampleCount,
        Double avgSentiment) {}
