package com.adamastorx.aggregator.api;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * {@code GET /aggregates/{ticker}}'s response shape (backlog #81's AC: "a
 * small, plain REST query API; no gold-plated streaming/GraphQL API for
 * v1", ADR 0021) -- no history, no pagination, matching {@code api}'s own
 * simplest lookup endpoints ({@code VariantLookupController}).
 *
 * <p><b>This is a query-time correlation, not a topology-level join</b>
 * (see {@code AggregatorTopology}'s own javadoc for why): {@code
 * sentimentSampleCount}/{@code avgSentiment} are {@code null} when no
 * sentiment data exists for this ticker at all (not just "not in the
 * current window" -- see below), a real, expected, common state (news is
 * sparse), not an error. A consumer (backlog #82's {@code visualizer})
 * correlates the two by reading both fields together for the same ticker,
 * which is the entire "correlated against" requirement this v1 satisfies
 * -- a real Pearson-style correlation coefficient across many windows is
 * out of scope (would need a history of past windows this store doesn't
 * retain, real added scope with no stated v1 need, ADR 0021).
 *
 * <p><b>{@code windowStart}/{@code windowEnd} vs {@code priceAsOf}/{@code
 * sentimentAsOf} -- found live 2026-08-05, real design change, not
 * cosmetic.</b> {@code windowStart}/{@code windowEnd} remain the
 * *current* 15-minute tumbling window's own boundaries (kept, unchanged
 * shape, for {@code visualizer}'s existing "window HH:MM-HH:MM" display)
 * -- but they no longer reliably describe when the price/sentiment shown
 * here was actually observed. Real trade ticks only flow during US market
 * hours and real news is naturally sparse, so most of the time (including
 * whenever a human opens {@code visualizer}) the current window has no
 * data for a ticker at all; {@code api.AggregateQueryService} now falls
 * back independently, per field, to each ticker's most recently known
 * price/sentiment (see {@code AggregatorTopology}'s "latest known state"
 * KTables) rather than returning nothing. {@code priceAsOf}/{@code
 * sentimentAsOf} are the honest signal a consumer must read to know how
 * stale that fallback data actually is -- "when this data was actually
 * last seen," Kafka's own real record timestamp (see {@code
 * AggregatorTopology}'s "Windowing on Kafka's own record timestamp"),
 * never fabricated as "now" just because a value is present. {@code
 * priceAsOf} is non-null whenever this response exists at all (price
 * presence is the "does this ticker have any data at all" gate --
 * unchanged from before, see {@code AggregateQueryService}'s own
 * javadoc); {@code sentimentAsOf} is null exactly when {@code
 * sentimentSampleCount}/{@code avgSentiment} are null (no sentiment ever
 * seen for this ticker), independent of {@code priceAsOf} -- a ticker's
 * price and sentiment can each be fresh or stale on their own.
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
        Instant priceAsOf,
        Long sentimentSampleCount,
        Double avgSentiment,
        Instant sentimentAsOf) {}
