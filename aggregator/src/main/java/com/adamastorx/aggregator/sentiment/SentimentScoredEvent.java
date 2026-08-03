package com.adamastorx.aggregator.sentiment;

/**
 * This service's own consumer-side view of {@code news.sentiment.scored}'s
 * wire shape (backlog #80's real producer:
 * {@code sentiment-analyzer/app/events.py}'s {@code SentimentScoredEvent},
 * one JSON object per (article, ticker) pair). The real payload also
 * carries {@code headline, source, articlePublishedAt, link, guid,
 * scoredAt}; only {@code ticker} and {@code score} are deserialized here --
 * the rolling-average-sentiment aggregate this service computes needs
 * nothing else, and {@code articlePublishedAt}/{@code scoredAt} are exactly
 * the two fields {@code sentiment-analyzer}'s own README/events.py docstring
 * flags as never independently re-verified against a live producer (one is
 * an opaque pass-through of a Java {@code Instant}'s still-not-fully-nailed-
 * down encoding, the other a Python {@code isoformat()} string) --
 * sidestepped entirely by not needing them, not by guessing a parse format.
 * This topology windows on Kafka's own record timestamp instead (see
 * {@code AggregatorTopology}'s javadoc).
 *
 * <p>Not a shared Java type with the Python producer (there is no Java
 * type to share here anyway -- ADR 0007's "agree on the JSON shape, not a
 * shared Java type" convention). {@link
 * com.fasterxml.jackson.annotation.JsonIgnoreProperties} tolerates the
 * extra fields the real payload carries.
 *
 * <p>{@code score} is VADER's compound score, -1..+1 (sentiment-analyzer's
 * own {@code app/scoring.py}), a plain {@code double} -- matches the
 * producer's own {@code score: float} JSON encoding exactly, no BigDecimal
 * precision concern the way a money-shaped value would have.
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
public record SentimentScoredEvent(String ticker, double score) {}
