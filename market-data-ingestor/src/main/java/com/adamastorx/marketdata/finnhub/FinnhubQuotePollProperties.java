package com.adamastorx.marketdata.finnhub;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Config for the REST-poll fallback ({@link FinnhubQuotePoller}'s own
 * javadoc has the real problem this solves). Deliberately its own {@code
 * @ConfigurationProperties} record, not folded into {@link
 * FinnhubProperties} -- that record documents the *websocket* connection
 * specifically, and its own javadoc is written around that. {@code token}
 * is NOT duplicated here: {@link FinnhubQuotePoller} reads the one real
 * Finnhub API key from {@link FinnhubProperties#token()} (same env var,
 * same real credential the websocket path already uses).
 *
 * <p>The poll interval itself is intentionally NOT a field on this record
 * -- {@code app.finnhub-quote-poll.interval-ms} /
 * {@code app.finnhub-quote-poll.initial-delay-ms} (see {@code
 * application.yml}) are read directly as {@code @Scheduled} property
 * placeholders on {@link FinnhubQuotePoller#pollAllTickers()}, the same
 * shape {@code news-ingestor}'s own {@code FeedPoller} uses for {@code
 * app.poll-interval-ms} -- {@code @Scheduled}'s {@code fixedDelayString}
 * needs a millisecond literal resolved at bean-creation time, not a value
 * read back out of an injected {@code @ConfigurationProperties} bean.
 *
 * @param quoteUri Finnhub's real REST quote endpoint base URI, {@code
 *     https://finnhub.io/api/v1/quote} -- {@code symbol} and {@code token}
 *     are appended as query parameters at call time (mirrors {@link
 *     FinnhubProperties#websocketUri()}'s own "the token is appended at
 *     connect time, never baked into the configured default" shape), see
 *     {@link FinnhubQuoteClient}.
 */
@ConfigurationProperties("app.finnhub-quote-poll")
public record FinnhubQuotePollProperties(String quoteUri) {}
