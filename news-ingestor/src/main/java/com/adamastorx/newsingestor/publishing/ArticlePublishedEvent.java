package com.adamastorx.newsingestor.publishing;

import java.time.Instant;
import java.util.List;

/**
 * Wire shape of {@code news.article.published}. Published only for
 * articles matching at least one watchlist ticker -- a non-matching
 * article is dropped, not forwarded (this backlog item's own stated
 * design choice bounding the topic to real signal).
 *
 * @param tickers matched watchlist tickers, non-empty (an event is never
 *     published for zero matches)
 * @param headline the article's title
 * @param source which feed this came from ("wsj-markets" / "marketwatch")
 * @param publishedAt the article's own {@code pubDate}, not ingestion time
 * @param link the article's canonical URL
 * @param guid the feed's own item identity (kept alongside {@code link}
 *     as a stable article reference for downstream consumers, e.g. #80's
 *     per-(article,ticker) sentiment event -- cheap to carry, already
 *     computed for dedup, not gold-plating)
 */
public record ArticlePublishedEvent(
        List<String> tickers, String headline, String source, Instant publishedAt, String link, String guid) {}
