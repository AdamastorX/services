package com.adamastorx.newsingestor.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Real counters for the poll loop's outcomes, same "a scrape of
 * /actuator/prometheus after this change" verification discipline
 * observability#15 already established for the other services --
 * critically, {@code feed_unreachable_total} is what makes the AC's
 * "logged and skipped, not a crash loop" a real, observable metric rather
 * than only a log line no one is watching.
 */
@Component
public class NewsIngestorMetrics {

    private final Counter feedPollSucceeded;
    private final Counter feedUnreachable;
    private final Counter articlesMatched;
    private final Counter articlesPublished;
    private final Counter articlesDroppedNoMatch;
    private final Counter articlesSkippedDuplicate;

    public NewsIngestorMetrics(MeterRegistry meterRegistry) {
        this.feedPollSucceeded = Counter.builder("news_ingestor_feed_poll_total")
                .tag("outcome", "succeeded")
                .description("Feed poll cycles that completed without a fetch/parse failure")
                .register(meterRegistry);
        this.feedUnreachable = Counter.builder("news_ingestor_feed_poll_total")
                .tag("outcome", "unreachable")
                .description("Feed poll cycles that failed (fetch or parse) and were skipped")
                .register(meterRegistry);
        this.articlesMatched = Counter.builder("news_ingestor_articles_total")
                .tag("outcome", "matched")
                .description("New articles matching at least one watchlist ticker")
                .register(meterRegistry);
        this.articlesPublished = Counter.builder("news_ingestor_articles_total")
                .tag("outcome", "published")
                .description("Matched articles successfully published to news.article.published")
                .register(meterRegistry);
        this.articlesDroppedNoMatch = Counter.builder("news_ingestor_articles_total")
                .tag("outcome", "dropped_no_match")
                .description("New articles seen but matching no watchlist ticker, not forwarded")
                .register(meterRegistry);
        this.articlesSkippedDuplicate = Counter.builder("news_ingestor_articles_total")
                .tag("outcome", "skipped_duplicate")
                .description("Articles already seen in a prior poll (by guid/link), skipped")
                .register(meterRegistry);
    }

    public void recordFeedPollSucceeded() {
        feedPollSucceeded.increment();
    }

    public void recordFeedUnreachable() {
        feedUnreachable.increment();
    }

    public void recordArticleMatched() {
        articlesMatched.increment();
    }

    public void recordArticlePublished() {
        articlesPublished.increment();
    }

    public void recordArticleDroppedNoMatch() {
        articlesDroppedNoMatch.increment();
    }

    public void recordArticleSkippedDuplicate() {
        articlesSkippedDuplicate.increment();
    }
}
