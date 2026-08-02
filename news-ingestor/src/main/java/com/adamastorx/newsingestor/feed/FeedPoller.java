package com.adamastorx.newsingestor.feed;

import com.adamastorx.newsingestor.matching.TickerMatcher;
import com.adamastorx.newsingestor.observability.NewsIngestorMetrics;
import com.adamastorx.newsingestor.publishing.ArticleDedupService;
import com.adamastorx.newsingestor.publishing.ArticlePublishedEvent;
import com.adamastorx.newsingestor.publishing.ArticlePublisher;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The AC's main loop: on a fixed cadence, poll each configured feed,
 * extract new articles, match against the watchlist, dedupe, publish
 * matches only.
 *
 * <p><strong>Feed-unreachable handling (the AC's explicit requirement):</strong>
 * each feed's fetch+parse runs inside its own {@code try/catch} in {@link
 * #pollAllFeeds()} -- one feed's failure (a real HTTP error, a connect
 * timeout, a malformed body) is logged, counted ({@code
 * NewsIngestorMetrics#recordFeedUnreachable}), and skipped; it never
 * throws out of this method, so {@code @Scheduled} keeps re-invoking it
 * on the next cadence tick rather than the scheduler thread dying after
 * one bad poll (a real, tested behavior -- see {@code
 * FeedPollerLiveIntegrationTest}, not asserted from the code shape alone).
 */
@Component
public class FeedPoller {

    private static final Logger log = LoggerFactory.getLogger(FeedPoller.class);

    private final FeedsProperties feedsProperties;
    private final RssFeedClient feedClient;
    private final RssFeedParser feedParser;
    private final TickerMatcher tickerMatcher;
    private final ArticleDedupService dedupService;
    private final ArticlePublisher articlePublisher;
    private final NewsIngestorMetrics metrics;

    public FeedPoller(
            FeedsProperties feedsProperties,
            RssFeedClient feedClient,
            RssFeedParser feedParser,
            TickerMatcher tickerMatcher,
            ArticleDedupService dedupService,
            ArticlePublisher articlePublisher,
            NewsIngestorMetrics metrics) {
        this.feedsProperties = feedsProperties;
        this.feedClient = feedClient;
        this.feedParser = feedParser;
        this.tickerMatcher = tickerMatcher;
        this.dedupService = dedupService;
        this.articlePublisher = articlePublisher;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${app.poll-interval-ms:300000}", initialDelayString = "${app.poll-initial-delay-ms:0}")
    public void pollAllFeeds() {
        List<FeedsProperties.Feed> feeds = feedsProperties.feeds();
        if (feeds == null) {
            return;
        }
        for (FeedsProperties.Feed feed : feeds) {
            pollOneFeed(feed);
        }
    }

    private void pollOneFeed(FeedsProperties.Feed feed) {
        try {
            String xml = feedClient.fetch(feed.url());
            List<RssArticle> articles = feedParser.parse(xml, feed.name());
            for (RssArticle article : articles) {
                processArticle(article);
            }
            metrics.recordFeedPollSucceeded();
        } catch (Exception ex) {
            // Any fetch failure (real HTTP status the RestClient turns into
            // an exception, connect/read timeout, DNS failure, ...) lands
            // here -- logged and skipped, this poll cycle's turn for this
            // feed simply produces nothing, the next scheduled tick tries
            // again. No exception escapes this method.
            log.warn("Feed '{}' ({}) unreachable this poll cycle, skipping: {}", feed.name(), feed.url(), ex.getMessage());
            metrics.recordFeedUnreachable();
        }
    }

    private void processArticle(RssArticle article) {
        if (!dedupService.markIfNew(article.dedupKey())) {
            metrics.recordArticleSkippedDuplicate();
            return;
        }

        Set<String> matchedTickers = tickerMatcher.match(article.title() + " " + article.summary());
        if (matchedTickers.isEmpty()) {
            // Explicit design choice (AC): a non-matching article is
            // dropped, not forwarded.
            metrics.recordArticleDroppedNoMatch();
            return;
        }
        metrics.recordArticleMatched();

        ArticlePublishedEvent event = new ArticlePublishedEvent(
                List.copyOf(matchedTickers),
                article.title(),
                article.source(),
                article.publishedAt(),
                article.link(),
                article.guid());

        if (articlePublisher.publish(event)) {
            metrics.recordArticlePublished();
        }
    }
}
