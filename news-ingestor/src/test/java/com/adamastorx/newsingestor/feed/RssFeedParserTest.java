package com.adamastorx.newsingestor.feed;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class RssFeedParserTest {

    private final RssFeedParser parser = new RssFeedParser();

    private static final String SAMPLE_RSS =
            """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
            <channel>
            <title>Sample Feed</title>
            <item>
            <guid isPermaLink="false">WP-TEST-0001</guid>
            <title>Apple Falls, But Amazon Push Nasdaq Higher</title>
            <description>A real market movement story.</description>
            <link>https://example.com/story/1</link>
            <pubDate>Sun, 02 Aug 2026 18:50:26 GMT</pubDate>
            </item>
            <item>
            <guid isPermaLink="false">WP-TEST-0002</guid>
            <title>Local Weather Roundup</title>
            <description>Nothing about tickers here.</description>
            <link>https://example.com/story/2</link>
            <pubDate>Sun, 02 Aug 2026 17:00:00 GMT</pubDate>
            </item>
            </channel>
            </rss>
            """;

    @Test
    void parsesAllItemFieldsFromRealFeedShape() {
        List<RssArticle> articles = parser.parse(SAMPLE_RSS, "test-source");

        assertThat(articles).hasSize(2);
        RssArticle first = articles.get(0);
        assertThat(first.guid()).isEqualTo("WP-TEST-0001");
        assertThat(first.title()).isEqualTo("Apple Falls, But Amazon Push Nasdaq Higher");
        assertThat(first.summary()).isEqualTo("A real market movement story.");
        assertThat(first.link()).isEqualTo("https://example.com/story/1");
        assertThat(first.source()).isEqualTo("test-source");
        assertThat(first.publishedAt()).isEqualTo(Instant.parse("2026-08-02T18:50:26Z"));
    }

    @Test
    void malformedXmlIsLoggedAndSkippedNotThrown() {
        List<RssArticle> articles = parser.parse("<rss><channel><item><title>unterminated", "broken-source");

        assertThat(articles).isNotNull();
    }

    @Test
    void unparsablePubDateFallsBackToNowRatherThanDroppingTheArticle() {
        String rssWithBadDate =
                """
                <rss version="2.0"><channel>
                <item>
                <guid isPermaLink="false">WP-TEST-0003</guid>
                <title>Some headline</title>
                <description>Some summary</description>
                <link>https://example.com/story/3</link>
                <pubDate>not-a-real-date</pubDate>
                </item>
                </channel></rss>
                """;

        List<RssArticle> articles = parser.parse(rssWithBadDate, "test-source");

        assertThat(articles).hasSize(1);
        assertThat(articles.get(0).publishedAt()).isCloseTo(Instant.now(), org.assertj.core.api.Assertions.within(java.time.Duration.ofSeconds(10)));
    }
}
