package com.adamastorx.newsingestor.feed;

import java.time.Instant;

/**
 * One {@code <item>} parsed out of an RSS 2.0 feed. {@code guid} is
 * preferred as the dedup/identity key ({@link
 * com.adamastorx.newsingestor.publishing.ArticleDedupService}); both WSJ
 * Markets and MarketWatch emit a real, stable {@code <guid isPermaLink="false">}
 * on every item (confirmed against a live fetch of both feeds), so {@code
 * link} is kept only as a fallback for a feed that ever omits one, not the
 * primary key.
 */
public record RssArticle(String guid, String link, String title, String summary, Instant publishedAt, String source) {

    /** {@link #guid}, falling back to {@link #link} if a feed ever omits a guid. */
    public String dedupKey() {
        return (guid != null && !guid.isBlank()) ? guid : link;
    }
}
