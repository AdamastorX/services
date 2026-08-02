package com.adamastorx.newsingestor.publishing;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Bounded, in-memory LRU of article dedup keys (GUID, falling back to
 * link) already seen -- the AC's "a re-poll of an unchanged feed never
 * republishes the same article". Deliberately in-memory, not a Postgres
 * table: this service is stateless by design (no dedicated DB instance,
 * see {@code pom.xml}'s comment and the news-ingestor README) -- the
 * accepted v1 gap is that a pod restart forgets recently-seen articles,
 * so the first poll after a restart could re-publish a handful of
 * articles still present in the feed's current window (WSJ/MarketWatch
 * both keep dozens of recent items per feed). Bounded at {@link
 * #maxEntries} so this can never grow unbounded across a long-running
 * pod's lifetime; oldest entries evict first (insertion order), which is
 * safe because both feeds' current-item windows are far smaller than the
 * default cap.
 */
@Component
public class ArticleDedupService {

    private final int maxEntries;
    private final Map<String, Boolean> seen;

    public ArticleDedupService(@Value("${app.dedup.max-entries:2000}") int maxEntries) {
        this.maxEntries = maxEntries;
        this.seen = new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > ArticleDedupService.this.maxEntries;
            }
        };
    }

    /**
     * @return {@code true} if {@code dedupKey} is genuinely new (and is
     *     now marked seen); {@code false} if it was already seen (already
     *     marked, this call is a no-op).
     */
    public synchronized boolean markIfNew(String dedupKey) {
        if (dedupKey == null || dedupKey.isBlank()) {
            // No usable identity at all -- treat as always-new rather than
            // silently dropping every such article as a permanent
            // "duplicate" of itself. Neither chosen feed has ever been
            // observed omitting both guid and link (see RssFeedParser),
            // this is a defensive fallback only.
            return true;
        }
        if (seen.containsKey(dedupKey)) {
            return false;
        }
        seen.put(dedupKey, Boolean.TRUE);
        return true;
    }
}
