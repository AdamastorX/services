package com.adamastorx.newsingestor.matching;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Matches article text against the fixed watchlist via case-insensitive
 * substring matching -- explicitly not NER/NLP (ADR 0021's anti-gold-
 * plating discipline, restated directly in this backlog item's own AC).
 * A ticker matches if the text contains the ticker symbol itself or any
 * of its configured aliases, case-insensitively.
 */
@Component
public class TickerMatcher {

    /** ticker -> lower-cased match terms (the ticker itself + its aliases). */
    private final Map<String, List<String>> matchTermsByTicker;

    public TickerMatcher(WatchlistProperties watchlistProperties) {
        Map<String, List<String>> built = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : watchlistProperties.tickers().entrySet()) {
            String ticker = entry.getKey();
            List<String> terms = new java.util.ArrayList<>();
            terms.add(ticker.toLowerCase());
            for (String alias : entry.getValue()) {
                terms.add(alias.toLowerCase());
            }
            built.put(ticker, terms);
        }
        this.matchTermsByTicker = built;
    }

    /**
     * @param text the article's title + summary, matched as one blob (the
     *     AC doesn't distinguish which field a match came from).
     * @return the watchlist tickers mentioned, in watchlist declaration
     *     order; empty if none matched -- {@code FeedPoller} drops the
     *     article rather than publishing an empty-tickers event.
     */
    public Set<String> match(String text) {
        Set<String> matched = new LinkedHashSet<>();
        if (text == null || text.isBlank()) {
            return matched;
        }
        String lowerText = text.toLowerCase();
        for (Map.Entry<String, List<String>> entry : matchTermsByTicker.entrySet()) {
            for (String term : entry.getValue()) {
                if (lowerText.contains(term)) {
                    matched.add(entry.getKey());
                    break;
                }
            }
        }
        return matched;
    }
}
