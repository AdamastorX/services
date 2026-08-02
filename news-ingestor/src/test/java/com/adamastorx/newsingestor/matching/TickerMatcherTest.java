package com.adamastorx.newsingestor.matching;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TickerMatcherTest {

    private final TickerMatcher matcher = new TickerMatcher(new WatchlistProperties(Map.of(
            "AAPL", List.of("Apple", "Apple Inc"),
            "MSFT", List.of("Microsoft"),
            "AMZN", List.of("Amazon"))));

    @Test
    void matchesOnAliasCaseInsensitively() {
        Set<String> matched = matcher.match("apple FALLS, but AMAZON push nasdaq higher");

        assertThat(matched).containsExactlyInAnyOrder("AAPL", "AMZN");
    }

    @Test
    void matchesOnTickerSymbolItselfEvenWithoutAliasHit() {
        Set<String> matched = matcher.match("MSFT posts biggest one-day gain");

        assertThat(matched).containsExactly("MSFT");
    }

    @Test
    void nonMatchingArticleProducesNoMatches() {
        Set<String> matched = matcher.match("Local weather roundup for the weekend");

        assertThat(matched).isEmpty();
    }

    @Test
    void blankTextProducesNoMatches() {
        assertThat(matcher.match("")).isEmpty();
        assertThat(matcher.match(null)).isEmpty();
    }
}
