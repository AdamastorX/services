package com.adamastorx.newsingestor.matching;

import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The fixed watchlist: ticker symbol -&gt; a small, stated list of aliases
 * (company-name synonyms, e.g. {@code AAPL -> [Apple, Apple Inc]}) that
 * also count as a match, per the AC's "ticker symbols plus a small stated
 * alias list". The ticker symbol itself is always an implicit alias of
 * itself ({@link TickerMatcher} adds it) -- no need to repeat "AAPL" in
 * its own alias list.
 *
 * <p>Default set is the same 5 large-cap tickers backlog #78
 * (market-data-ingestor) uses (AAPL/MSFT/GOOGL/AMZN/TSLA, per that
 * backlog item's own suggested default) -- #78 had not yet landed a
 * concrete list in the services repo at the time this was built (no
 * market-data-ingestor PR existed), so this is the same reasonable
 * default the backlog item itself names, not a guess; coordinate/reconcile
 * if #78 lands a different final list.
 */
@ConfigurationProperties(prefix = "app.watchlist")
public record WatchlistProperties(Map<String, List<String>> tickers) {}
