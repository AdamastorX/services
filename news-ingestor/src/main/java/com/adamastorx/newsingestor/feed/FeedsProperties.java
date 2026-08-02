package com.adamastorx.newsingestor.feed;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * The fixed set of RSS feeds polled every cycle -- WSJ Markets +
 * MarketWatch top stories (ADR 0029's chosen sources, both real HTTP 200
 * with live content, re-verified live this session, not assumed from the
 * ADR). Configured (not hardcoded) so a fallback source (Seeking Alpha /
 * investing.com, also verified live in ADR 0029 but not built into v1)
 * can be added via config alone if WSJ/MarketWatch ever go the way of
 * CNBC/Reuters.
 */
@ConfigurationProperties(prefix = "app")
public record FeedsProperties(List<Feed> feeds) {

    public record Feed(String name, String url) {}
}
