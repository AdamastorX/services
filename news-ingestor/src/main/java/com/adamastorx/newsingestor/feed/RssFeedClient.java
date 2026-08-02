package com.adamastorx.newsingestor.feed;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Plain GET of an RSS feed URL, returning the raw XML body. Same bounded-
 * timeout shape as {@code watchlist-service}'s {@code NtfyClient} -- a
 * hung or unreachable feed must fail the poll attempt within a few
 * seconds, not block {@link com.adamastorx.newsingestor.feed.FeedPoller}'s
 * single scheduled thread indefinitely (the exact "feed-unreachable" path
 * ADR 0029 records CNBC/Reuters/Yahoo actually hitting live: 403/301/429).
 * Throws on any non-2xx response or network failure -- {@code FeedPoller}
 * catches this per-feed and logs+skips, it never propagates out of one
 * poll tick.
 */
@Component
public class RssFeedClient {

    private final RestClient restClient;

    public RssFeedClient() {
        JdkClientHttpRequestFactory requestFactory =
                new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build());
        requestFactory.setReadTimeout(Duration.ofSeconds(10));
        this.restClient = RestClient.builder().requestFactory(requestFactory).build();
    }

    public String fetch(String url) {
        return restClient.get().uri(url).retrieve().body(String.class);
    }
}
