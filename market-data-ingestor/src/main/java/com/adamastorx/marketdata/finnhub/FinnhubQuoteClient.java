package com.adamastorx.marketdata.finnhub;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * Plain GET of Finnhub's real REST {@code /quote} endpoint for one ticker
 * at a time. Same bounded-timeout shape as {@code news-ingestor}'s own
 * {@code RssFeedClient} -- a hung or unreachable Finnhub REST call must
 * fail within a few seconds, not block {@link FinnhubQuotePoller}'s single
 * scheduled thread indefinitely. Throws on any non-2xx response (including
 * a real 429 rate-limit response) or network failure -- {@link
 * FinnhubQuotePoller} catches this per-ticker and logs+skips, matching
 * {@code news-ingestor}'s {@code FeedPoller}/{@code RssFeedClient} "one
 * failure doesn't take down the whole cycle" precedent (see that class's
 * own javadoc).
 */
@Component
public class FinnhubQuoteClient {

    private final RestClient restClient;

    public FinnhubQuoteClient() {
        JdkClientHttpRequestFactory requestFactory =
                new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build());
        requestFactory.setReadTimeout(Duration.ofSeconds(10));
        this.restClient = RestClient.builder().requestFactory(requestFactory).build();
    }

    /**
     * @param quoteUri Finnhub's real REST quote base URI ({@code
     *     app.finnhub-quote-poll.quote-uri})
     * @param ticker the watchlisted symbol, e.g. {@code "AAPL"}
     * @param token the real Finnhub API key -- appended as a query
     *     parameter (Finnhub's own auth mechanism for this endpoint, the
     *     same real credential the websocket path uses). Never logged by
     *     this class -- see {@link FinnhubQuotePoller}'s own catch block
     *     for why callers must be equally careful with any exception this
     *     method throws.
     */
    public FinnhubQuote fetchQuote(String quoteUri, String ticker, String token) {
        return restClient
                .get()
                .uri(quoteUri + "?symbol=" + ticker + "&token=" + token)
                .retrieve()
                .body(FinnhubQuote.class);
    }
}
