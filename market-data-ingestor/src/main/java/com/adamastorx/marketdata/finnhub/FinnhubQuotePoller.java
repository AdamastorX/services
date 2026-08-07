package com.adamastorx.marketdata.finnhub;

import com.adamastorx.marketdata.tick.MarketDataProperties;
import com.adamastorx.marketdata.tick.StockPriceTick;
import com.adamastorx.marketdata.tick.StockPriceTickPublisher;
import com.adamastorx.marketdata.tick.TickSource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * The REST-poll fallback for {@code stock.price.tick} -- a second,
 * independent scheduled task alongside {@link FinnhubWebSocketClient}, not
 * a replacement for it.
 *
 * <p><strong>The real problem this solves:</strong> {@code aggregator}
 * (backlog #81) windows {@code stock.price.tick} into 15-minute tumbling
 * aggregates with no history -- if no tick lands in the current window, the
 * ticker shows no data at all. During real US market hours this is fine
 * (the websocket pushes ticks constantly). Outside market hours (most of
 * the day, every evening, every weekend) Finnhub's websocket is connected
 * but silent -- correct, expected behavior, not a bug -- so {@code
 * stock.price.tick} gets nothing at all, and {@code aggregator}'s output
 * (and therefore {@code visualizer}) sits empty most of the time. This
 * class is the project owner's own explicitly-confirmed "reasonable middle
 * ground": keep the real-time websocket as the primary, free, push-based
 * source during actual market hours (untouched by this class), and
 * supplement it with a deliberately low-frequency REST poll so there is
 * always a reasonably fresh real price available even when the market is
 * closed.
 *
 * <p><strong>Why 30 minutes:</strong> the real interval the project owner
 * explicitly confirmed. Frequent enough that {@code aggregator}'s 15-
 * minute windows are never more than two windows away from a real price
 * even at the worst possible poll phase, infrequent enough to be a
 * genuine, deliberate supplement rather than a second real-time feed
 * fighting the websocket for the same job.
 *
 * <p><strong>Rate-limit math (Finnhub free tier: 60 API calls/minute):
 * </strong> one poll cycle makes exactly one REST call per watchlisted
 * ticker -- 5 calls per cycle, every 30 minutes, i.e. 10 calls/hour on
 * average. Even in the worst case (all 5 calls landing in the same
 * wall-clock second, since {@link #pollAllTickers()} loops synchronously
 * with no artificial spacing) that's 5 calls against a 60-calls/minute
 * budget -- under 10% of one minute's allowance, with the other ~29
 * minutes of every cycle spent making zero calls at all. Trivially, and
 * deliberately, nowhere near hammering the API.
 *
 * <p><strong>{@code volume}:</strong> {@link StockPriceTick#volume()} is a
 * non-null {@link BigDecimal} everywhere else in this codebase (every real
 * trade tick over the websocket carries a real trade size, {@link
 * FinnhubTrade#volume()}). Finnhub's REST {@code /quote} endpoint has no
 * per-quote equivalent -- it reports a price snapshot, not a trade. This
 * class publishes {@link BigDecimal#ZERO}, not {@code null}: {@code
 * aggregator} (backlog #81) does not currently do arithmetic over {@code
 * volume} at all (its own consumer-side {@code StockPriceTick} record
 * doesn't even deserialize the field), but {@code null} would make this
 * the first nullable value ever carried by this wire contract, a latent
 * NPE for any future consumer that assumes "a tick always has a volume"
 * the way every real trade tick until now has guaranteed. {@code ZERO} is
 * not literally accurate (no zero-volume trade actually happened) --
 * that's a deliberate, honestly-stated v1 modeling gap, not a claim this
 * was a real trade.
 *
 * <p><strong>Deliberately does NOT feed {@link
 * com.adamastorx.marketdata.observability.StaleFeedMetrics}:</strong> that
 * gauge exists to alert on the websocket going silent *during real market
 * hours* -- a real incident. If this class's ticks counted as "the feed is
 * alive," a genuinely dead websocket during market hours could be masked
 * for up to 30 minutes by this fallback's own poll, defeating the alert's
 * purpose. {@link com.adamastorx.marketdata.observability.StaleFeedMetrics}
 * stays wired to the websocket path only, unchanged by this class.
 */
@Component
public class FinnhubQuotePoller {

    private static final Logger log = LoggerFactory.getLogger(FinnhubQuotePoller.class);

    private final FinnhubProperties finnhubProperties;
    private final FinnhubQuotePollProperties pollProperties;
    private final MarketDataProperties marketDataProperties;
    private final FinnhubQuoteClient quoteClient;
    private final StockPriceTickPublisher publisher;
    private final Counter pollSucceededCounter;
    private final Counter pollFailedCounter;
    private final Counter ticksPublishedCounter;

    public FinnhubQuotePoller(
            FinnhubProperties finnhubProperties,
            FinnhubQuotePollProperties pollProperties,
            MarketDataProperties marketDataProperties,
            FinnhubQuoteClient quoteClient,
            StockPriceTickPublisher publisher,
            MeterRegistry meterRegistry) {
        this.finnhubProperties = finnhubProperties;
        this.pollProperties = pollProperties;
        this.marketDataProperties = marketDataProperties;
        this.quoteClient = quoteClient;
        this.publisher = publisher;
        this.pollSucceededCounter = Counter.builder("market_data_quote_poll_succeeded_total")
                .description("Real Finnhub REST /quote calls (the market-closed fallback path) that returned a "
                        + "usable price for one watchlisted ticker")
                .register(meterRegistry);
        this.pollFailedCounter = Counter.builder("market_data_quote_poll_failed_total")
                .description("Real Finnhub REST /quote calls (the market-closed fallback path) that failed, or "
                        + "returned no usable price, for one watchlisted ticker -- logged and skipped, this poll "
                        + "cycle's turn for that ticker simply produces nothing, the other watchlisted tickers "
                        + "in the same cycle are unaffected")
                .register(meterRegistry);
        this.ticksPublishedCounter = Counter.builder("market_data_quote_ticks_published_total")
                .description("Ticks published to stock.price.tick via the REST-poll fallback path specifically "
                        + "-- a subset of market_data_ticks_published_total, which also counts the real-time "
                        + "websocket path; this one isolates how much of that total came from the fallback")
                .register(meterRegistry);
    }

    /**
     * Runs unconditionally on a fixed 30-minute cadence, regardless of
     * real US market hours -- deliberately simpler than gating this on
     * {@link com.adamastorx.marketdata.observability.MarketHoursService}:
     * the rate-limit math above already holds even if this ran 24/7, and
     * a real, fresh REST price during market hours (when the websocket is
     * already covering the same ticker far more often) is harmless
     * redundancy, not a problem worth extra conditional logic to avoid.
     */
    @Scheduled(
            fixedDelayString = "${app.finnhub-quote-poll.interval-ms:1800000}",
            initialDelayString = "${app.finnhub-quote-poll.initial-delay-ms:60000}")
    public void pollAllTickers() {
        for (String ticker : marketDataProperties.tickers()) {
            pollOneTicker(ticker);
        }
    }

    private void pollOneTicker(String ticker) {
        try {
            FinnhubQuote quote = quoteClient.fetchQuote(pollProperties.quoteUri(), ticker, finnhubProperties.token());
            if (quote == null || quote.currentPrice() == null || quote.currentPrice().compareTo(BigDecimal.ZERO) <= 0) {
                // Finnhub's own documented behavior for a symbol it has no
                // real quote for right now: c:0 (every field 0), not an
                // HTTP error -- treated the same as a failure, never as a
                // real $0.00 price for a large-cap watchlisted ticker.
                log.warn("Finnhub quote poll for {} returned no usable price this cycle, skipping", ticker);
                pollFailedCounter.increment();
                return;
            }
            Instant ingestionTimestamp = Instant.now();
            StockPriceTick tick = new StockPriceTick(
                    ticker,
                    quote.currentPrice(),
                    // No real per-quote volume from this REST endpoint --
                    // see this class's own javadoc for why BigDecimal.ZERO,
                    // not null, was chosen.
                    BigDecimal.ZERO,
                    Instant.ofEpochSecond(quote.epochSeconds()),
                    ingestionTimestamp,
                    TickSource.POLL_FALLBACK);
            publisher.publish(tick);
            pollSucceededCounter.increment();
            ticksPublishedCounter.increment();
        } catch (Exception ex) {
            // Any REST failure (a real HTTP error status RestClient turns
            // into an exception -- including a real 429 rate-limit
            // response -- connect/read timeout, DNS failure, ...) lands
            // here: logged and skipped, matching news-ingestor's
            // FeedPoller/RssFeedClient precedent (see that class's own
            // javadoc). One ticker's failure never blocks the other four
            // in the same cycle, and never escapes this method, so
            // @Scheduled keeps re-invoking pollAllTickers on the next
            // cadence tick rather than the scheduler thread dying.
            //
            // Deliberately logs only the exception's class name, not its
            // full message/toString(): unlike news-ingestor's RSS feed
            // URLs, this request's URI carries the real Finnhub API key as
            // a query parameter (FinnhubQuoteClient's own javadoc), and
            // some JDK/Spring HTTP exception messages embed the request
            // URI -- the same "the token is never logged" discipline
            // FinnhubProperties' own javadoc states for the websocket
            // path, applied here too.
            log.warn(
                    "Finnhub quote poll failed for {} this cycle, skipping: {}",
                    ticker,
                    ex.getClass().getSimpleName());
            pollFailedCounter.increment();
        }
    }
}
