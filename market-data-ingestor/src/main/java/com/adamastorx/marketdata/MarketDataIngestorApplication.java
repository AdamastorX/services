package com.adamastorx.marketdata;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling}: required by {@link
 * com.adamastorx.marketdata.finnhub.FinnhubQuotePoller}'s {@code
 * @Scheduled} REST-poll-fallback method (backlog #78's live/websocket path
 * itself schedules its own reconnect/watchdog work directly on a hand-
 * rolled {@code ScheduledExecutorService}, {@link
 * com.adamastorx.marketdata.finnhub.FinnhubWebSocketClient}, so this
 * annotation was never needed here until now). Same annotation {@code
 * news-ingestor}'s own {@code NewsIngestorApplication} already carries for
 * its own {@code @Scheduled} {@code FeedPoller}.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class MarketDataIngestorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MarketDataIngestorApplication.class, args);
    }

    /**
     * The one real clock every real deployment uses -- {@link
     * com.adamastorx.marketdata.observability.StaleFeedMetrics}'s own
     * javadoc explains why it takes this as a bean rather than calling
     * {@code Instant.now()} directly: it's the test seam that lets
     * staleness/market-hours logic be asserted against a fixed "now" in
     * {@code StaleFeedMetricsTest}, without a second implementation to
     * drift from this one.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
