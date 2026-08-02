package com.adamastorx.newsingestor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Backlog #79 (M13, ADR 0029): polls WSJ Markets + MarketWatch RSS, matches
 * against a fixed watchlist, publishes {@code news.article.published}. See
 * {@code news-ingestor/README.md} for the full design.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class NewsIngestorApplication {

    public static void main(String[] args) {
        SpringApplication.run(NewsIngestorApplication.class, args);
    }
}
