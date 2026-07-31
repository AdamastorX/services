package com.adamastorx.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * {@code @EnableScheduling} drives {@link
 * com.adamastorx.api.outbox.OutboxRelay}'s poll loop (backlog #16, ADR
 * 0026) -- found live via CI, not locally: without it, {@code @Scheduled}
 * is silently a no-op (no error, no log line), which is exactly the kind
 * of gap a test asserting "the row is never lost" can't catch on its own
 * (a row that's never even attempted also never gets lost) -- only
 * {@code WorkItemControllerIntegrationTest}'s "does the message actually
 * arrive on the topic" assertion caught this, by timing out. Same
 * annotation watchlist-service's own {@code WatchlistServiceApplication}
 * already carries for {@code NotificationRelay}.
 */
@SpringBootApplication
@EnableScheduling
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }
}
