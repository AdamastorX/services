package com.adamastorx.watchlist;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Backlog #53. {@code @EnableScheduling} drives {@link
 * com.adamastorx.watchlist.delivery.NotificationRelay}'s poll loop -- the
 * half of the outbox-plus-relay design (ADR 0026) that must keep running
 * independently of Kafka delivery, so a crash between "event consumed" and
 * "notification sent" self-heals on the next tick after restart rather than
 * needing a fresh Kafka message to retrigger it.
 */
@SpringBootApplication
@EnableScheduling
public class WatchlistServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(WatchlistServiceApplication.class, args);
    }
}
