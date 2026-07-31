package com.adamastorx.watchlist.observability;

import com.adamastorx.watchlist.delivery.DeliveryJpaRepository;
import com.adamastorx.watchlist.delivery.DeliveryStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Backlog #53's own metric surface AC: fan-out latency, delivery attempts,
 * DLQ depth. Same convention as clinvar-service's {@code app/metrics.py}
 * module-level registration -- one place, constructed once, named after
 * what the AC literally asks for rather than generic names.
 */
@Component
public class WatchlistMetrics {

    /** Time from receiving the Kafka event to durably persisting every matching
     * subscription's PENDING delivery row (the "event consumed" checkpoint). */
    private final Timer fanoutLatency;

    /** Time from a delivery row's creation to it being marked SENT -- the
     * end-to-end subscriber-facing latency the dashboard/SLO care about,
     * distinct from fanoutLatency's narrower "was it durably recorded" window. */
    private final Timer deliveryLatency;

    private final Counter attemptsSent;
    private final Counter attemptsFailed;
    private final Counter attemptsDeadLettered;

    public WatchlistMetrics(MeterRegistry meterRegistry, DeliveryJpaRepository deliveryRepository) {
        this.fanoutLatency = Timer.builder("watchlist_fanout_latency_seconds")
                .description("Time from Kafka event received to all matching delivery rows durably persisted")
                .register(meterRegistry);
        this.deliveryLatency = Timer.builder("watchlist_delivery_latency_seconds")
                .description("Time from a delivery row's creation to it being marked SENT")
                .register(meterRegistry);
        this.attemptsSent = Counter.builder("watchlist_delivery_attempts_total")
                .tag("outcome", "sent")
                .register(meterRegistry);
        this.attemptsFailed = Counter.builder("watchlist_delivery_attempts_total")
                .tag("outcome", "failed")
                .register(meterRegistry);
        this.attemptsDeadLettered = Counter.builder("watchlist_delivery_attempts_total")
                .tag("outcome", "dead_lettered")
                .register(meterRegistry);
        // DLQ depth: a real-time gauge sampled from the deliveries table on
        // every scrape, not a counter that only ever goes up -- the AC asks
        // for "DLQ depth", i.e. how many are dead-lettered *right now*, which
        // is what the alert/dashboard actually need to page or drain against.
        meterRegistry.gauge(
                "watchlist_delivery_dlq_depth",
                deliveryRepository,
                repo -> repo.countByStatus(DeliveryStatus.DEAD_LETTERED));
    }

    public Timer fanoutLatency() {
        return fanoutLatency;
    }

    public Timer deliveryLatency() {
        return deliveryLatency;
    }

    public void recordSent() {
        attemptsSent.increment();
    }

    public void recordFailed() {
        attemptsFailed.increment();
    }

    public void recordDeadLettered() {
        attemptsDeadLettered.increment();
    }
}
