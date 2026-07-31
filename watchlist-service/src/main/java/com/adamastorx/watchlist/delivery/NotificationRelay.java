package com.adamastorx.watchlist.delivery;

import com.adamastorx.watchlist.subscription.SubscriptionEntity;
import com.adamastorx.watchlist.subscription.SubscriptionJpaRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * ADR 0026's relay half of the outbox-table-plus-relay design: an
 * independent poll loop (not triggered by, and not blocked on, the Kafka
 * listener) that reads PENDING delivery rows and actually calls ntfy.
 *
 * <p>This is what makes the crash-mid-delivery AC hold: on restart after a
 * kill, the very next {@code @Scheduled} tick finds every row still
 * PENDING (or SENDING, if the kill landed after {@link #claim} but before
 * the ntfy call -- see the recovery note below) and resumes exactly where
 * it left off, with no dependency on a fresh Kafka message ever arriving
 * again.
 *
 * <p><strong>Known, stated residual gap</strong>: a crash between {@link
 * #claim} succeeding (row now SENDING) and the ntfy call actually
 * completing leaves that one row stuck SENDING forever -- claim() is
 * intentionally one-way (PENDING -&gt; SENDING) so two relay ticks can never
 * race the same row, but nothing currently reclaims a SENDING row whose
 * owner died mid-call. A stuck-SENDING reaper (reclaim rows SENDING longer
 * than N minutes back to PENDING) is real, valuable follow-on work, not
 * built here -- the crash test this item's AC requires targets the much
 * larger and more likely window (kill between "event consumed"/Kafka
 * acked and the relay's next tick even starting), which this design does
 * close completely. Recorded here rather than silently assumed away.
 */
@Component
public class NotificationRelay {

    private static final Logger log = LoggerFactory.getLogger(NotificationRelay.class);

    private final DeliveryJpaRepository deliveryRepository;
    private final SubscriptionJpaRepository subscriptionRepository;
    private final NtfyClient ntfyClient;
    private final com.adamastorx.watchlist.observability.WatchlistMetrics metrics;
    private final int batchSize;
    private final int maxAttempts;

    public NotificationRelay(
            DeliveryJpaRepository deliveryRepository,
            SubscriptionJpaRepository subscriptionRepository,
            NtfyClient ntfyClient,
            com.adamastorx.watchlist.observability.WatchlistMetrics metrics,
            @Value("${app.delivery.batch-size:50}") int batchSize,
            @Value("${app.delivery.max-attempts:5}") int maxAttempts) {
        this.deliveryRepository = deliveryRepository;
        this.subscriptionRepository = subscriptionRepository;
        this.ntfyClient = ntfyClient;
        this.metrics = metrics;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(fixedDelayString = "${app.delivery.relay-interval-ms:2000}")
    public void relayPendingDeliveries() {
        List<DeliveryEntity> batch = deliveryRepository.findByStatusOrderByCreatedAtAsc(
                DeliveryStatus.PENDING, PageRequest.of(0, batchSize));
        for (DeliveryEntity delivery : batch) {
            attemptDelivery(delivery.getId());
        }
    }

    /**
     * One delivery attempt, in its own transaction so one bad row can't roll
     * back the batch and so {@link #claim} is committed (and thus visible to
     * other pods/ticks) before the actual, potentially slow, ntfy HTTP call.
     */
    private void attemptDelivery(UUID deliveryId) {
        if (claim(deliveryId) == 0) {
            // Lost the race (another tick/pod claimed it first) -- normal, not an error.
            return;
        }

        Optional<DeliveryEntity> maybeDelivery = deliveryRepository.findById(deliveryId);
        if (maybeDelivery.isEmpty()) {
            return;
        }
        DeliveryEntity delivery = maybeDelivery.get();
        Optional<SubscriptionEntity> maybeSubscription = subscriptionRepository.findById(delivery.getSubscriptionId());
        if (maybeSubscription.isEmpty()) {
            // Subscription deleted after the delivery row was created -- nothing to
            // notify; dead-letter it so it stops being polled instead of retrying forever.
            deadLetter(delivery, "subscription no longer exists");
            return;
        }
        SubscriptionEntity subscription = maybeSubscription.get();

        String target = subscription.getVariantKey() != null ? subscription.getVariantKey() : subscription.getGeneSymbol();
        String message = "ClinVar release %s: %s classification changed".formatted(delivery.getReleaseId(), target);

        try {
            ntfyClient.send(subscription.getNtfyTopic(), message);
            markSent(delivery);
        } catch (Exception ex) {
            recordFailure(delivery, ex);
        }
    }

    /**
     * Found live (backlog #53's own real-cluster crash test, not a unit test):
     * Spring Data JPA does <em>not</em> automatically wrap a custom
     * {@code @Modifying} query method in a transaction just because it's called
     * from a repository proxy -- unlike the CRUD methods {@code SimpleJpaRepository}
     * itself implements (which are transactional by default), a hand-written
     * {@code @Query}/{@code @Modifying} method throws {@code
     * jakarta.persistence.TransactionRequiredException} if invoked with no active
     * transaction. {@link #attemptDelivery} used to call {@link
     * DeliveryJpaRepository#claim} directly with no transaction wrapping it at
     * all -- worked in this class's own unit-shaped assumptions, broke the first
     * time it ran for real against the live cluster after a genuine pod kill and
     * restart exercised the relay's very first tick. Same fix shape as {@link
     * #markSent}/{@link #recordFailure}/{@link #deadLetter} below, just missing
     * here originally.
     */
    @Transactional
    int claim(UUID deliveryId) {
        return deliveryRepository.claim(deliveryId, Instant.now());
    }

    @Transactional
    void markSent(DeliveryEntity delivery) {
        DeliveryEntity managed = deliveryRepository.findById(delivery.getId()).orElseThrow();
        managed.setStatus(DeliveryStatus.SENT);
        deliveryRepository.save(managed);
        metrics.recordSent();
        metrics.deliveryLatency().record(Duration.between(managed.getCreatedAt(), Instant.now()));
        log.info("Delivered subscription {} release {} variant {}", managed.getSubscriptionId(), managed.getReleaseId(), managed.getVariantKey());
    }

    @Transactional
    void recordFailure(DeliveryEntity delivery, Exception ex) {
        DeliveryEntity managed = deliveryRepository.findById(delivery.getId()).orElseThrow();
        managed.recordAttempt(ex.getMessage());
        if (managed.getAttempts() >= maxAttempts) {
            managed.setStatus(DeliveryStatus.DEAD_LETTERED);
            deliveryRepository.save(managed);
            metrics.recordDeadLettered();
            log.warn(
                    "Dead-lettering delivery {} (subscription {}) after {} attempts: {}",
                    managed.getId(), managed.getSubscriptionId(), managed.getAttempts(), ex.getMessage());
        } else {
            managed.setStatus(DeliveryStatus.PENDING);
            deliveryRepository.save(managed);
            metrics.recordFailed();
            log.warn(
                    "Delivery {} attempt {} failed, retrying: {}", managed.getId(), managed.getAttempts(), ex.getMessage());
        }
    }

    @Transactional
    void deadLetter(DeliveryEntity delivery, String reason) {
        DeliveryEntity managed = deliveryRepository.findById(delivery.getId()).orElseThrow();
        managed.recordAttempt(reason);
        managed.setStatus(DeliveryStatus.DEAD_LETTERED);
        deliveryRepository.save(managed);
        metrics.recordDeadLettered();
    }
}
