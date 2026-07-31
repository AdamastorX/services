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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
 * <p><strong>Found live, twice, via backlog #53's own real-cluster crash
 * test (not a unit test)</strong>: this class's every write to {@link
 * DeliveryJpaRepository} is done through a {@link TransactionTemplate}
 * (programmatic transactions), not {@code @Transactional} on private
 * helper methods of this same class. The first live attempt used {@code
 * @Transactional} on {@code claim()}/{@code markSent()}/etc. and it
 * compiled and looked correct, but failed the instant a genuine pod
 * restart exercised the relay's very first tick against the live
 * cluster: {@code attemptDelivery} called {@code claim(deliveryId)} as a
 * plain internal method call on {@code this} -- classic Spring AOP
 * self-invocation, which bypasses the CGLIB proxy {@code @Transactional}
 * relies on entirely, so no transaction was ever actually opened despite
 * the annotation being present and correct-looking. {@link
 * TransactionTemplate} is a direct programmatic API, not proxy-based, so
 * it has no self-invocation hole to fall into. Recorded here because this
 * is exactly the kind of bug that a written-but-never-executed unit test
 * would not have caught either (a mocked repository never exercises real
 * transaction demarcation) -- only running it for real, against a real
 * Postgres, after a real restart, found it.
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
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;
    private final int maxAttempts;

    public NotificationRelay(
            DeliveryJpaRepository deliveryRepository,
            SubscriptionJpaRepository subscriptionRepository,
            NtfyClient ntfyClient,
            com.adamastorx.watchlist.observability.WatchlistMetrics metrics,
            PlatformTransactionManager transactionManager,
            @Value("${app.delivery.batch-size:50}") int batchSize,
            @Value("${app.delivery.max-attempts:5}") int maxAttempts) {
        this.deliveryRepository = deliveryRepository;
        this.subscriptionRepository = subscriptionRepository;
        this.ntfyClient = ntfyClient;
        this.metrics = metrics;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
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
     * One delivery attempt. {@link #claim} runs and commits in its own
     * transaction before the actual, potentially slow, ntfy HTTP call --
     * so a claimed row is visible to other pods/ticks immediately, and a
     * bad row's eventual failure handling is a second, separate
     * transaction rather than rolling back the claim itself.
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

    int claim(UUID deliveryId) {
        return transactionTemplate.execute(status -> deliveryRepository.claim(deliveryId, Instant.now()));
    }

    void markSent(DeliveryEntity delivery) {
        transactionTemplate.executeWithoutResult(status -> {
            DeliveryEntity managed = deliveryRepository.findById(delivery.getId()).orElseThrow();
            managed.setStatus(DeliveryStatus.SENT);
            deliveryRepository.save(managed);
            metrics.recordSent();
            metrics.deliveryLatency().record(Duration.between(managed.getCreatedAt(), Instant.now()));
            log.info(
                    "Delivered subscription {} release {} variant {}",
                    managed.getSubscriptionId(), managed.getReleaseId(), managed.getVariantKey());
        });
    }

    void recordFailure(DeliveryEntity delivery, Exception ex) {
        transactionTemplate.executeWithoutResult(status -> {
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
                        "Delivery {} attempt {} failed, retrying: {}",
                        managed.getId(), managed.getAttempts(), ex.getMessage());
            }
        });
    }

    void deadLetter(DeliveryEntity delivery, String reason) {
        transactionTemplate.executeWithoutResult(status -> {
            DeliveryEntity managed = deliveryRepository.findById(delivery.getId()).orElseThrow();
            managed.recordAttempt(reason);
            managed.setStatus(DeliveryStatus.DEAD_LETTERED);
            deliveryRepository.save(managed);
            metrics.recordDeadLettered();
        });
    }
}
