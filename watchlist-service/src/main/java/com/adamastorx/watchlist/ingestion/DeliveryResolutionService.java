package com.adamastorx.watchlist.ingestion;

import com.adamastorx.watchlist.delivery.DeliveryJpaRepository;
import com.adamastorx.watchlist.observability.WatchlistMetrics;
import com.adamastorx.watchlist.subscription.SubscriptionEntity;
import com.adamastorx.watchlist.subscription.SubscriptionJpaRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * ADR 0026's outbox write. For each changed variant key in the event,
 * resolves every subscription watching it and durably inserts one PENDING
 * delivery row per match, all in one transaction per Kafka message.
 *
 * <p>This is the entire "event consumed" checkpoint: {@link
 * com.adamastorx.watchlist.ingestion.ClinVarIngestionListener} only
 * acknowledges the Kafka offset after this method returns (i.e. after the
 * transaction here has committed). A crash before this commits means Kafka
 * redelivers the message and this runs again -- {@link
 * DeliveryJpaRepository#insertIgnoringConflict} makes that safe (rows
 * already inserted are skipped, not duplicated). A crash after this commits
 * but before the offset is acknowledged has the same outcome: redelivery,
 * no-op re-insert, no lost or duplicated delivery row either way.
 */
@Service
public class DeliveryResolutionService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryResolutionService.class);

    private final SubscriptionJpaRepository subscriptionRepository;
    private final DeliveryJpaRepository deliveryRepository;
    private final WatchlistMetrics metrics;

    public DeliveryResolutionService(
            SubscriptionJpaRepository subscriptionRepository,
            DeliveryJpaRepository deliveryRepository,
            WatchlistMetrics metrics) {
        this.subscriptionRepository = subscriptionRepository;
        this.deliveryRepository = deliveryRepository;
        this.metrics = metrics;
    }

    @Transactional
    public void resolveAndPersist(ClinVarIngestionCompletedEvent event) {
        metrics.fanoutLatency().record(() -> resolve(event));
    }

    private void resolve(ClinVarIngestionCompletedEvent event) {
        List<String> changedKeys = event.changedKeys();
        if (changedKeys == null || changedKeys.isEmpty()) {
            log.info("ClinVar release {}: no changed keys, nothing to fan out", event.newReleaseId());
            return;
        }

        Instant now = Instant.now();
        int insertedRows = 0;
        for (String variantKey : changedKeys) {
            List<SubscriptionEntity> matches = subscriptionRepository.findByVariantKey(variantKey);
            for (SubscriptionEntity subscription : matches) {
                int inserted = deliveryRepository.insertIgnoringConflict(
                        UUID.randomUUID(), subscription.getId(), event.newReleaseId(), variantKey, now);
                insertedRows += inserted;
            }
        }
        log.info(
                "ClinVar release {}: {} changed keys, {} new delivery rows persisted",
                event.newReleaseId(),
                changedKeys.size(),
                insertedRows);
    }
}
