package com.adamastorx.watchlist.delivery;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface DeliveryJpaRepository extends JpaRepository<DeliveryEntity, UUID> {

    /**
     * ON CONFLICT DO NOTHING against the V2 migration's dedupe UNIQUE
     * constraint (subscription_id, release_id, variant_key) -- the idempotency
     * mechanism itself. A real Kafka redelivery of the same message calls this
     * again for the same triple and gets 0 rows affected instead of a duplicate
     * row / duplicate eventual notification.
     */
    @Modifying
    @Query(
            nativeQuery = true,
            value =
                    """
                    INSERT INTO deliveries (id, subscription_id, release_id, variant_key, status, attempts, created_at, updated_at)
                    VALUES (:id, :subscriptionId, :releaseId, :variantKey, 'PENDING', 0, :now, :now)
                    ON CONFLICT (subscription_id, release_id, variant_key) DO NOTHING
                    """)
    int insertIgnoringConflict(
            @Param("id") UUID id,
            @Param("subscriptionId") UUID subscriptionId,
            @Param("releaseId") String releaseId,
            @Param("variantKey") String variantKey,
            @Param("now") Instant now);

    /**
     * Atomically claims one PENDING row for delivery -- returns 1 if this call
     * won the race, 0 if another relay tick (or another pod, briefly possible
     * during a rolling update) already claimed it. NotificationRelay only
     * proceeds to actually call ntfy when this returns 1.
     */
    @Modifying
    @Query("UPDATE DeliveryEntity d SET d.status = com.adamastorx.watchlist.delivery.DeliveryStatus.SENDING, d.updatedAt = :now "
            + "WHERE d.id = :id AND d.status = com.adamastorx.watchlist.delivery.DeliveryStatus.PENDING")
    int claim(@Param("id") UUID id, @Param("now") Instant now);

    List<DeliveryEntity> findByStatusOrderByCreatedAtAsc(DeliveryStatus status, Pageable pageable);

    long countByStatus(DeliveryStatus status);
}
