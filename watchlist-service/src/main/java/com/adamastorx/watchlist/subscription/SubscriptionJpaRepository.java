package com.adamastorx.watchlist.subscription;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SubscriptionJpaRepository extends JpaRepository<SubscriptionEntity, UUID> {

    /** DeliveryResolutionService's fan-out lookup: every subscription watching this exact variant key. */
    List<SubscriptionEntity> findByVariantKey(String variantKey);
}
