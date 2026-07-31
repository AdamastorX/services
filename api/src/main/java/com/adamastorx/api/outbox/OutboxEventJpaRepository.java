package com.adamastorx.api.outbox;

import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventJpaRepository extends JpaRepository<OutboxEventEntity, UUID> {

    java.util.List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(String status, Pageable pageable);

    /** Marks PUBLISHED only if still PENDING -- returns 0 if another relay tick
     * already marked it (a possible double-publish on a rare race between two
     * relay ticks or two pods during a rolling update; acceptable here, unlike
     * watchlist-service's stricter per-subscriber dedupe, because work-items'
     * consumer already tolerates at-least-once, same as before this change). */
    @Modifying
    @Query("UPDATE OutboxEventEntity o SET o.status = 'PUBLISHED', o.publishedAt = :now "
            + "WHERE o.id = :id AND o.status = 'PENDING'")
    int markPublished(@Param("id") UUID id, @Param("now") Instant now);
}
