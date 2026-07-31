package com.adamastorx.watchlist.subscription;

import java.util.UUID;

/** REST request/response shape -- kept distinct from {@link SubscriptionEntity}, same
 * DTO/JPA-entity layering precedent as api's WorkItem/WorkItemEntity split (ADR 0012). */
public record Subscription(UUID id, String variantKey, String geneSymbol, String ntfyTopic) {}
