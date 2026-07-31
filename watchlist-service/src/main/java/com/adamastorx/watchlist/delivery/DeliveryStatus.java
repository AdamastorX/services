package com.adamastorx.watchlist.delivery;

public enum DeliveryStatus {
    PENDING,
    /** Claimed by one NotificationRelay tick (see the claim() update) -- prevents two
     * relay ticks (e.g. during a rolling-update surge, resourcequota.yaml allows a
     * second pod briefly) from both sending the same delivery. */
    SENDING,
    SENT,
    DEAD_LETTERED
}
