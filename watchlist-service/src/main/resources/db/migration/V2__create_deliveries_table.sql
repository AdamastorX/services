-- Backlog #53 / ADR 0026 (outbox-table-plus-relay). This table is the
-- outbox: DeliveryResolutionService inserts one PENDING row per matching
-- (subscription, changed variant) pair transactionally as part of consuming
-- the Kafka event -- the Kafka offset is only acknowledged after this
-- commits, and NotificationRelay's independent poll loop is what actually
-- calls ntfy and marks rows SENT. That split is the whole mechanism behind
-- "a crash between event-consumed and notification-sent doesn't lose it":
-- the row survives the crash in Postgres regardless of which side dies.
--
-- The UNIQUE constraint is what makes re-processing the same Kafka message
-- (a real redelivery, e.g. after a rebalance or a manual offset reset) safe
-- -- DeliveryResolutionService inserts with ON CONFLICT DO NOTHING, so a
-- second resolution of the same (subscription, release, variant) triple is
-- a no-op instead of a duplicate row / duplicate notification.
CREATE TABLE deliveries (
    id               UUID PRIMARY KEY,
    subscription_id  UUID NOT NULL REFERENCES subscriptions (id),
    release_id       VARCHAR(255) NOT NULL,
    variant_key      VARCHAR(255) NOT NULL,
    status           VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    attempts         INT NOT NULL DEFAULT 0,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT deliveries_dedupe_key UNIQUE (subscription_id, release_id, variant_key),
    CONSTRAINT deliveries_status_check CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'DEAD_LETTERED'))
);

-- NotificationRelay's poll query (status = 'PENDING', oldest first) --
-- narrow, partial index rather than indexing the whole table by status,
-- since SENT/DEAD_LETTERED rows accumulate and are never polled again.
CREATE INDEX idx_deliveries_pending ON deliveries (created_at)
    WHERE status = 'PENDING';
