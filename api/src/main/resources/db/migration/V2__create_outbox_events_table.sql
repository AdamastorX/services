-- Backlog #16 / ADR 0026 (outbox-table-plus-relay, decided for real while
-- building watchlist-service#53). Closes the dual-write gap ADR 0012 named
-- and deliberately deferred: api used to persist a work_items row then
-- call KafkaTemplate.send() as two independent operations, so a publish
-- failure after a committed save silently dropped the Kafka side with
-- nothing to retry it. WorkItemOutboxService now writes this row in the
-- same transaction as the work_items save; OutboxRelay is the independent
-- process that actually publishes to Kafka and marks it PUBLISHED --
-- exactly the same split ADR 0026 uses for watchlist-service's own
-- delivery table, applied here to api's original producer-side gap.
CREATE TABLE outbox_events (
    id           UUID PRIMARY KEY,
    topic        VARCHAR(255) NOT NULL,
    message_key  VARCHAR(255),
    payload      TEXT NOT NULL,
    status       VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CONSTRAINT outbox_events_status_check CHECK (status IN ('PENDING', 'PUBLISHED'))
);

CREATE INDEX idx_outbox_events_pending ON outbox_events (created_at)
    WHERE status = 'PENDING';
