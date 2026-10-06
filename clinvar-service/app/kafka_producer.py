"""Publishes the ``clinvar.ingestion.completed`` event (ADR 0019).

``confluent-kafka`` (librdkafka-backed), the same production choice the
ADR calls for. Wire shape below is the *new* contract this ADR
introduces -- a superset of ADR 0018's original event (``newReleaseId``
replaces ``releaseId`` for symmetry with ``previousReleaseId``, and
``changedKeys`` is new: the whole reason api no longer needs to re-read a
tabix file itself to know what to invalidate).
"""

from __future__ import annotations

import json
import logging
import uuid

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

# Payload budget per message. librdkafka's own producer limit
# (message.max.bytes) is 1,000,000 bytes and the broker's default is
# 1,048,588, so 600,000 leaves a wide margin for the record envelope.
MAX_EVENT_PAYLOAD_BYTES = 600_000


class IngestionCompletedEvent:
    def __init__(
        self,
        new_release_id: uuid.UUID,
        previous_release_id: uuid.UUID | None,
        published_date: str,
        variant_count: int,
        ingested_at: str,
        changed_keys: list[str],
    ) -> None:
        self.new_release_id = new_release_id
        self.previous_release_id = previous_release_id
        self.published_date = published_date
        self.variant_count = variant_count
        self.ingested_at = ingested_at
        self.changed_keys = changed_keys

    def _payload(self, changed_keys: list[str]) -> str:
        return json.dumps(
            {
                "newReleaseId": str(self.new_release_id),
                "previousReleaseId": str(self.previous_release_id) if self.previous_release_id else None,
                "publishedDate": self.published_date,
                "variantCount": self.variant_count,
                "ingestedAt": self.ingested_at,
                "changedKeys": changed_keys,
            }
        )

    def to_json(self) -> str:
        return self._payload(self.changed_keys)

    def to_json_chunks(self, max_bytes: int = MAX_EVENT_PAYLOAD_BYTES) -> list[str]:
        """The event as one or more payloads, each at most ``max_bytes``.

        ``changedKeys`` is every Redis key whose ClinVar classification changed
        since the previous release, so its size is the size of the diff: a
        weekly diff is small, but a long gap between ingestions (or the first
        ingestion of a full release) is not, and a single event past the
        broker's ``message.max.bytes`` fails with ``MSG_SIZE_TOO_LARGE`` after
        the whole VCF has already been scanned (found live 2026-10-05, the
        first ingestion in six weeks).

        Every chunk carries the same release fields and a slice of the keys,
        so each is a valid ``clinvar.ingestion.completed`` event under the
        unchanged schema. That is safe for the consumer: api deletes the keys
        in whatever event it receives and holds no per-release state, so the
        events are independent and idempotent. An event with no changed keys
        still yields exactly one payload, as before.
        """
        base = len(self._payload([]).encode("utf-8"))
        chunks: list[list[str]] = []
        current: list[str] = []
        size = base
        for key in self.changed_keys:
            # +2 for the ", " separator json.dumps puts between items.
            cost = len(json.dumps(key).encode("utf-8")) + 2
            if current and size + cost > max_bytes:
                chunks.append(current)
                current = []
                size = base
            current.append(key)
            size += cost
        chunks.append(current)
        return [self._payload(chunk) for chunk in chunks]


class IngestionEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._topic = topic

    def publish(self, event: IngestionCompletedEvent) -> None:
        # Null key, same reasoning as the Java producer this replaces: no
        # natural partitioning key, one consumer group, ingestions are
        # already serialized by the weekly/manual trigger so
        # cross-ingestion ordering isn't a real concern.
        def _delivery_callback(err, msg) -> None:
            if err is not None:
                logger.error("Failed to deliver ClinVar ingestion event: %s", err)

        payloads = event.to_json_chunks()
        if len(payloads) > 1:
            logger.info(
                "Release %s has %d changed keys: publishing %d events to stay under the broker's message size limit",
                event.new_release_id,
                len(event.changed_keys),
                len(payloads),
            )
        for payload in payloads:
            self._producer.produce(
                self._topic,
                value=payload.encode("utf-8"),
                callback=_delivery_callback,
            )
            # Serve delivery callbacks and keep librdkafka's local queue from
            # filling when a large diff is split into many events.
            self._producer.poll(0)
        # Fire-and-forget-ish, but flush with a bound so a failed/slow
        # broker doesn't hang ingestion forever -- ingestion itself is
        # already async/scheduled, not a request path.
        remaining = self._producer.flush(timeout=30)
        if remaining:
            logger.warning("%d ClinVar ingestion event(s) still undelivered after the flush timeout", remaining)

    def close(self) -> None:
        self._producer.flush(timeout=10)
