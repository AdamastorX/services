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

    def to_json(self) -> str:
        return json.dumps(
            {
                "newReleaseId": str(self.new_release_id),
                "previousReleaseId": str(self.previous_release_id) if self.previous_release_id else None,
                "publishedDate": self.published_date,
                "variantCount": self.variant_count,
                "ingestedAt": self.ingested_at,
                "changedKeys": self.changed_keys,
            }
        )


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

        self._producer.produce(
            self._topic,
            value=event.to_json().encode("utf-8"),
            callback=_delivery_callback,
        )
        # Fire-and-forget-ish, but flush with a bound so a failed/slow
        # broker doesn't hang ingestion forever -- ingestion itself is
        # already async/scheduled, not a request path.
        self._producer.flush(timeout=30)

    def close(self) -> None:
        self._producer.flush(timeout=10)
