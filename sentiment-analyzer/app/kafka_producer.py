"""Publishes `news.sentiment.scored` (backlog #80, ADR 0029).

`confluent-kafka` (librdkafka-backed) -- the same production Kafka client
`clinvar-service` (ADR 0019) already established as this project's Python
choice, reused rather than picking a second one.

Bounded, synchronous publish: `produce()` then a bounded `flush()`,
raising if delivery didn't confirm within the timeout or the delivery
report carried an error. Same "correctness over raw throughput" choice
`news-ingestor`'s `ArticlePublisher` (a bounded `send().get(timeout)`)
and `clinvar-service`'s `IngestionEventProducer` (`flush(timeout=30)`
right after every `produce()`) both already made, applied here at the
same per-event granularity -- this service's real traffic (a handful of
matched articles per 5-minute news-ingestor poll cycle, times however
many tickers each names) is nowhere near the volume where per-event
`flush()` would be a real bottleneck, so there's no throughput reason to
switch to fire-and-forget batching only to lose the "did this actually
land" signal `sentiment_analyzer_publish_errors_total` depends on.
"""

from __future__ import annotations

import logging

from confluent_kafka import Producer

from app.events import SentimentScoredEvent

logger = logging.getLogger(__name__)


class SentimentEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._topic = topic

    def publish(self, event: SentimentScoredEvent, key: str | None = None, timeout: float = 5.0) -> None:
        """Raises if the send could not be confirmed delivered within
        `timeout` seconds, or the broker reported a delivery error --
        callers (`app/kafka_consumer.py`) treat that as a dropped event
        (`sentiment_analyzer_publish_errors_total`), not a retry-forever
        loop; see this module's docstring and the README for the
        tradeoff.
        """
        delivery_errors: list[Exception] = []

        def _delivery_callback(err, _msg) -> None:
            if err is not None:
                delivery_errors.append(RuntimeError(str(err)))

        self._producer.produce(
            self._topic,
            key=key.encode("utf-8") if key else None,
            value=event.to_json().encode("utf-8"),
            callback=_delivery_callback,
        )
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            raise RuntimeError(
                f"Kafka producer flush timed out after {timeout}s with {remaining} message(s) still in-flight"
            )
        if delivery_errors:
            raise delivery_errors[0]

    def close(self) -> None:
        self._producer.flush(timeout=10)
