"""Consumes `news.article.published`, scores, publishes `news.sentiment.scored`
(backlog #80, ADR 0029) -- this project's **first Python Kafka consumer**
(`clinvar-service` only ever produced, via `app/kafka_producer.py`-shaped
code; no consumer precedent existed anywhere in this repo before this).

**Why a plain background thread running a `confluent_kafka.Consumer`
poll loop, not APScheduler** (the "app + background worker" tool
`clinvar-service/app/scheduler.py` already uses): APScheduler is built
for periodic/cron-triggered jobs -- a callable invoked on a schedule,
that returns and gets invoked again later. A Kafka consumer is the
opposite shape: one long-lived, tight, *blocking* `poll()` loop that
must run continuously for the life of the process, reacting to messages
arriving on Kafka's own schedule, not this service's. Modeling that as a
"scheduled job" would mean either a job that reschedules itself every
`poll()` call (pointless overhead, fights the scheduler's own design) or
one job that runs forever and blocks its executor thread for the
process's whole lifetime (defeats the purpose of using a scheduler at
all). A plain `threading.Thread` running an unbounded loop is the
directly-matching primitive for "one thing that runs forever in the
background alongside uvicorn" -- the same reasoning
`clinvar-service/app/scheduler.py`'s own docstring uses to explain why
it picked APScheduler over a Kubernetes CronJob for the *opposite*
reason (picking the tool whose shape actually matches the work, not
defaulting to whatever's already in the codebase).
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from app.events import ArticlePublishedEvent, SentimentScoredEvent
from app.kafka_producer import SentimentEventProducer
from app.metrics import (
    ARTICLES_CONSUMED_TOTAL,
    CONSUME_ERRORS_TOTAL,
    CONSUMER_LAG,
    EVENTS_PUBLISHED_TOTAL,
    PUBLISH_ERRORS_TOTAL,
    SCORING_DURATION_SECONDS,
)
from app.scoring import VaderScorer

logger = logging.getLogger(__name__)


class SentimentConsumerWorker:
    """One consumer-group member, one background thread. Manual offset
    commit, once per fully-handled message (after every matched ticker's
    event has at least been *attempted* -- see `_handle_message`) --
    at-least-once processing, the same model `workers` (Java, this
    project's other Kafka consumer) already runs under. **Accepted v1
    gap, stated explicitly**: a crash between finishing `_handle_message`
    and the commit landing would reprocess that article on restart and
    republish duplicate `news.sentiment.scored` events for it (a
    duplicate event, not a lost one) -- no idempotency/dedup key exists
    on the consumer side. Same trade `news-ingestor`'s own
    `ArticleDedupService` accepted for a different failure window
    (there: a pod restart forgets recently-seen articles; here: a crash
    mid-commit can double-publish) -- fixing this for real would mean an
    outbox-style dedup table, real scope this item doesn't need yet
    (ADR 0021's anti-gold-plating discipline).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        consume_topic: str,
        group_id: str,
        producer: SentimentEventProducer,
        scorer: VaderScorer,
        poll_timeout_s: float = 1.0,
        # backlog #90: 15s matches this project's other near-real-time
        # scrape/poll cadences (news-ingestor's own 5-minute feed poll
        # is a different, much coarser cadence for a different reason)
        # -- frequent enough for a consumer-lag alert to have real
        # signal, not so frequent it adds meaningful librdkafka-internal
        # overhead for a single-partition topic. A real constructor
        # parameter (not a fixed literal) specifically so
        # test_consumer_lag_gauge_populated_from_real_librdkafka_stats_cb
        # doesn't have to wait out the full production interval for a
        # real stats_cb tick.
        stats_interval_ms: int = 15000,
    ) -> None:
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "statistics.interval.ms": stats_interval_ms,
                "stats_cb": self._on_stats,
            }
        )
        self._consume_topic = consume_topic
        self._producer = producer
        self._scorer = scorer
        self._poll_timeout_s = poll_timeout_s
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._consumer.subscribe([self._consume_topic])
        self._thread = threading.Thread(target=self._run, name="sentiment-consumer", daemon=True)
        self._thread.start()
        logger.info(
            "sentiment-analyzer consumer started: topic=%s group=%s",
            self._consume_topic,
            self._consumer,
        )

    def stop(self, timeout_s: float = 10.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout_s)
        self._consumer.close()

    def _on_stats(self, stats_json: str) -> None:
        """librdkafka's own `statistics.interval.ms` callback (backlog
        #90) -- real per-partition lag, computed by librdkafka itself
        from the broker's own high-watermark vs. this consumer's last
        committed offset, not derived independently here. Real JSON
        shape (librdkafka's STATISTICS.md): `topics.<topic>.partitions`
        is a dict keyed by partition id *plus* a synthetic `"-1"` entry
        that is librdkafka's own cross-partition aggregate row, not a
        real partition -- skipped here, or `sentiment_analyzer_consumer_lag`
        would double-count every real partition's lag into a bogus
        extra series. `consumer_lag` is `-1` (librdkafka's own "not yet
        computed" sentinel, real for the first few seconds after
        (re)connect) until the first real fetch response lands --
        normalized to `0` rather than published as a literal negative
        lag value (see metrics.py's own comment on `CONSUMER_LAG`).
        Never raises out into librdkafka's C callback path: a malformed
        or unexpected stats payload is logged and skipped, since a
        broken metrics gauge is not worth risking the actual consumer
        loop over.
        """
        try:
            stats = json.loads(stats_json)
            topic_stats = stats.get("topics", {}).get(self._consume_topic, {})
            for partition_id, partition_stats in topic_stats.get("partitions", {}).items():
                if partition_id == "-1":
                    continue
                lag = partition_stats.get("consumer_lag", -1)
                CONSUMER_LAG.labels(partition=partition_id).set(max(lag, 0))
        except Exception:
            logger.warning("Failed to parse librdkafka stats payload for consumer lag, skipping", exc_info=True)

    def is_alive(self) -> bool:
        """Real liveness signal for `GET /healthz` (`app/routes/health.py`)
        -- found live during review: without this, an uncaught exception
        inside `_run`'s loop body (e.g. an unexpected non-string
        `headline` reaching `VaderScorer.score`) would silently kill this
        daemon thread forever while `/healthz` kept returning `{"status":
        "UP"}` unconditionally -- the pod would report Healthy/Ready with
        no consumer actually running, and Kubernetes would never restart
        it. `_run` itself now also guards its per-message work in a
        try/except so a single bad message can't reach this state in the
        first place; this method is the second, independent layer -- if
        the thread ever *does* die despite that, this makes it a real,
        observable, restart-triggering failure instead of a silent one.
        """
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        logger.info("Sentiment consumer loop running, topic=%s", self._consume_topic)
        while not self._stop_event.is_set():
            msg = self._consumer.poll(self._poll_timeout_s)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka consume error: %s", msg.error())
                CONSUME_ERRORS_TOTAL.inc()
                continue

            try:
                self._handle_message(msg.value())
                self._consumer.commit(message=msg, asynchronous=False)
            except Exception:
                # Found live during review: neither ArticlePublishedEvent
                # parsing nor the publish() call inside
                # _score_and_publish_one can escape uncaught (both have
                # their own try/except), but VaderScorer.score() itself
                # was not guarded -- an unexpected input (e.g. a
                # malformed message that parses but yields a non-string
                # headline) would otherwise propagate out of this loop
                # and kill the daemon thread for the rest of the
                # process's life, with is_alive() (above) the only thing
                # left to notice. Treated the same as every other
                # per-message failure mode here: logged, counted, this
                # message's offset is not committed (so a restarted
                # consumer would re-fetch it -- consistent with this
                # class's own stated at-least-once/no-idempotency model,
                # not a new gap), the loop keeps running for the next
                # message rather than the whole thread going down over
                # one bad one.
                logger.error("Unexpected error handling a news.article.published message, skipping", exc_info=True)
                CONSUME_ERRORS_TOTAL.inc()

    def _handle_message(self, raw_value: bytes | None) -> None:
        if raw_value is None:
            return

        try:
            article = ArticlePublishedEvent.from_json(raw_value)
        except Exception:
            logger.error("Failed to parse news.article.published message, skipping", exc_info=True)
            CONSUME_ERRORS_TOTAL.inc()
            return

        ARTICLES_CONSUMED_TOTAL.inc()

        if not article.tickers:
            # news-ingestor's own ArticlePublishedEvent contract says
            # tickers is "non-empty (an event is never published for
            # zero matches)" -- defensive skip, not trusted blindly.
            logger.warning(
                "Consumed a news.article.published message with no tickers (guid=%s) -- skipping", article.guid
            )
            return

        for ticker in article.tickers:
            self._score_and_publish_one(article, ticker)

    def _score_and_publish_one(self, article: ArticlePublishedEvent, ticker: str) -> None:
        with SCORING_DURATION_SECONDS.time():
            score = self._scorer.score(article.headline)

        event = SentimentScoredEvent(
            ticker=ticker,
            score=score,
            headline=article.headline,
            source=article.source,
            published_at=article.published_at,
            link=article.link,
            guid=article.guid,
            scored_at=datetime.now(timezone.utc).isoformat(),
        )
        try:
            self._producer.publish(event, key=ticker)
            EVENTS_PUBLISHED_TOTAL.inc()
        except Exception:
            # Dropped, not retried -- same accepted-gap precedent
            # news-ingestor's ArticlePublisher already states for its own
            # publish path (see app/kafka_producer.py's docstring).
            logger.error(
                "Failed to publish news.sentiment.scored for guid=%s ticker=%s", article.guid, ticker, exc_info=True
            )
            PUBLISH_ERRORS_TOTAL.inc()
