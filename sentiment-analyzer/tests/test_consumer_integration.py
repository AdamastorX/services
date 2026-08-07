"""Consume-then-produce integration test against a real (testcontainers)
Kafka broker -- not mocked, matching this project's Testcontainers-based
integration-test culture (``clinvar-service``'s own Postgres tests,
``news-ingestor``'s ``FeedPollerIntegrationTest`` real embedded broker).
This is this project's first real end-to-end test of a Python Kafka
*consumer* (no precedent existed before backlog #80).

Proves backlog #80's AC directly:

- a real ``news.article.published`` message naming two tickers produces
  exactly two ``news.sentiment.scored`` events, not one (the AC's
  explicit "an article naming 2 tickers produces 2 events" fan-out
  requirement);
- each event is tagged with the correct ticker, not just any two events;
- an unambiguous, clearly-positive headline scores with a positive sign
  in the real published event, exercising the real VADER scorer, the
  real JSON wire encode/decode, and the real Kafka produce/consume path
  together -- not a unit test of ``VaderScorer`` in isolation (already
  covered by ``tests/test_scoring.py``) asserted to be "good enough".
"""

from __future__ import annotations

import json
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from prometheus_client import generate_latest

from app.kafka_consumer import SentimentConsumerWorker
from app.kafka_producer import SentimentEventProducer
from app.scoring import VaderScorer

ARTICLE_TOPIC = "news.article.published"
SCORED_TOPIC = "news.sentiment.scored"


def _ensure_topics(bootstrap_servers: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(ARTICLE_TOPIC, num_partitions=1, replication_factor=1),
         NewTopic(SCORED_TOPIC, num_partitions=1, replication_factor=1)]
    )
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as exc:  # already exists is fine
            if "already exists" not in str(exc).lower():
                raise


def _produce_article(bootstrap_servers: str, article: dict) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    producer.produce(ARTICLE_TOPIC, key=article["guid"], value=json.dumps(article).encode("utf-8"))
    producer.flush(timeout=10)


def _drain_scored_events(bootstrap_servers: str, guid: str, expected_count: int, timeout_s: float = 20.0) -> list[dict]:
    # The broker fixture is session-scoped (one real broker for the whole
    # test module, not re-created per test -- starting a fresh
    # testcontainers broker per test would be needlessly slow), so
    # SCORED_TOPIC accumulates every test's events across the module.
    # Reading from "earliest" and filtering by this call's own article
    # guid keeps each test's assertions scoped to the events it actually
    # produced, not whatever another test already left on the topic.
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"test-consumer-{time.time_ns()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([SCORED_TOPIC])
    events: list[dict] = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and len(events) < expected_count:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value())
            if event.get("guid") == guid:
                events.append(event)
    finally:
        consumer.close()
    return events


def test_multi_ticker_article_produces_one_correctly_tagged_event_per_ticker(kafka_bootstrap_servers):
    _ensure_topics(kafka_bootstrap_servers)

    worker = SentimentConsumerWorker(
        bootstrap_servers=kafka_bootstrap_servers,
        consume_topic=ARTICLE_TOPIC,
        group_id=f"sentiment-analyzer-test-{time.time_ns()}",
        producer=SentimentEventProducer(kafka_bootstrap_servers, SCORED_TOPIC),
        scorer=VaderScorer(),
        poll_timeout_s=0.5,
    )
    worker.start()
    try:
        article = {
            "tickers": ["AAPL", "AMZN"],
            "headline": "Apple Shares Surge to Record High, Amazon Delights Investors With Blowout Growth",
            "source": "wsj-markets",
            "publishedAt": "2026-08-02T18:50:26Z",
            "link": "https://example.com/story/live-1",
            "guid": "IT-TEST-0001",
        }
        _produce_article(kafka_bootstrap_servers, article)

        events = _drain_scored_events(kafka_bootstrap_servers, guid="IT-TEST-0001", expected_count=2)

        assert len(events) == 2, f"expected exactly one event per ticker (2 tickers), got {len(events)}: {events}"

        by_ticker = {e["ticker"]: e for e in events}
        assert set(by_ticker.keys()) == {"AAPL", "AMZN"}

        for ticker, event in by_ticker.items():
            assert event["guid"] == "IT-TEST-0001"
            assert event["headline"] == article["headline"]
            assert event["source"] == "wsj-markets"
            assert event["link"] == "https://example.com/story/live-1"
            assert event["articlePublishedAt"] == "2026-08-02T18:50:26Z"
            # Unambiguously positive real headline -> real, expected-
            # direction positive sign (backlog #80's own AC).
            assert event["score"] > 0, f"expected positive score for {ticker}, got {event['score']}"
            assert -1.0 <= event["score"] <= 1.0
    finally:
        worker.stop()


def test_negative_headline_produces_negative_sign_event(kafka_bootstrap_servers):
    _ensure_topics(kafka_bootstrap_servers)

    worker = SentimentConsumerWorker(
        bootstrap_servers=kafka_bootstrap_servers,
        consume_topic=ARTICLE_TOPIC,
        group_id=f"sentiment-analyzer-test-{time.time_ns()}",
        producer=SentimentEventProducer(kafka_bootstrap_servers, SCORED_TOPIC),
        scorer=VaderScorer(),
        poll_timeout_s=0.5,
    )
    worker.start()
    try:
        article = {
            "tickers": ["TSLA"],
            "headline": "Tesla Stock Craters as Investors Panic Over Grim Outlook",
            "source": "marketwatch",
            "publishedAt": "2026-08-02T19:10:00Z",
            "link": "https://example.com/story/live-2",
            "guid": "IT-TEST-0002",
        }
        _produce_article(kafka_bootstrap_servers, article)

        events = _drain_scored_events(kafka_bootstrap_servers, guid="IT-TEST-0002", expected_count=1)

        assert len(events) == 1
        event = events[0]
        assert event["ticker"] == "TSLA"
        assert event["score"] < 0, f"expected negative score, got {event['score']}"
    finally:
        worker.stop()


def test_consumer_lag_gauge_populated_from_real_librdkafka_stats_cb(kafka_bootstrap_servers):
    """backlog #90: sentiment_analyzer_consumer_lag has to come from
    librdkafka's own real statistics, not a value this test fabricates
    -- so this drives a real produce, a real consumer lagging behind it
    by construction (the message is produced *before* the worker
    starts), and waits for librdkafka's own stats_cb to fire and set
    the gauge, then reads it back through the same generate_latest()
    scrape path Prometheus itself would hit (test_metrics.py's own
    precedent in clinvar-service), not by reaching into the Gauge
    object directly.
    """
    _ensure_topics(kafka_bootstrap_servers)
    topic = ARTICLE_TOPIC

    # Produced before the worker (and therefore its consumer group)
    # exists -- guarantees a real, non-zero lag for librdkafka to
    # actually report on the first stats tick, rather than a lag of 0
    # that wouldn't distinguish "the gauge works" from "the gauge is
    # stuck at its default".
    _produce_article(
        kafka_bootstrap_servers,
        {
            "tickers": ["MSFT"],
            "headline": "Microsoft Reports Steady Cloud Growth",
            "source": "wsj-markets",
            "publishedAt": "2026-08-06T12:00:00Z",
            "link": "https://example.com/story/lag-test",
            "guid": "IT-TEST-LAG-0001",
        },
    )

    worker = SentimentConsumerWorker(
        bootstrap_servers=kafka_bootstrap_servers,
        consume_topic=topic,
        group_id=f"sentiment-analyzer-lag-test-{time.time_ns()}",
        producer=SentimentEventProducer(kafka_bootstrap_servers, SCORED_TOPIC),
        scorer=VaderScorer(),
        poll_timeout_s=0.5,
        # Real, fast stats_cb interval for this test only -- the
        # production default (15s) would make this test slow for no
        # extra confidence; librdkafka's own stats_cb mechanism is what
        # is under test, not the specific interval value.
        stats_interval_ms=500,
    )
    worker.start()
    try:
        deadline = time.monotonic() + 15.0
        body = ""
        while time.monotonic() < deadline:
            body = generate_latest().decode()
            if "sentiment_analyzer_consumer_lag" in body and 'partition="0"' in body:
                break
            time.sleep(0.5)

        assert "sentiment_analyzer_consumer_lag" in body, (
            f"expected a real sentiment_analyzer_consumer_lag sample from librdkafka's own stats_cb "
            f"within 15s, got none. Full scrape:\n{body}"
        )
    finally:
        worker.stop()
