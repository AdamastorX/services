"""Prometheus metrics (backlog #80, ADR 0029/0020).

`sentiment_analyzer_*` naming, matching this project's per-service metric
prefix convention (`clinvar_*`, `news_ingestor_*`, `market_data_*`). Five
metrics, the minimum the backlog item's own "real metrics (articles-
consumed, events-published, scoring-latency at minimum)" line names, plus
two error counters -- a consume/parse failure and a publish failure are
each a real, distinct failure mode (ADR 0020's "a rejection/failure is
itself a signal" discipline, same reasoning `clinvar_ingestion_rejected_total`
and `news_ingestor_feed_poll_total{outcome="unreachable"}` already apply),
not left to show up only as "consumed count stayed flat" with no error
signal of its own.

Same single-process/default-registry reasoning as
`clinvar-service/app/metrics.py`'s own module docstring: this service
runs uvicorn with no `--workers` flag (see `Dockerfile`), so there is
exactly one process and no need for `prometheus_client`'s multiprocess
mode.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

ARTICLES_CONSUMED_TOTAL = Counter(
    "sentiment_analyzer_articles_consumed_total",
    "news.article.published messages consumed (one per article, regardless of ticker count).",
)

EVENTS_PUBLISHED_TOTAL = Counter(
    "sentiment_analyzer_events_published_total",
    "news.sentiment.scored events successfully published, one per (article, ticker) pair.",
)

PUBLISH_ERRORS_TOTAL = Counter(
    "sentiment_analyzer_publish_errors_total",
    "news.sentiment.scored publish attempts that failed. Dropped, not retried -- see README's "
    "accepted-gap note (same drop-on-failure precedent news-ingestor's own ArticlePublisher established).",
)

CONSUME_ERRORS_TOTAL = Counter(
    "sentiment_analyzer_consume_errors_total",
    "news.article.published consume/parse failures -- a Kafka-reported error or malformed JSON payload.",
)

SCORING_DURATION_SECONDS = Histogram(
    "sentiment_analyzer_scoring_duration_seconds",
    "Wall-clock time for one VADER compound-score computation over a single headline.",
)

__all__ = [
    "ARTICLES_CONSUMED_TOTAL",
    "EVENTS_PUBLISHED_TOTAL",
    "PUBLISH_ERRORS_TOTAL",
    "CONSUME_ERRORS_TOTAL",
    "SCORING_DURATION_SECONDS",
]
