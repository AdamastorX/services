"""Environment-driven configuration (backlog #80, ADR 0029).

Same env-var-naming convention `clinvar-service` (ADR 0019) already
established for this project's Python components -- reuse the exact
names the Java `api`/`workers`/`news-ingestor` modules use for the same
infrastructure concept (`KAFKA_BOOTSTRAP_SERVERS`,
`OTLP_COLLECTOR_ENDPOINT`) rather than inventing a second convention just
because this component is Python too.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092"
        )
    )

    # news-ingestor's (backlog #79) output topic -- this service's input.
    news_article_published_topic: str = field(
        default_factory=lambda: os.environ.get(
            "NEWS_ARTICLE_PUBLISHED_TOPIC", "news.article.published"
        )
    )
    # This service's own output topic (backlog #80's AC).
    news_sentiment_scored_topic: str = field(
        default_factory=lambda: os.environ.get(
            "NEWS_SENTIMENT_SCORED_TOPIC", "news.sentiment.scored"
        )
    )
    # One consumer group -- one logical reader of news.article.published.
    # A second replica joining this same group would split partitions
    # between them (correct, cooperative scale-out) rather than
    # double-processing every article the way a second independent
    # group would -- see kubernetes/sentiment-analyzer/deployment.yaml
    # for why this ships as replicas: 1 anyway (no AC need for more yet).
    consumer_group_id: str = field(
        default_factory=lambda: os.environ.get(
            "SENTIMENT_ANALYZER_CONSUMER_GROUP", "sentiment-analyzer"
        )
    )

    otlp_collector_endpoint: str = field(
        default_factory=lambda: os.environ.get(
            "OTLP_COLLECTOR_ENDPOINT",
            "http://otel-collector.otel.svc.cluster.local:4318/v1/traces",
        )
    )

    service_name: str = "sentiment-analyzer"


def get_settings() -> Settings:
    return Settings()
