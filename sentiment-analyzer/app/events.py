"""Wire shapes for `news.article.published` (consumed) and
`news.sentiment.scored` (produced) -- backlog #80, ADR 0029.

**`news.article.published`'s real wire shape has no summary/description
field** (confirmed by reading the actual merged record,
`news-ingestor/src/main/java/com/adamastorx/newsingestor/publishing/ArticlePublishedEvent.java`,
backlog #79):

```java
public record ArticlePublishedEvent(
        List<String> tickers, String headline, String source, Instant publishedAt, String link, String guid) {}
```

Backlog #80's own AC text says "over the headline+summary text" -- there
is no summary field to read. This is not silently worked around by
inventing one: VADER runs against `headline` alone. Stated here, in the
README, and in this PR's description, exactly once each, not glossed --
a second, smaller accepted v1 gap layered on top of the AC's own already-
stated "VADER is tuned on general/social-media text, not finance
jargon" gap. Fixing this for real means changing `news-ingestor`'s event
shape, a separate decision for a human, out of scope here.

**`publishedAt` is treated as an opaque JSON value, not parsed into a
Python datetime.** Jackson's `JsonSerializer` (Spring Kafka,
`NewsPublisherKafkaConfig`) has no `WRITE_DATES_AS_TIMESTAMPS=false`
override anywhere in `news-ingestor` (checked: no such config exists in
its `application.yml` or Java config classes) -- so whatever the SDK's
default `java.time.Instant` encoding actually is (a numeric epoch value,
or an ISO-8601 string, depending on Jackson/jackson-datatype-jsr310
defaults) was **not independently re-verified against a live producer in
this implementation session** (no running news-ingestor + Kafka broker
available here). Rather than guess a wire format and risk silently
mis-parsing it, `ArticlePublishedEvent.from_json` reads `publishedAt`
as whatever `json.loads` decodes it to (`int`, `float`, `str`, or a
`list`) and `SentimentScoredEvent` echoes that same value back out
unchanged into `news.sentiment.scored`'s own `articlePublishedAt` field.
No sentiment-analyzer logic ever needs `publishedAt` as a real timestamp
(only as pass-through article-reference data), so this sidesteps the
ambiguity instead of asserting a format this session couldn't confirm.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ArticlePublishedEvent:
    tickers: list[str]
    headline: str
    source: str
    published_at: Any
    link: str
    guid: str

    @staticmethod
    def from_json(raw: bytes) -> "ArticlePublishedEvent":
        data = json.loads(raw)
        return ArticlePublishedEvent(
            tickers=list(data["tickers"]),
            headline=data["headline"],
            source=data["source"],
            published_at=data.get("publishedAt"),
            link=data["link"],
            guid=data["guid"],
        )


@dataclass(frozen=True)
class SentimentScoredEvent:
    """One per (article, ticker) pair -- backlog #80's AC. `score` is
    VADER's compound score, -1..+1. `scored_at` is this service's own
    processing timestamp (ISO-8601 UTC, unambiguous since this service
    produces it directly rather than passing it through)."""

    ticker: str
    score: float
    headline: str
    source: str
    published_at: Any
    link: str
    guid: str
    scored_at: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "ticker": self.ticker,
                "score": self.score,
                "headline": self.headline,
                "source": self.source,
                "articlePublishedAt": self.published_at,
                "link": self.link,
                "guid": self.guid,
                "scoredAt": self.scored_at,
            }
        )
