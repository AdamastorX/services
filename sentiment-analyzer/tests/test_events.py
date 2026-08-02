"""ArticlePublishedEvent.from_json against the real wire shape
news-ingestor's ArticlePublishedEvent record actually produces (field
names/order: tickers, headline, source, publishedAt, link, guid) --
and SentimentScoredEvent.to_json's own shape.
"""

from __future__ import annotations

import json

from app.events import ArticlePublishedEvent, SentimentScoredEvent


def test_parses_real_wire_shape_with_numeric_published_at():
    # One real Jackson default encoding candidate for java.time.Instant
    # (WRITE_DATES_AS_TIMESTAMPS=true, the SDK default with no override
    # found anywhere in news-ingestor -- see app/events.py's docstring).
    raw = json.dumps(
        {
            "tickers": ["AAPL", "AMZN"],
            "headline": "Apple Falls, But Amazon Push Nasdaq Higher",
            "source": "wsj-markets",
            "publishedAt": 1785790226.123000000,
            "link": "https://example.com/story/live-1",
            "guid": "WP-LIVE-TEST-0001",
        }
    ).encode("utf-8")

    event = ArticlePublishedEvent.from_json(raw)

    assert event.tickers == ["AAPL", "AMZN"]
    assert event.headline == "Apple Falls, But Amazon Push Nasdaq Higher"
    assert event.source == "wsj-markets"
    assert event.published_at == 1785790226.123
    assert event.link == "https://example.com/story/live-1"
    assert event.guid == "WP-LIVE-TEST-0001"


def test_parses_real_wire_shape_with_string_published_at():
    # The other real candidate encoding (an ISO-8601 string) -- from_json
    # must not assume one shape over the other (see app/events.py's
    # docstring on why published_at is opaque pass-through).
    raw = json.dumps(
        {
            "tickers": ["TSLA"],
            "headline": "Tesla Stock Craters as Investors Panic",
            "source": "marketwatch",
            "publishedAt": "2026-08-02T18:50:26Z",
            "link": "https://example.com/story/live-2",
            "guid": "WP-LIVE-TEST-0002",
        }
    ).encode("utf-8")

    event = ArticlePublishedEvent.from_json(raw)

    assert event.published_at == "2026-08-02T18:50:26Z"


def test_sentiment_scored_event_echoes_published_at_unchanged():
    event = SentimentScoredEvent(
        ticker="AAPL",
        score=0.65,
        headline="Apple Shares Surge to Record High",
        source="wsj-markets",
        published_at=1785790226.123,
        link="https://example.com/story/live-1",
        guid="WP-LIVE-TEST-0001",
        scored_at="2026-08-02T19:00:00+00:00",
    )

    payload = json.loads(event.to_json())

    assert payload == {
        "ticker": "AAPL",
        "score": 0.65,
        "headline": "Apple Shares Surge to Record High",
        "source": "wsj-markets",
        "articlePublishedAt": 1785790226.123,
        "link": "https://example.com/story/live-1",
        "guid": "WP-LIVE-TEST-0001",
        "scoredAt": "2026-08-02T19:00:00+00:00",
    }
