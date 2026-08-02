"""VADER lexicon-based sentiment scoring (backlog #80, ADR 0029).

ADR 0029's decision, restated once here: VADER (`vaderSentiment`) over a
FinBERT-style transformer, on a real, current, measured CPU basis --
#77's accounting had this node at 63% of allocatable requested (2545m/
4000m, ~1.4 free cores) before this milestone's five new always-on
services. VADER is sub-millisecond, no model download (its lexicon ships
inside the PyPI package, no network fetch at import or score time), no
GPU. `sentiment_analyzer_scoring_duration_seconds`
(`app/metrics.py`) verifies the "sub-millisecond" part is real, not just
asserted from VADER's own reputation.

**Two accepted v1 gaps, stated explicitly (backlog #80's AC demands
this, not an afterthought):**

1. VADER is tuned on general/social-media text, not finance-specific
   jargon -- it has no notion that "beats estimates" or "guidance cut"
   carry domain-specific sentiment weight a general lexicon wouldn't
   assign them. A FinBERT-style (or comparable finance-tuned
   transformer) model is recorded as a real, explicit, **deferred v2
   upgrade** -- revisited once M7's dedicated hardware exists and idle
   capacity is actually measured against it (ADR 0029 section 3), not
   silently dropped.
2. Scoring runs against `headline` alone, not "headline+summary" the AC
   text names -- see `app/events.py`'s module docstring for why (news-
   ingestor's real `ArticlePublishedEvent` wire shape has no summary
   field to read).
"""

from __future__ import annotations

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


class VaderScorer:
    """Thin wrapper -- `SentimentIntensityAnalyzer` itself is not
    documented as thread-safe or unsafe either way; this service runs
    exactly one consumer thread calling `score()` (see
    `app/kafka_consumer.py`), so this is never a real concern here, but
    kept as its own class (rather than a bare module-level analyzer) so
    that boundary is explicit and a future multi-threaded consumer
    wouldn't inherit an unstated assumption.
    """

    def __init__(self) -> None:
        self._analyzer = SentimentIntensityAnalyzer()

    def score(self, text: str) -> float:
        """Returns VADER's compound score, -1.0 (most negative) to +1.0
        (most positive)."""
        return self._analyzer.polarity_scores(text)["compound"]
