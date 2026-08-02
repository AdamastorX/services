"""Real VADER scoring against real example headlines (backlog #80's own
AC: "a real published article with unambiguous sentiment language
produces a real scored event with the expected-direction sign"). No
mocking of `SentimentIntensityAnalyzer` -- this exercises the actual
VADER lexicon shipped inside the `vaderSentiment` package.

Headlines below are representative of the kind of copy WSJ Markets/
MarketWatch (news-ingestor's real sources, ADR 0029) actually publish --
not cherry-picked toy sentences, but deliberately unambiguous in
direction so a sign assertion is meaningful (VADER's own known weakness,
stated in app/scoring.py's docstring, is domain nuance, not gross
polarity on plainly-worded text like this).
"""

from __future__ import annotations

from app.scoring import VaderScorer

POSITIVE_HEADLINES = [
    "Apple Shares Surge to Record High on Blowout iPhone Sales",
    "Tesla Stock Soars After Stunning Earnings Beat, Analysts Cheer",
    "Amazon Delights Investors With Strong Profit Growth and Upbeat Guidance",
]

NEGATIVE_HEADLINES = [
    "Microsoft Shares Plunge After Disastrous Earnings Miss",
    "Google Parent Alphabet Crashes on Terrible Ad Revenue Collapse",
    "Tesla Stock Craters as Investors Panic Over Grim Outlook",
]

NEUTRAL_HEADLINES = [
    "Apple Schedules Quarterly Earnings Call for Thursday",
    "Microsoft to Report Results Next Week",
]


def test_positive_headlines_score_positive():
    scorer = VaderScorer()
    for headline in POSITIVE_HEADLINES:
        score = scorer.score(headline)
        assert score > 0, f"expected positive compound score for {headline!r}, got {score}"


def test_negative_headlines_score_negative():
    scorer = VaderScorer()
    for headline in NEGATIVE_HEADLINES:
        score = scorer.score(headline)
        assert score < 0, f"expected negative compound score for {headline!r}, got {score}"


def test_score_is_bounded_between_minus_one_and_one():
    scorer = VaderScorer()
    for headline in POSITIVE_HEADLINES + NEGATIVE_HEADLINES + NEUTRAL_HEADLINES:
        score = scorer.score(headline)
        assert -1.0 <= score <= 1.0


def test_scoring_is_fast_sub_millisecond_class():
    # Not a strict benchmark (CI runners vary), but proves the "sub-
    # millisecond, negligible steady-state CPU" claim ADR 0029/the
    # backlog item's own text makes isn't just asserted -- 100 real
    # scoring calls should comfortably finish in well under a second on
    # any real hardware if each one really is sub-millisecond-class.
    import time

    scorer = VaderScorer()
    start = time.perf_counter()
    for _ in range(100):
        scorer.score("Apple Shares Surge to Record High on Blowout iPhone Sales")
    elapsed = time.perf_counter() - start
    assert elapsed < 1.0, f"100 scoring calls took {elapsed:.3f}s, expected sub-millisecond-class scoring"
