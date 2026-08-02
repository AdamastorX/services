# sentiment-analyzer

Backlog #80 (M13, ADR 0029). Consumes `news.article.published`
(news-ingestor, backlog #79) and, for every ticker an article names,
computes a VADER compound sentiment score and publishes one
`news.sentiment.scored` event per (article, ticker) pair.

## Shape

- **Language**: Python, per ADR 0019's rule applied the same way it was
  applied to `clinvar-service` -- a real, stated ecosystem reason (VADER
  has no equivalent, equally-established zero-cost Java lexicon library),
  not Python by default. ADR 0029 section 4 records this explicitly.
- **Scoring**: [`vaderSentiment`](https://pypi.org/project/vaderSentiment/)
  (`app/scoring.py`), a mature lexicon/rule-based scorer -- sub-
  millisecond per call (`tests/test_scoring.py::test_scoring_is_fast_sub_millisecond_class`
  proves this against the real package, not asserted from its
  reputation), no model download (its lexicon ships inside the PyPI
  package), no GPU.
- **Storage**: none. Stateless, same shape `news-ingestor` already is --
  no dedicated Postgres instance.
- **Kafka**: this project's **first Python Kafka consumer**
  (`clinvar-service`, ADR 0019, only ever produced). A
  `confluent_kafka.Consumer` poll loop on a plain background thread
  (`app/kafka_consumer.py`), started alongside a small FastAPI app
  (`/healthz`, `/metrics`) in `app/main.py`'s lifespan -- see
  `app/kafka_consumer.py`'s module docstring for why a thread and not
  APScheduler (`clinvar-service`'s own "app + background worker" tool):
  APScheduler is built for periodic/cron-triggered jobs: a callable
  invoked on a schedule that returns and gets invoked again later. A
  Kafka consumer is the opposite shape -- one long-lived, continuously-
  blocking `poll()` loop reacting to messages on Kafka's own schedule,
  not this service's. A plain thread is the primitive that actually
  matches that shape.

## Two accepted v1 gaps, stated explicitly (not glossed over)

Backlog #80's own AC requires both of these to be named plainly, not
assumed away:

1. **VADER is tuned on general/social-media text, not finance-specific
   jargon.** It has no notion that "beats estimates" or "guidance cut"
   carry domain-specific sentiment weight a general lexicon wouldn't
   assign them. **A FinBERT-style (or comparable finance-tuned
   transformer) model is recorded here as a real, explicit, deferred v2
   upgrade** -- ADR 0029 section 3 records the real, current, measured
   reason it isn't v1: #77's CPU accounting had this node at 63% of
   allocatable requested (2545m/4000m, ~1.4 free cores) before this
   milestone's five new always-on services; a CPU-only transformer's
   realistic inference footprint would eat most of that remaining
   headroom for one of the five. Revisit once M7's dedicated hardware
   exists and idle capacity is actually measured against it.
2. **Scoring runs against `headline` only, not "headline+summary" the AC
   text names.** The real, merged `news.article.published` wire shape
   (`news-ingestor/src/main/java/com/adamastorx/newsingestor/publishing/ArticlePublishedEvent.java`,
   backlog #79, now on `services` main) is:

   ```java
   public record ArticlePublishedEvent(
           List<String> tickers, String headline, String source, Instant publishedAt, String link, String guid) {}
   ```

   There is no `summary`/`description` field. This is not silently
   worked around by inventing one on this service's side -- `app/events.py`'s
   module docstring states this in full, and VADER runs against
   `headline` alone: a smaller (but real) accepted v1 gap, layered on
   top of gap 1 above, not hidden inside it. Fixing this for real means
   changing `news-ingestor`'s event shape -- a separate decision for a
   human, out of scope for this PR.

## `publishedAt` is treated as opaque pass-through data

`news-ingestor`'s Kafka producer config (`NewsPublisherKafkaConfig`) has
no `WRITE_DATES_AS_TIMESTAMPS=false` override anywhere in its
`application.yml` or Java config classes, so this service does not
assume a specific Jackson `java.time.Instant` wire encoding (a numeric
epoch value vs. an ISO-8601 string) -- **not independently re-verified
against a live producer in this implementation session** (no running
news-ingestor + Kafka broker available here). `app/events.py`'s
`ArticlePublishedEvent.from_json` reads `publishedAt` as whatever
`json.loads` decodes it to and `SentimentScoredEvent` echoes that same
value back out, unchanged, as `articlePublishedAt` -- this service never
needs to interpret it as a real timestamp, only carry it through as
article-reference data, so the ambiguity is sidestepped rather than
guessed at. See `app/events.py`'s module docstring for the full
reasoning; `tests/test_events.py` proves both plausible encodings parse
and round-trip correctly.

## Kafka contract

**Consumes** `news.article.published` (configurable via
`NEWS_ARTICLE_PUBLISHED_TOPIC`), consumer group
`SENTIMENT_ANALYZER_CONSUMER_GROUP` (default `sentiment-analyzer`).

**Publishes** `news.sentiment.scored` (configurable via
`NEWS_SENTIMENT_SCORED_TOPIC`), one event per (article, ticker) pair --
an article naming 2 tickers produces 2 events, keyed on the ticker
(partition locality for "recent sentiment per ticker", same reasoning
`news-ingestor`'s own `ArticlePublisher` uses for its first-matched-
ticker key):

```json
{
  "ticker": "AAPL",
  "score": 0.8176,
  "headline": "Apple Shares Surge to Record High, Amazon Delights Investors With Blowout Growth",
  "source": "wsj-markets",
  "articlePublishedAt": "2026-08-02T18:50:26Z",
  "link": "https://example.com/story/live-1",
  "guid": "IT-TEST-0001",
  "scoredAt": "2026-08-02T19:00:03.412871+00:00"
}
```

`score` is VADER's compound score, -1.0 (most negative) to +1.0 (most
positive). `scoredAt` is this service's own processing timestamp
(ISO-8601 UTC) -- unambiguous, since this service produces it directly
rather than passing it through from another producer's serializer.

## Processing model: at-least-once, no consumer-side idempotency (v1 gap)

Manual offset commit, once per fully-handled `news.article.published`
message (after every named ticker's event has at least been attempted).
**Accepted v1 gap, stated explicitly**: a crash between finishing a
message and its commit landing would reprocess that article on restart
and republish duplicate `news.sentiment.scored` events for it (a
duplicate event, not a lost one) -- there is no idempotency/dedup key on
the consumer side. Same trade `news-ingestor`'s own `ArticleDedupService`
accepted for a different failure window; fixing this for real would mean
an outbox-style dedup table, real scope this item doesn't need yet (ADR
0021's anti-gold-plating discipline).

A `news.sentiment.scored` publish failure (e.g. a Kafka outage mid-
processing) is logged and the event is dropped, not retried from a
persisted queue -- same accepted-drop-on-failure precedent
`news-ingestor`'s own `ArticlePublisher` already established for its own
publish path, applied here rather than inventing a different tradeoff for
a comparably low/moderate-throughput producer.

## Metrics

`sentiment_analyzer_*` naming, matching this project's per-service metric
prefix convention (`clinvar_*`, `news_ingestor_*`, `market_data_*`):

- `sentiment_analyzer_articles_consumed_total` -- one per
  `news.article.published` message consumed, regardless of ticker count.
- `sentiment_analyzer_events_published_total` -- one per successfully
  published `news.sentiment.scored` event (one per (article, ticker)
  pair).
- `sentiment_analyzer_publish_errors_total` -- publish attempts that
  failed (dropped, see above).
- `sentiment_analyzer_consume_errors_total` -- consume/parse failures (a
  Kafka-reported error or malformed JSON payload).
- `sentiment_analyzer_scoring_duration_seconds` -- wall-clock time for
  one VADER compound-score computation over a single headline.

## Verified

- **Real VADER scoring, real headlines** (`tests/test_scoring.py`): a
  set of unambiguous, WSJ/MarketWatch-representative positive headlines
  all score `> 0`; a set of unambiguous negative headlines all score
  `< 0`; every score stays within VADER's own `[-1, 1]` bound; 100 real
  scoring calls comfortably finish in well under a second, backing up
  the "sub-millisecond, negligible steady-state CPU" claim with a real
  measurement rather than asserting it from VADER's own reputation.
- **Real event parsing/serialization** (`tests/test_events.py`): both
  plausible Jackson `Instant` wire encodings (numeric, ISO-8601 string)
  parse via `ArticlePublishedEvent.from_json` and round-trip unchanged
  through `SentimentScoredEvent.to_json` as `articlePublishedAt`.
- **Real Kafka consume-then-produce, real fan-out**
  (`tests/test_consumer_integration.py`, `testcontainers[kafka]`, not
  mocked): a real `news.article.published` message naming two tickers
  (`AAPL`, `AMZN`) produces exactly two `news.sentiment.scored` events,
  correctly tagged by ticker, each carrying a real positive VADER score
  for an unambiguously positive real headline; a second, unambiguously
  negative headline produces a single event with a negative score.
  **Honesty note**: this implementation session's own sandbox has no
  Docker daemon (`tests/conftest.py`'s `kafka_bootstrap_servers` fixture
  confirms this live -- `docker.from_env()` fails with a real "no such
  file or directory" connecting to the Docker socket, not a code bug),
  so this test could not actually be *run* in this session; it ran to
  completion as `SKIPPED` with that reason stated (`pytest -rs`), not
  silently omitted. It is real, will run for real in CI (which has
  Docker, same as `clinvar-service`'s/`workload-generator`'s own CI jobs
  already rely on), and needs no code change to do so.
- **Not verified live against the real cluster**: this PR does not
  deploy anything or exercise a real `news-ingestor` -> real Kafka ->
  this service path on the actual k3s cluster. M13's own incremental-
  rollout discipline (this service's `argocd/apps/sentiment-analyzer.yaml`
  in the `platform` repo, deliberately with no `syncPolicy.automated`)
  means a human syncs this after a fresh `kubectl describe node`
  headroom check, separately from this PR merging.

## Configuration (env vars)

| Variable | Default | Notes |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.kafka.svc.cluster.local:9092` | Same convention/default as every other service here. |
| `NEWS_ARTICLE_PUBLISHED_TOPIC` | `news.article.published` | This service's input topic (news-ingestor, backlog #79). |
| `NEWS_SENTIMENT_SCORED_TOPIC` | `news.sentiment.scored` | This service's output topic (backlog #80's AC). |
| `SENTIMENT_ANALYZER_CONSUMER_GROUP` | `sentiment-analyzer` | One consumer group -- a second replica would join it and split partitions, not double-process (see `app/config.py`). |
| `OTLP_COLLECTOR_ENDPOINT` | `http://otel-collector.otel.svc.cluster.local:4318/v1/traces` | **Full URL including `/v1/traces`** -- same convention/reasoning as `clinvar-service`'s `app/telemetry.py`. |

## Running tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/test_scoring.py`/`tests/test_events.py` need nothing beyond the
installed packages. `tests/test_consumer_integration.py` needs a real
Kafka broker -- either point at one you already have running:

```bash
export SENTIMENT_ANALYZER_TEST_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
pytest -q
```

or let `tests/conftest.py`'s `kafka_bootstrap_servers` fixture spin one
up via `testcontainers[kafka]` (needs a reachable Docker daemon -- what
CI actually uses, per `.github/workflows/ci.yml`'s `sentiment_analyzer`
job).
