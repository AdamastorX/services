# news-ingestor

Backlog #79 (M13, ADR 0029). Polls WSJ Markets + MarketWatch RSS on a fixed
cadence, matches article title/summary against a fixed watchlist of ticker
symbols plus a small alias list, and publishes `news.article.published`
only for matched articles.

## Shape

- **Language**: Java/Spring Boot, per ADR 0019's rule -- RSS polling plus
  substring matching is generic HTTP-poll-plus-Kafka-producer messaging
  work, the same shape `api`/`workers`/`watchlist-service` already are, no
  domain-specific-tooling reason to leave Java.
- **Storage**: none. Deliberately stateless -- no dedicated Postgres
  instance, unlike `api`/`watchlist-service`. Dedup state is a small,
  bounded, in-memory LRU (`ArticleDedupService`); see its javadoc for the
  accepted v1 gap (a pod restart forgets recently-seen articles).
- **Sources** (ADR 0029, real comparisons against CNBC/Reuters/Yahoo,
  each verified live and found broken/blocked at the time): WSJ Markets
  (`feeds.content.dowjones.io/public/rss/RSSMarketsMain`) and MarketWatch
  top stories (same domain, `/mw_topstories`). Both re-verified live
  (real HTTP 200, real dated content) during this implementation session,
  not assumed from the ADR alone -- RSS feeds are exactly the kind of
  thing that silently changes.
- **Matching**: case-insensitive substring/keyword matching
  (`TickerMatcher`) against the fixed watchlist + alias list in
  `application.yml` (`app.watchlist.tickers`) -- explicitly not NER/NLP
  (ADR 0021's anti-gold-plating discipline, restated directly in this
  backlog item's own AC).
- **Dedup**: by article `guid` (falling back to `link` if a feed ever
  omits one) -- `ArticleDedupService`, a bounded in-memory LRU.
- **Publishing**: `news.article.published` only for articles matching at
  least one watchlist ticker -- a non-matching article is dropped, not
  forwarded, a stated design choice. Publish failures (e.g. a Kafka
  outage during a poll cycle) are logged and the article is dropped, not
  retried from a persisted queue -- this service has no outbox table (see
  `ArticlePublisher`'s javadoc for the tradeoff and what the fix would be
  if it ever matters in practice).
- **Feed-unreachable handling**: each feed's fetch+parse runs in its own
  try/catch inside `FeedPoller#pollAllFeeds` -- a failure is logged,
  counted (`news_ingestor_feed_poll_total{outcome="unreachable"}`), and
  skipped; the scheduler thread is never killed by one bad feed, and the
  other feed in the same cycle is unaffected. Proven live against a real
  closed TCP port, not asserted from the code shape alone --
  `FeedPollerIntegrationTest#unreachableFeedIsSkippedNotCrashed`.

## Watchlist

`AAPL`/`MSFT`/`GOOGL`/`AMZN`/`TSLA` -- the same default backlog #78
(`market-data-ingestor`)'s own AC names. No `market-data-ingestor` PR/module
existed in this repo at the time this was built, so there was no landed,
concrete list to coordinate against; this is the same reasonable default
the backlog item itself suggests for both services, not an independent
guess. Reconcile if #78 lands a different final list.

## Verified live (this implementation session, 2026-08-02)

- Both feeds re-fetched directly: real HTTP 200, real dated articles
  (headlines mentioning Apple/Amazon/Microsoft present in the live
  response, confirmed the matching pipeline has real signal to find, not
  just a synthetic fixture).
- `FeedPollerIntegrationTest` runs the real `FeedPoller` against a real
  local HTTP server (JDK `HttpServer`, not a mocked `RssFeedClient`) and a
  real embedded Kafka broker:
  - a matching article produces a real `news.article.published` event
    within one poll cycle, consumed by a real `Consumer`;
  - polling the identical feed body twice publishes exactly once (dedup
    by guid);
  - a feed pointed at a real closed TCP port is logged and skipped, not
    thrown, and the other feed in the same cycle still succeeds (proven
    via the real `news_ingestor_feed_poll_total` counters, not inferred).

## Metrics

- `news_ingestor_feed_poll_total{outcome=succeeded|unreachable}`
- `news_ingestor_articles_total{outcome=matched|published|dropped_no_match|skipped_duplicate}`

## Topic

`news.article.published`, provisioned declaratively via the Kafka Helm
chart's own `provisioning.topics` list (`platform/argocd/apps/kafka.yaml`)
-- the same, already-established mechanism `work-items` uses (ADR 0011),
not a second, hand-written provisioning path.
