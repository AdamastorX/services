# market-data-ingestor

Backlog #78 / ADR 0029 (M13). Subscribes to Finnhub's free real-time trade
websocket for a fixed watchlist and republishes each real trade tick as a
`stock.price.tick` Kafka event.

## Shape

- **Language**: Java/Spring Boot, per ADR 0019's rule -- generic
  websocket-client-plus-Kafka-producer messaging work, no domain-specific
  tooling need that would earn Python.
- **Storage**: none. The watchlist is fixed config (`app.market-data.tickers`,
  `application.yml`); the last-tick-per-ticker map behind the stale-feed
  gauge is happily rebuilt from live traffic after any restart. No Postgres,
  no Flyway, unlike `api`/`watchlist-service`.
- **Websocket client**: `java.net.http.WebSocket` (JDK built-in since Java
  11), not a third-party library -- see `pom.xml`'s own comment.
- **Kafka producer**: a typed `KafkaTemplate<String, StockPriceTick>`
  (`tick/StockPriceTickProducerConfig`), keyed by ticker so all of one
  ticker's ticks stay on the same partition (ordering, if this service is
  ever scaled to more than one producer).

## The real watchlist

`AAPL`, `MSFT`, `GOOGL`, `AMZN`, `TSLA` -- the AC's own suggested five.
All large-cap, all Nasdaq/NYSE, all heavily traded enough that the
stale-feed threshold below is a meaningful signal, not noise.

## Reconnect-and-resume (the AC's own live-proof requirement)

`finnhub/FinnhubWebSocketClient` funnels three independent failure paths
into the same `connect()`:

1. `onClose`/`onError` -- the peer or the JDK's transport reports the
   connection is gone.
2. A 10s watchdog -- backstop for a silent, half-open connection: sends an
   RFC 6455 ping if idle past `app.finnhub.ping-interval` (any compliant
   peer must pong), force-reconnects if idle past `app.finnhub.idle-timeout`
   with nothing back at all.
3. `admin.ReconnectController`'s `POST /internal/market-data/force-reconnect`
   -- an operational/test hook that calls the exact same
   `abort()`-then-reconnect path, used to prove this AC live. See that
   class's own javadoc for why an admin-triggered abort, not a
   NetworkPolicy or an in-pod `ss --kill`, is the real mechanism: the
   runtime image has neither `ss`/`iproute2` nor `CAP_NET_ADMIN`, and this
   cluster's flannel CNI (k3s default) does not enforce `NetworkPolicy` --
   a policy meant to block egress to Finnhub would be a silent no-op, worse
   than not testing at all if trusted as a real block.

Every path re-subscribes the full watchlist on the new connection
(`subscribeAll`) -- "resumes subscriptions," not just a bare reconnect.

**Live proof (this item's PR description has the full transcript):** with
the service running against the real cluster and a live consumer attached
to `stock.price.tick`, `POST /internal/market-data/force-reconnect` was
called, ticks stopped, `market_data_websocket_reconnects_total` incremented
and the logs showed a fresh `subscribe` for all 5 tickers, and new ticks
resumed appearing on the live consumer within `app.finnhub.reconnect-delay`
-- no pod restart.

## Market-hours logic (the AC's own named, accepted gap)

`observability/MarketHoursService`: Monday-Friday, 09:30-16:00
America/New_York (`ZoneId` handles the EST/EDT transition automatically).
**Deliberately does not model US market holidays or early closes** -- a
real market holiday on a weekday still reads as "market hours," so a
watchlisted ticker going silent that whole session would (falsely) read as
stale. A holiday calendar is real, ongoing maintenance for a failure mode
this project's no-gold-plating discipline (ADR 0021) doesn't justify for a
personal portfolio project's alerting -- an honestly-stated v1 gap, not a
silent one.

## REST-poll fallback (market-closed supplement, not a replacement)

`aggregator` (backlog #81) windows `stock.price.tick` into 15-minute
tumbling aggregates with no history -- if no tick lands in the current
window, the ticker shows no data at all. During real US market hours
that's fine (the websocket pushes ticks constantly). Outside market hours
-- most of the day, every evening, every weekend -- Finnhub's websocket is
connected but silent (correct, expected behavior, not a bug: see "Market-
hours logic" above), so `stock.price.tick` gets nothing at all, and
`aggregator`'s output (and therefore `visualizer`) sits empty most of the
time.

`finnhub/FinnhubQuotePoller` is the project owner's own explicitly-
confirmed "reasonable middle ground": a second, independent `@Scheduled`
task, alongside `FinnhubWebSocketClient`, not a replacement for it. Keeps
the real-time websocket as the primary, free, push-based source during
actual market hours (untouched by this poller), and supplements it with a
deliberately low-frequency REST poll of Finnhub's real `GET
/api/v1/quote?symbol={ticker}&token={key}` endpoint -- the same real
endpoint shape and the same real `FINNHUB_API_KEY` the websocket path
already authenticates with -- so there is always a reasonably fresh real
price available even when the market is closed.

- **Interval: 30 minutes** -- the real number the project owner explicitly
  confirmed. Runs unconditionally, 24/7, not gated on market hours:
  simpler than adding conditional logic, and harmless redundancy with the
  websocket during market hours (see the rate-limit math below for why
  running around the clock is still trivially cheap).
- **Wire contract: unchanged.** One `StockPriceTick` per ticker per poll,
  published through the exact same `StockPriceTickPublisher` onto the
  exact same `stock.price.tick` topic the websocket path uses -- no
  second topic, no second contract. `volume` has no real per-quote
  equivalent from this REST endpoint (unlike a real trade tick): this
  poller publishes `BigDecimal.ZERO`, not `null` -- `StockPriceTick`'s
  `volume` field is a non-null `BigDecimal` everywhere else in this
  codebase (every real trade tick carries a real trade size), and `null`
  would make this the first nullable value this wire contract has ever
  carried, a latent NPE risk for any future consumer. `ZERO` is not
  literally accurate (no zero-volume trade actually happened) -- a
  deliberate, honestly-stated v1 modeling gap; `aggregator` doesn't
  currently do any arithmetic over `volume` at all, so this has no real
  effect on `aggregator`'s own output today (see `FinnhubQuotePoller`'s
  own javadoc for the full reasoning).
- **Rate-limit math (Finnhub free tier: 60 API calls/minute):** one poll
  cycle makes exactly one REST call per watchlisted ticker -- 5 calls per
  cycle, every 30 minutes, i.e. 10 calls/hour on average. Even in the
  worst case (all 5 landing in the same wall-clock second) that's 5 calls
  against a 60-calls/minute budget, under 10% of one minute's allowance --
  trivially, and deliberately, nowhere near hammering the API.
- **Per-ticker failure handling:** each ticker's fetch runs inside its own
  try/catch in `pollAllTickers()`, the same "one failure doesn't take down
  the whole cycle" discipline `news-ingestor`'s own `FeedPoller` already
  established in this repo -- a real HTTP error (including a real 429
  rate-limit response), a connect/read timeout, or an unusable response
  (Finnhub's own `c:0` "no quote for this symbol" shape) is logged and
  skipped for that ticker only; the scheduler thread and the other four
  tickers in the same cycle are unaffected.
- **Deliberately does NOT feed `StaleFeedMetrics`:** that gauge alerts on
  the websocket going silent *during real market hours* -- a real
  incident. If this poller's ticks counted as "the feed is alive," a
  genuinely dead websocket during market hours could be masked for up to
  30 minutes by this fallback, defeating the alert's purpose.
  `StaleFeedMetrics` stays wired to the websocket path only.

### REST-poll fallback metrics

- `market_data_quote_poll_succeeded_total` -- real Finnhub `/quote` calls
  that returned a usable price for one watchlisted ticker.
- `market_data_quote_poll_failed_total` -- real Finnhub `/quote` calls
  that failed, or returned no usable price, for one watchlisted ticker.
- `market_data_quote_ticks_published_total` -- ticks published to
  `stock.price.tick` via this REST-poll path specifically (a subset of
  `market_data_ticks_published_total`, which also counts the websocket
  path).

### REST-poll fallback config

`app.finnhub-quote-poll.quote-uri` (default
`https://finnhub.io/api/v1/quote`), `app.finnhub-quote-poll.interval-ms`
(default `1800000`, 30 minutes), `app.finnhub-quote-poll.initial-delay-ms`
(default `60000`) -- `application.yml`. The real Finnhub API key is reused
from `app.finnhub.token` (`FINNHUB_API_KEY`), not duplicated.

## Stale-feed metric

`observability/StaleFeedMetrics` exposes, per watchlisted ticker:

- `market_data_seconds_since_last_tick{ticker="AAPL"}` -- always live.
- `market_data_stale_feed{ticker="AAPL"}` -- 1 if no tick within
  `app.market-data.stale-threshold` (5 minutes) **during real US market
  hours**, 0 otherwise. The market-hours check is baked into the gauge's
  own value, application-side (matches `watchlist_delivery_dlq_depth`'s own
  "the app computes real state, the alert just reads a threshold" shape) --
  after-hours/weekend silence is never reported stale.
  `platform/argocd/apps/prometheus.yaml`'s `MarketDataStaleFeed` alert
  rule keys directly on this gauge.

## Latency

`market_data_tick_publish_latency` (a Micrometer `Timer`,
`_seconds_bucket` series on `/actuator/prometheus`): receipt (websocket
`onText`) to Kafka `send()` completion. AC bound: under 2s.

## Reconnect/publish metrics

- `market_data_websocket_reconnects_total` -- successful (re)connections,
  including the first at startup.
- `market_data_websocket_connect_failures_total`
- `market_data_ticks_published_total{ticker=...}` /
  `market_data_ticks_publish_failed_total`

## Topic

`stock.price.tick` -- provisioned the same way `work-items` is
(`platform/argocd/apps/kafka.yaml`'s Bitnami chart's declarative
`provisioning.topics` Job, not a hand-run `kafka-topics.sh --create`; see
that file's own comment on why `auto.create.topics.enable: false` forces
topic creation through the Job). 3 partitions, RF=1 (this single-broker
dev cluster, ADR 0011), keyed by ticker.

## Tests

- `observability/MarketHoursServiceTest`, `observability/StaleFeedMetricsTest`:
  deterministic, fixed-`Instant`/`Clock` proofs of the AC's positive
  (stale during market hours) and negative (never stale after-hours/
  weekend) cases.
- `finnhub/FinnhubMessageParsingTest`: Finnhub's real documented wire
  shapes (trade/ping/error) parse correctly.
- `tick/StockPriceTickPublisherIntegrationTest`: a tick handed to the
  publisher really lands on `stock.price.tick` (embedded broker) with the
  exact AC'd shape, keyed by ticker, inside the 2s bound.
- `finnhub/FinnhubQuoteParsingTest`: Finnhub's real documented REST
  `/quote` response shape (including the `c:0` "no quote for this symbol"
  shape) parses correctly.
- `finnhub/FinnhubQuotePollerIntegrationTest`: the REST-poll fallback
  proven end to end against a real local HTTP server (`com.sun.net.httpserver.HttpServer`,
  the same pattern `news-ingestor`'s own `FeedPollerIntegrationTest` uses)
  and a real (embedded) Kafka broker -- a real quote response produces a
  real published `StockPriceTick` on `stock.price.tick` for every
  reachable ticker, and a real HTTP 500 for exactly one ticker (simulating
  a Finnhub rate-limit/error response) is logged and skipped without
  stopping the other four tickers in the same poll cycle.
- The full Finnhub-to-Kafka path and the reconnect behavior are verified
  live against the real cluster, not in CI -- `app.finnhub.auto-connect`
  defaults `false` in every test's properties specifically so CI never
  depends on reaching a real external vendor (see
  `FinnhubWebSocketClient`'s own javadoc on that flag).
