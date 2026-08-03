# aggregator

Backlog #81 (M13, ADR 0029) -- supersedes/revives #55. A Kafka Streams
topology consuming `stock.price.tick` (#78) and `news.sentiment.scored`
(#80), computing a real windowed rolling-average-sentiment and
price-movement aggregate per ticker, served over a small REST query API
for #82 (`visualizer`).

## Shape

- **Language**: Java/Spring, Kafka Streams (ADR 0029 decision 4) -- this
  project's first Kafka Streams app. Matches #55's own stated preference
  and the existing Spring/Kafka stack; Flink rejected for the same
  "boring, well-understood tools" reasoning every prior tool choice here
  has used.
- **Topology**: two independent tumbling-window aggregations
  (`price-window-store`, `sentiment-window-store`), one per input topic,
  each keyed by ticker -- see `AggregatorTopology`'s own javadoc for why
  this is *not* a topology-level `KTable`-`KTable` join (the two source
  topics have different partition counts, 3 vs 1; a real join would need
  an extra repartition step this milestone's real traffic never needs).
  The "correlate sentiment against price movement" step happens at
  **query time** in `AggregateQueryService`, reading both stores for the
  same ticker/window and combining them into one response.
- **REST API**: `GET /aggregates/{ticker}` and `GET /aggregates` (all
  watchlisted tickers) -- a small, plain query API (ADR 0021's
  anti-gold-plating discipline), matching `api`'s own simplest lookup
  endpoints (`VariantLookupController`). `503` while the state store is
  still restoring after a restart; `404` for a ticker with no data yet
  in the current window (a real, common state -- price ticks are
  continuous, but news/sentiment is sparse).
- **Window**: 15 minutes, tumbling, zero grace. See "ADR 0011, resolved"
  below for why.

## Wire shapes consumed (verified live, not assumed)

This service's own consumer-side records
(`tick/StockPriceTick.java`, `sentiment/SentimentScoredEvent.java`) are
deliberately narrower than the real producers' own records -- only the
fields the windowed aggregation actually needs (`ticker`+`price`,
`ticker`+`score`).

**`stock.price.tick`'s `Instant` fields were empirically verified this
session**, not assumed: a standalone check using the exact dependency
versions and serializer construction `StockPriceTickProducerConfig` uses
(plain `spring-kafka` `JsonSerializer<T>`, `jackson-datatype-jsr310` on
the classpath, `spring.json.add.type.headers=false`) showed `Instant`
fields serialize as a JSON **number**: epoch seconds with a fractional-
nanosecond component (e.g. `1785767400.123456789`), not an ISO-8601
string. This service doesn't need to parse those fields at all (see
"Windowing on Kafka's own record timestamp" below), so this finding
matters only as a fact recorded for whoever builds the next consumer of
that topic, not as something this service depends on.

`news.sentiment.scored`'s `articlePublishedAt`/`scoredAt` fields are
similarly not parsed here, sidestepping `sentiment-analyzer`'s own
documented uncertainty about one Jackson `Instant` wire encoding
(`sentiment-analyzer/app/events.py`'s own docstring) entirely.

## Windowing on Kafka's own record timestamp

Neither store's window assignment reads a business-payload timestamp
field -- Kafka Streams' default timestamp extractor
(`FailOnInvalidTimestamp`) uses each record's own Kafka-level timestamp
(`CreateTime`, i.e. roughly "when the producing service's Kafka client
sent it"). Simpler, matches "boring, well-understood tools", and avoids
needing to trust either producer's `Instant` wire encoding for anything
this service actually does.

## ADR 0011, resolved (not merely re-named)

ADR 0011 deliberately gave Kafka ephemeral storage on this single-broker
dev cluster. #55 named the resulting conflict -- a stream processor whose
durability model is "the changelog topic in Kafka" on a broker whose
topics don't survive a restart -- as real and unresolved. Backlog #81's
own AC requires this ADR to state and justify a real answer here, not
leave it open the way #55 did.

**The answer: a bounded tumbling window (15 minutes, zero grace) small
enough that a full changelog rebuild after a broker/topic loss is an
accepted, measured cost** -- `TimeWindows.ofSizeAndGrace` sets each
windowed store's changelog retention directly to window size + grace, so
15 minutes is not just the aggregation window, it is the real ceiling on
how much history a from-scratch restore ever has to replay.

**Real measured numbers this session** (`StateStoreRecoveryTest`, a real
embedded KRaft broker + two real, sequential `KafkaStreams` instances --
not `TopologyTestDriver`, not a mock):

| | 15-minute window | 60-minute window (4x) |
|---|---|---|
| Real records produced | 45 (5 tickers x (6 ticks + 3 sentiments)) | 180 (5 tickers x (24 ticks + 12 sentiments)) |
| Real changelog records replayed | 5 + 5 (one per ticker per store) | 5 + 5 (one per ticker per store) |
| Real changelog-replay time | ~625ms | ~654ms |
| Real total "kill -> ready to serve correct data" time | ~45.4s | ~45.4s |

**Why changelog volume didn't scale with window size or raw event count
here**: Kafka Streams' own record cache (`state-store-cache-max-size`,
this service's own `application.yml`) deduplicates repeated updates to
the same (ticker, window) key before they ever reach the changelog --
every tick/score for one ticker inside one still-open window collapses to
a single final aggregate record, flushed once (on cache-size limit,
commit interval, or clean shutdown). At this milestone's real traffic
(a handful of events per ticker per window), the changelog for a single
open window is bounded by *ticker count*, not raw event volume or window
size -- a real, useful property, not the naive "N events = N changelog
records" picture. This means restoring THIS milestone's real traffic
shape is cheap regardless of which of these two window sizes is chosen;
a much higher-frequency workload (outside this milestone's real scope,
see ADR 0029) would show real proportionality between window size and
restore cost, since more distinct windows would be retained
simultaneously.

**The dominant real cost is not changelog replay at all: it's Kafka
consumer-group rejoin.** Both measured runs above spent ~44.7 of their
~45.4 total seconds waiting for the *previous* consumer-group member to
be evicted by the group coordinator, before the new member could even be
assigned partitions and start restoring. This is real, expected Kafka
Streams behavior, not a test artifact: Kafka Streams does not proactively
send a `LeaveGroupRequest` on `close()` (a deliberate "sticky membership"
design choice, to avoid a needless full rebalance on every quick
restart) -- the coordinator only evicts the old member once its session
times out (`session.timeout.ms`, defaulting to 45000ms via the classic
consumer group protocol this Kafka Streams version uses). **This is
directly realistic for the real "kill the pod" scenario the AC asks
about**, not a test-only quirk: a hard pod kill (OOMKill, node loss)
never gets to send a graceful leave either, so this ~45-second
group-rejoin stall is the real, honest number a live restart on this
cluster should be expected to cost today, on top of the sub-second real
changelog replay.

**Stated, not silently fixed**: `session.timeout.ms` could be tuned down
(trading faster failover for more sensitivity to a transient GC pause or
network blip falsely evicting a live member) -- a real, deploy-time
decision left to a human, consistent with this module's whole "state the
real number, don't quietly pick one that looks better" discipline.

## Resource sizing (RocksDB / state store memory)

`topology/BoundedRocksDbConfigSetter` bounds each RocksDB instance's
memory explicitly, rather than leaving Kafka Streams' out-of-the-box
RocksDB defaults in place (which reserve on the order of 100+MB *per
store instance*, before a single byte of this milestone's actual tiny
data volume is written -- a well-documented "Kafka Streams RocksDB
memory surprises people" failure mode). This topology opens two
persistent windowed stores; under the bounded config, total RocksDB
memory should stay in the tens of MB, not several hundred. See that
class's own javadoc for the exact numbers and reasoning. **This is a
deliberate ceiling this config enforces, not a measured number** -- real
container RSS has not been sampled against a live deployment (out of
scope here, no live sync attempted, see the PR description).

## #23a is not a real blocker for this measurement

Backlog #81 names #23a (backup/restore, still open, no established
restore discipline anywhere in this project) as a dependency. It is
**not** a real blocker for the state-store-recovery measurement above:
Kafka Streams state-store recovery rebuilds from the changelog topic
already inside Kafka, a completely different mechanism from a Postgres
`pg_dump`/restore. `StateStoreRecoveryTest` proves this directly against
a real embedded broker and real `KafkaStreams` instances -- nothing in
it needed #23a's Postgres backup/restore discipline to exist, and
nothing was faked to make it look otherwise. #23a's actual, real gap
(no documented/automated backup for the two PostgreSQL instances) is
untouched by this item and remains genuinely open.

## Tests

- `AggregatorTopologyTest` -- `TopologyTestDriver`-based correctness
  tests: real windowed price/sentiment aggregation, per-ticker
  independence, window rollover, and that both stores key the same
  ticker into matching window boundaries (the property the query-time
  correlation depends on). Along the way, found and documented a real
  Kafka Streams behavior: `ReadOnlyWindowStore.fetch(key, time)` only
  matches when `time` is the window's own exact start timestamp, not any
  point within it -- verified against a real embedded `KafkaStreams`
  instance too, not just the test driver, since it directly gated a real
  bug in `AggregateQueryService`'s first draft (see that class's own
  javadoc).
- `StateStoreRecoveryTest` -- the real state-store-recovery measurement
  described above, against a real embedded KRaft broker.

## Consumer lag / restoration metrics

`aggregator_state_restore_duration_seconds` (real, per-store,
`StateRestoreMetrics`) and Kafka Streams' own `records-lag`/
`records-lag-max` (bound via `KafkaStreamsMetrics`, `AggregatorStreamsConfig`)
are both real Micrometer metrics on `/actuator/prometheus` -- backlog
#81's AC.
