# workers

Kafka consumers, async processing (services#3, ADR 0011).

## What this proves

`api`'s `POST /work-items` publishes a `WorkItem` onto the `work-items`
topic; a `workers` replica consumes it and logs it
(`LoggingWorkItemHandler`). No persistence, no real business logic yet —
this is the project's Kafka delivery-guarantee exercise, not a real domain
feature.

## Wire contract

Producer (`api.workitem.WorkItem`) and consumer (`workers.workitem.WorkItem`)
are separate Java records in separate modules, deliberately not shared via
the `shared` module (ADR 0007). They only agree on the JSON shape:

- `api` disables Kafka's JSON type headers
  (`spring.json.add.type.headers: false`) when producing.
- `workers` deserializes into a fixed target type instead of trusting a
  header (`JsonDeserializer<WorkItem>` constructed with
  `useHeadersIfPresent=false` in `WorkItemConsumerConfig`).

So the two modules stay decoupled from each other's compiled class, closer
to how a real cross-service Kafka contract works (a schema, not a shared
jar).

## Topic

`work-items`, 3 partitions, replication factor 1 (ADR 0011 — RF 1 is
forced by the single-broker dev cluster, not a durability choice). No
partition key yet: messages publish with a `null` key and round-robin
across partitions, since there's no domain entity yet to key on.

## Delivery guarantee

At-least-once. `enable-auto-commit: false` +
`AckMode.MANUAL_IMMEDIATE` (set explicitly on the container factory in
`WorkItemConsumerConfig` — `spring.kafka.listener.ack-mode` alone wasn't
reliably propagated in this Boot 4.1 line): the offset only commits after
`WorkItemListener#onMessage` returns normally, so a crash mid-processing,
or a rebalance from scaling replicas, redelivers the record to whichever
replica next owns that partition instead of silently losing it. `workers`
has no persistent state yet, so idempotency on redelivery isn't
implemented — flagged as a requirement once it does.

A failed processing attempt gets retried up to twice more (3 attempts
total, 1s fixed backoff), then the record is published to
`work-items.DLT` (`DeadLetterPublishingRecoverer`'s default naming)
instead of being dropped.

## Consumer group behaviour

Consumer group id is `workers` (`spring.application.name`). With 3
partitions, up to 3 `workers` replicas get parallel assignment via Kafka's
normal group rebalancing (`spring-kafka`'s default
`CooperativeStickyAssignor`); a 4th+ replica sits idle until a partition
frees up.

This is deployment-time behaviour, not something the embedded-broker unit
test below exercises (it runs a single consumer instance) — proving it
means scaling the `workers` Deployment against the real single-broker
cluster (ADR 0011) and capturing the resulting partition-assignment/
rebalance log lines, not a JUnit test.

## Tests

`WorkItemListenerIntegrationTest` proves the AC end to end against an
embedded broker: a message produced in exactly the wire format `api`
actually uses (JSON value, no key, no type headers) is consumed by
`WorkItemListener` and acknowledged.

## ClinVar ingestion (services#25, ADR 0018)

A second, unrelated domain living in this same module: a weekly (and
manually-triggerable) pipeline that downloads NCBI's GRCh38 ClinVar VCF,
tabix-indexes it, and records release provenance in Postgres. See ADR 0018
in `adamastorx` for the full decision record; this section covers only
what's specific to this module's implementation.

**Why this lives in `workers` and not a Kubernetes Job/CronJob**: ADR 0018
explicitly excludes Kubernetes Jobs from this milestone's scope. A
`CronJob` spawns a `Job` under the hood, which on a literal reading would
violate that same boundary — so ingestion is an in-process `@Scheduled`
trigger (`ClinVarIngestionScheduler`, cron `app.clinvar.ingestion-cron`,
default weekly Monday 03:00) instead, requiring `@EnableScheduling` on
`WorkersApplication`. A manual `POST /internal/clinvar/ingest`
(`ClinVarIngestionController`) exists alongside it for dev/CI use — no
Kubernetes `Service` routes to it (ADR 0009, unchanged for this module),
reachable the same way the actuator probes already are.

**`workers`' first stateful dependencies**: this is the first thing in
`workers` that writes to Postgres (`clinvar_release`,
`clinvar_variant_index` — schema owned by `api`'s Flyway history, see
`api/src/main/resources/db/migration/V2__create_clinvar_tables.sql`'s
header comment for why this module runs with `hibernate.ddl-auto: none`
and no Flyway dependency of its own) and the first thing that writes to
the shared RWX PVC (`app.clinvar.refdata-path`, defaulting to
platform#35's `/data/refdata/clinvar`) — ADR 0018's stated precedent shift
from stateless to stateful, `local-path`-pinned, `replicas: 1` for as long
as this PVC exists.

**Filesystem layout**:

```
{refdata-path}/
  releases/
    {releaseId}/
      clinvar.vcf.gz
      clinvar.vcf.gz.tbi
  current -> releases/{releaseId}   (symlink, atomically re-pointed)
```

`ClinVarRefdataPaths.flipCurrent` only runs after the corresponding
`clinvar_release` Postgres row has committed (`ClinVarReleaseActivationService`)
— readers (this module's own next query, and `api`'s lookup endpoint,
services#24) never see a half-written release. Only the current release
and the immediately-previous one are kept on disk
(`ClinVarRefdataPaths.pruneOtherThan`) — sized for the PVC's stated
double-buffered download-then-swap headroom, and specifically because
services#26's cache invalidation needs both the old and new release's
files available at once to diff changed classifications.

**Checksum validation**: NCBI's published `.tbi` is validated against its
`.md5` sidecar file (`ClinVarTabixIndexer.validate`) before trusting it;
only rebuilt via htsjdk's `IndexFactory.createTabixIndex` if that
validation fails or the sidecar is unavailable (the safe default —
rebuild rather than trust an unverifiable index).

**Kafka**: publishes `ClinVarIngestionCompletedEvent` to
`clinvar.ingestion.completed` once an ingestion is fully committed —
`workers`' first outbound Kafka producer (until now it has only ever
consumed). `api`'s cache-invalidation consumer (services#26) is this
event's only consumer today.

**Tests**: `ClinVarTabixIndexerTest` (checksum validation + rebuild, no
Docker needed) and `ClinVarIngestionServiceIntegrationTest` (full
ingestion flow against Testcontainers Postgres, an embedded broker, and a
small local HTTP server standing in for NCBI) both run against
`src/test/resources/clinvar/fixture-release-1.vcf` — a small, real
subset of ClinVar's GRCh38 VCF (two real pathogenic BRCA1/BRCA2 founder
variants, rs80357906 and rs80359550, fetched and trimmed from a live
download while building this fixture, not fabricated data) rather than
the full ~250MB file.
