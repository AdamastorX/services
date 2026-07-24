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
