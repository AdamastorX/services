# watchlist-service

Backlog #53. Subscription CRUD and guaranteed, idempotent, eventually
delivered fan-out on `clinvar.ingestion.completed` -- the second,
independent consumer of that topic (the first is `api`'s Redis
cache-invalidation listener, ADR 0019; this service shares nothing with it
except the topic).

## Shape

- **Language**: Java/Spring Boot, per ADR 0019's rule -- this is generic
  request/messaging work, not bioinformatics-domain logic, so it stays in
  the language everything else here already uses.
- **Storage**: its own dedicated Postgres instance (`watchlist-postgresql`,
  `watchlist` namespace) -- same single-consumer-per-instance reasoning ADR
  0012/0019 already established, not a shared database across a namespace
  boundary.
- **Delivery guarantee**: ADR 0026 (outbox-table-plus-relay). The Kafka
  listener (`ingestion/ClinVarIngestionListener`) resolves matching
  subscriptions and durably inserts one `PENDING` row per match into the
  `deliveries` table in the same transaction, only acknowledging the Kafka
  offset after that commits. `delivery/NotificationRelay` is a fully
  independent, `@Scheduled` poll loop that reads `PENDING` rows and calls
  ntfy -- decoupled from Kafka delivery entirely, so it resumes on its own
  after a restart with no dependency on a fresh Kafka message.
- **Idempotency**: a `UNIQUE (subscription_id, release_id, variant_key)`
  constraint on `deliveries`, inserted with `ON CONFLICT DO NOTHING` --
  reprocessing the same Kafka message (a real redelivery) is a no-op, not a
  duplicate notification. Proven by
  `DeliveryIdempotencyIntegrationTest`, which publishes the identical event
  twice to a real embedded Kafka broker and asserts a real fake-ntfy HTTP
  server receives exactly one POST.
- **Dead-lettering**: a delivery row that fails `app.delivery.max-attempts`
  times moves to `DEAD_LETTERED` and stops being polled -- one permanently
  broken subscriber's ntfy target never blocks any other subscriber's fan-out,
  because every subscriber has its own delivery row processed independently.
  Proven by `NotificationRelayDeadLetterIntegrationTest`.
- **Notification transport**: the same ntfy.sh topic backlog #21c already
  proved works for Alertmanager (`argocd/apps/prometheus.yaml`), not a
  second channel -- `app.ntfy.default-topic` defaults to that exact topic.

## Known, stated gap: gene-based subscriptions

The AC calls for resolving matching subscriptions "by variant id or gene."
`gene_symbol` is modeled in the schema and accepted by the CRUD endpoint,
but **is not matched against any real event** in this pass: neither
`clinvar_variant_index`, `VariantAnnotation`/`VariantAnnotationResponse`,
nor `clinvar.ingestion.completed`'s `changedKeys` carry a gene symbol
anywhere today (confirmed by reading `clinvar-service/app/schemas.py` and
`api`'s `VariantAnnotation.java` before writing this service, not assumed).
Extracting and indexing ClinVar's VCF `GENEINFO` field is a real
prerequisite change to `clinvar-service`'s own ingestion pipeline --
out of scope here per ADR 0021's "don't build ahead of need" discipline.
Variant-id subscriptions (the data that *is* real today) are fully
implemented and proven live.

## Metrics

- `watchlist_fanout_latency_seconds`: event received -> all matching
  delivery rows durably persisted.
- `watchlist_delivery_latency_seconds`: delivery row created -> marked SENT.
- `watchlist_delivery_attempts_total{outcome=sent|failed|dead_lettered}`.
- `watchlist_delivery_dlq_depth`: current count of `DEAD_LETTERED` rows.

## Endpoints

- `POST /subscriptions` -- body `{"variantKey": "variantAnnotation:{chrom}:{pos}:{ref}:{alt}"}`
  or `{"geneSymbol": "..."}`  (exactly one), optional `"ntfyTopic"` override.
- `GET /subscriptions`, `GET /subscriptions/{id}`, `DELETE /subscriptions/{id}`.
