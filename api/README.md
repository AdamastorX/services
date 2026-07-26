# api

Core business logic. PostgreSQL + Redis backed (M2, issues #12, #14, #15).

## Clinical variant annotation (services#24/#26, ADR 0018; ADR 0019)

A second domain alongside work-items (ADR 0018 — added, not a
replacement): `GET /variants/lookup` accepts either `(chrom, pos, ref,
alt)` or `rsid` (mutually exclusive, 400 on both or neither), returning
ClinVar clinical significance/review status and the `clinvarReleaseId`
that produced the answer. `gnomadAlleleFrequency` is `null` whenever
`clinvar-service` hasn't populated it.

**ADR 0019 rewrote this domain's implementation, not its contract.**
Deploying ADR 0018's original design (services#25/#26) to the real
cluster surfaced the same root cause twice: a shared refdata PVC and a
Postgres credential, both namespace-scoped Kubernetes resources, needed
by two components in two different namespaces. All ClinVar/gnomAD-domain
logic (ingestion, tabix/VCF parsing, the `clinvar_release`/
`clinvar_variant_index` Postgres tables) has been extracted into a new,
standalone Python component, `clinvar-service` — this project's first
non-JVM component, its own namespace, own dedicated Postgres instance,
own PVC. `api` no longer touches a tabix file or either of those tables
directly, in any way:

- `VariantLookupService` calls `clinvar-service`'s internal `GET
  /internal/clinvar/lookup` endpoint over HTTP (`ClinVarServiceClient`, a
  plain Spring `RestClient` — the exact pattern `gateway.
  ApiForwardingController` already uses for `gateway` → `api`, ADR 0010,
  applied to this new internal boundary). Base URL from
  `CLINVAR_SERVICE_URL` (`clinvar-service.base-url` in
  `application.yml`), defaulting to the in-cluster Service DNS name
  `clinvar-service.clinvar.svc.cluster.local`.
- `VariantAnnotationCacheService` is unchanged: the same hand-rolled,
  fail-open Redis cache-aside pattern as `workitem.WorkItemCacheService`
  (ADR 0016) — same `cache.gets{result=hit|miss|error}` metric shape,
  `cache="variant-annotation"` tag — now fronting the HTTP call instead
  of the old direct file/DB read. Coordinate-based lookups check the
  cache first; rsID lookups always call `clinvar-service` (the local
  rsID→coordinates index that used to make a cheap pre-cache-check
  possible is gone under ADR 0019) and populate the cache under the
  resolved coordinate key afterward.
- Unlike work-items, this cache gets *invalidated on write*, not just
  TTL-expired: `VariantInvalidationService` consumes `clinvar-service`'s
  `clinvar.ingestion.completed` event and deletes exactly the Redis keys
  named in its `changedKeys` list — `clinvar-service` computes that diff
  itself (it holds both the old and new release locally) and publishes
  the already-resolved keys, so `api` no longer scans/diffs anything,
  observable via `cache.invalidations{cache="variant-annotation",
  reason="release-changed"}` — a distinct counter from `cache.gets`
  (services#26, ADR 0018; simplified under ADR 0019).

Tests fake `clinvar-service` with a tiny local `HttpServer` (JDK
built-in, no new test dependency — see `VariantLookupIntegrationTest`)
rather than seeding Postgres/filesystem fixtures directly, since `api`
no longer touches either.
