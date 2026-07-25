# api

Core business logic. PostgreSQL + Redis backed (M2, issues #12, #14, #15).

## Clinical variant annotation (services#24/#26, ADR 0018)

A second domain alongside work-items (ADR 0018 — added, not a
replacement): `GET /variants/lookup` accepts either `(chrom, pos, ref,
alt)` or `rsid` (mutually exclusive, 400 on both or neither), returning
ClinVar clinical significance/review status and the `clinvarReleaseId`
that produced the answer. `gnomadAlleleFrequency` is always `null` in this
PR — gnomAD population allele frequency is explicitly deferred out of
M5's first pass, see `VariantAnnotation`'s javadoc.

- rsID lookups resolve via `clinvar_variant_index` (Postgres) first, then
  query the current release's tabix-indexed VCF via htsjdk, in-process
  (`ClinVarVcfQueryService`) — not a bcftools/tabix subprocess.
- `VariantAnnotationCacheService` is a Redis cache-aside, the same
  hand-rolled, fail-open pattern as `workitem.WorkItemCacheService`
  (ADR 0016) — same `cache.gets{result=hit|miss|error}` metric shape,
  `cache="variant-annotation"` tag.
- Unlike work-items, this cache gets *invalidated on write*, not just
  TTL-expired: `VariantInvalidationService` consumes `workers`'
  `clinvar.ingestion.completed` event (services#25) and evicts only the
  cached keys whose classification actually changed between the old and
  new release, observable via `cache.invalidations{cache="variant-annotation",
  reason="release-changed"}` — a distinct counter from `cache.gets`
  (services#26, ADR 0018).
- The ClinVar file/refdata layout (`{refdata-path}/current`, `releases/{id}`)
  is written by `workers` (services#25) and only ever read here — see
  `workers/README.md`'s ClinVar section for the full filesystem contract.
- `clinvar_release`/`clinvar_variant_index` schema lives in this module's
  Flyway history (`V2__create_clinvar_tables.sql`) even though `workers`
  is the module that writes those rows during ingestion — see that
  migration's header comment for why schema ownership stays single.

Tests use a small, real subset of ClinVar's GRCh38 VCF (`src/test/resources/clinvar/`)
rather than the full ~250MB file — see `ClinVarVcfQueryServiceTest` and
`VariantLookupIntegrationTest`.
