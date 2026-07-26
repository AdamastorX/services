# clinvar-service

Python, standalone FastAPI service (ADR 0019). This project's first
non-JVM component. Owns ClinVar GRCh38 ingestion, its own dedicated
Postgres instance, the internal variant-lookup HTTP endpoint, and
release-diff computation on ingestion, exclusively — no other component
in this project reads its Postgres instance or its tabix files directly.

## Why this exists

ADR 0018 split ClinVar responsibilities across `api`/`workers` (both
JVM). Deploying it for real surfaced the same root cause twice: a PVC and
a Postgres credential Secret are both namespace-scoped Kubernetes
resources, and `api`/`workers` live in different namespaces. ADR 0019
extracts everything ClinVar-specific into one new component, in one
namespace, with its own PVC and its own Postgres instance, so the
namespace-crossing problem structurally can't recur. See
`docs/adr/0019-clinvar-service-python-extraction.md` in the `adamastorx`
repo for the full reasoning, including why Python/`pysam` specifically
(the tool practitioners in this domain actually reach for, same
underlying `htslib` as the `htsjdk` this replaces).

## What this service owns

- **Ingestion**: weekly download of ClinVar's GRCh38 VCF, checksum
  validation of NCBI's published `.tbi` (rebuilding via `pysam.tabix_index`
  only if validation fails), download-into-a-versioned-directory-then-flip-
  a-`current`-pointer (never serves a half-written release), and recording
  each release as a `clinvar_release` Postgres row (`published_date` parsed
  from the VCF's own `##fileDate` header, not file mtime).
- **Two Postgres tables**, on this service's own dedicated instance —
  `clinvar_release` and `clinvar_variant_index` (the rsID -> coordinates
  lookup table; tabix indexes are position-based, so this is the only
  feasible path for an rsID lookup without scanning the whole VCF).
- **`GET /internal/clinvar/lookup`** — see "HTTP contract" below.
- **Release-diff computation on ingestion** — when a new release lands,
  this service (which keeps both the current and immediately-previous
  release's tabix files on disk specifically for this) computes exactly
  which `variantAnnotation:{chrom}:{pos}:{ref}:{alt}` cache keys' clinical
  significance changed, and publishes the full list on Kafka. `api` never
  needs to re-read a tabix file itself to know what to invalidate.

## HTTP contract

`GET /internal/clinvar/lookup` accepts exactly one of two key styles:

- `?chrom={chrom}&pos={pos}&ref={ref}&alt={alt}`
- `?rsid={rsid}`

Both given, or neither given, is `400 Bad Request`. A partial coordinate
set (e.g. only `chrom`) is also `400`. No match for the given key is
`404`.

A match is `200` with this exact body shape (field names/casing match the
Java `api`-side `VariantAnnotation` record this replaces):

```json
{
  "chrom": "17",
  "pos": 43057062,
  "ref": "T",
  "alt": "TG",
  "rsid": "rs80357906",
  "clinicalSignificance": "Pathogenic",
  "clinicalReviewStatus": "criteria_provided,_single_submitter",
  "gnomadAlleleFrequency": null,
  "clinvarReleaseId": "<uuid>"
}
```

**gnomAD scope note, stated explicitly rather than silently skipped**:
`gnomadAlleleFrequency` is always `null` in this version. Wiring an actual
gnomAD chr21/chr22 slice (ADR 0018 originally scoped this as optional for
M5) is deferred out of this PR's scope — the field is kept on the
response shape now specifically so a future change only has to populate
it, never alter the contract.

## Kafka contract

Topic `clinvar.ingestion.completed` (configurable via
`CLINVAR_INGESTION_TOPIC`, same default). Published once per completed
ingestion:

```json
{
  "newReleaseId": "<uuid>",
  "previousReleaseId": "<uuid-or-null-if-first-ingestion>",
  "publishedDate": "2026-07-06",
  "variantCount": 123,
  "ingestedAt": "2026-07-25T12:00:00Z",
  "changedKeys": ["variantAnnotation:17:43057062:T:TG"]
}
```

`api`'s cache-invalidation consumer deletes exactly the Redis keys named
in `changedKeys` — no tabix re-read on `api`'s side, ever.

## Configuration (env vars)

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | *(none)* | Primary way to point at this service's own Postgres, e.g. `postgresql://user:pass@host:5432/clinvar`. Takes precedence over the five `POSTGRES_*` vars below if set. |
| `POSTGRES_HOST` | `localhost` | Used to compose a DSN only if `DATABASE_URL` is unset. |
| `POSTGRES_PORT` | `5432` | ” |
| `POSTGRES_USER` | *(none, required if composing)* | No baked-in default — a real credential has no sane public one (same reasoning as `api`'s `SPRING_DATASOURCE_PASSWORD`). |
| `POSTGRES_PASSWORD` | *(none, required if composing)* | ” |
| `POSTGRES_DB` | `clinvar` | ” |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.kafka.svc.cluster.local:9092` | Same convention/default as `api`/`workers`. |
| `CLINVAR_INGESTION_TOPIC` | `clinvar.ingestion.completed` | ” |
| `OTLP_COLLECTOR_ENDPOINT` | `http://otel-collector.otel.svc.cluster.local:4318/v1/traces` | **Full URL including `/v1/traces`** — passed literally to `OTLPSpanExporter(endpoint=...)`, which does not auto-append the path the way the SDK's own `OTEL_EXPORTER_OTLP_ENDPOINT` env var does. Same env var name and same fully-qualified-URL convention the Java services already use, for the identical reason (see `app/telemetry.py`). |
| `CLINVAR_REFDATA_PATH` | `/data/clinvar` | This service's own PVC mount point — not shared with any other component. |
| `CLINVAR_SOURCE_VCF_URL` | `https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz` | |
| `CLINVAR_SOURCE_TBI_URL` | `https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi` | |
| `CLINVAR_INGESTION_CRON` | `0 3 * * MON` | Standard 5-field cron (APScheduler's `CronTrigger.from_crontab`), not Spring's 6-field-with-seconds syntax. Weekly, Monday 03:00, same off-peak slot ADR 0018 originally picked. |

## Migrations

Plain numbered `.sql` files under `migrations/`, applied once each (in
filename order) at process startup, tracked in a `schema_migrations`
table this service creates and owns itself (see `app/migrator.py`). No
Flyway (this is Python) and deliberately no Alembic/yoyo either — two
tables, a handful of migrations expected over this service's lifetime,
boring beats clever.

## Scheduler

APScheduler's `BackgroundScheduler` with a cron trigger, in-process —
deliberately not a Kubernetes `CronJob` (which spawns a `Job` under the
hood, and this project's milestone boundary excludes Kubernetes Jobs
entirely; see ADR 0018/0019). A manual admin-triggered re-ingestion
endpoint (`POST /internal/clinvar/ingest`) exists alongside it for dev/CI
use, matching the continuity ADR 0018 established for its Java
equivalent.

## Deferred / simplified scope, stated explicitly

- **gnomAD integration**: stubbed. See "HTTP contract" above.
- **Resumable downloads**: ADR 0018's Java `ClinVarDownloadClient`
  supported resuming a partial download via HTTP `Range` requests. This
  Python port does a plain streaming download instead — infrequent
  (weekly), and a failed attempt just retries from scratch on the next
  scheduled run. Can be added back if partial-download retries ever
  become a real operational problem.
- **Full-file diff on every ingestion**: `app/diff.py` builds two
  in-memory dicts (old release, new release) and diffs them, rather than
  a streaming merge-join over two sorted VCFs. Trivially cheap at fixture
  scale; a known place to optimize if it's ever too slow at full
  ~2-3M-record ClinVar scale.

## Running tests

```bash
pip install -r requirements-dev.txt
pytest
```

Uses a real Postgres (via `testcontainers-python`'s Postgres module, same
Testcontainers-based culture the Java side already uses) and real
tabix-indexed VCF fixtures built at test time from small, checked-in
plain-text VCF fixtures (the same BRCA1/BRCA2/CFTR fixture data ADR
0018's Java tests used) — nothing about Postgres or `pysam` is mocked.

If Docker isn't available in your environment, point the suite at an
already-running Postgres instead:

```bash
export CLINVAR_TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/somedb
pytest
```

Kafka publishing is exercised via `tests/helpers.FakeEventProducer` (a
duck-typed capture double), not a live broker — the JSON event shape and
the fact that `ingest()` actually calls `.publish()` with the right
`changedKeys` are both asserted in `tests/test_ingestion.py`, but no
`testcontainers` Kafka module is used here; this environment had no
Docker daemon available to run one, and a real end-to-end Kafka delivery
test would need a live consumer group besides.
