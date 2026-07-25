-- services#25 (ADR 0018): ClinVar release provenance + rsID lookup index.
--
-- Schema owned by `api` (ADR 0012's Flyway convention -- one Flyway
-- history, one migration owner), even though `workers` is the module that
-- actually writes these rows during ingestion. `workers` connects to this
-- same Postgres database with plain JPA (hibernate.ddl-auto: none, no
-- Flyway dependency of its own) rather than running a second, independent
-- Flyway history against the same schema -- two Flyway owners racing
-- migrations against one schema_history table is the kind of thing that
-- works in dev and corrupts state under real concurrent startup. See
-- workers/README.md's ClinVar section for the full reasoning.

CREATE TABLE clinvar_release (
    release_id UUID PRIMARY KEY,
    source_url TEXT NOT NULL,
    file_sha256 CHAR(64) NOT NULL,
    -- Parsed from the VCF's own ##fileDate header at ingestion time, not
    -- file mtime (ADR 0018's explicit AC -- mtime reflects when *this
    -- app* downloaded the file, not when NCBI actually cut the release).
    published_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    variant_count BIGINT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT false
);

-- At most one active release at a time. This is the Postgres-side half of
-- ADR 0018's "readers never see a half-written release" guarantee -- the
-- filesystem `current` symlink (workers.clinvar.ClinVarRefdataPaths) is
-- only flipped after the row that sets is_active=true here has committed,
-- so a reader following either signal always lands on a fully-ingested
-- release, never a partial one. Old rows are kept forever, not deleted, on
-- a re-ingestion (only is_active flips) -- provenance of a previously
-- served/cached answer must not become unresolvable just because a newer
-- release exists (ADR 0018 AC).
CREATE UNIQUE INDEX uq_clinvar_release_active ON clinvar_release (is_active) WHERE is_active;

-- Postgres lookup table resolving rsID -> coordinates (tabix indexes are
-- position-based; scanning ~250MB per rsID lookup at request time is a
-- non-starter -- ADR 0018). Populated during ingestion
-- (ClinVarVariantIndexBuilder). Deliberately NOT retained across releases
-- the way clinvar_release rows are: at ~3M rows/release and a weekly
-- refresh, keeping every historical release's index would grow this table
-- unbounded for no benefit -- rsID resolution only ever needs to answer
-- against the *current* release (the actual annotation lookup that
-- follows always re-queries the current release's tabix file regardless
-- of which release originally indexed the rsID). Rows for a release other
-- than the new current one are pruned in the same ingestion transaction
-- that activates it (ClinVarIngestionService).
CREATE TABLE clinvar_variant_index (
    id BIGSERIAL PRIMARY KEY,
    rsid TEXT NOT NULL,
    chrom TEXT NOT NULL,
    pos INTEGER NOT NULL,
    ref TEXT NOT NULL,
    alt TEXT NOT NULL,
    clinvar_release_id UUID NOT NULL REFERENCES clinvar_release (release_id)
);

CREATE INDEX idx_clinvar_variant_index_rsid ON clinvar_variant_index (rsid);
CREATE INDEX idx_clinvar_variant_index_release ON clinvar_variant_index (clinvar_release_id);
