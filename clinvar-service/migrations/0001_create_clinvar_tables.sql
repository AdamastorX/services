-- ADR 0019: clinvar-service's own tables, on its own dedicated Postgres
-- instance -- not api's work_items database. Schema is functionally the
-- same shape ADR 0018 originally put in api's Flyway history
-- (V2__create_clinvar_tables.sql), migrated here because this service is
-- now the sole owner/writer of both tables (the whole point of this ADR:
-- no other component reaches into this Postgres instance at all).
--
-- Migration tool: plain numbered .sql files run at startup by
-- app.migrator (see that module's docstring), not Flyway/yoyo/alembic --
-- two tables, a handful of migrations expected ever, boring beats clever
-- here.

CREATE TABLE clinvar_release (
    release_id UUID PRIMARY KEY,
    source_url TEXT NOT NULL,
    file_sha256 CHAR(64) NOT NULL,
    -- Parsed from the VCF's own ##fileDate header at ingestion time, not
    -- file mtime (same ADR 0018 AC, reimplemented here).
    published_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    variant_count BIGINT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT false
);

-- At most one active release at a time -- the Postgres-side half of
-- "readers never see a half-written release" (the filesystem `current`
-- symlink is only flipped after this row's transaction commits, see
-- app/ingestion.py). Old rows are kept forever, not deleted, so a
-- previously served/cached answer's provenance never becomes
-- unresolvable just because a newer release exists.
CREATE UNIQUE INDEX uq_clinvar_release_active ON clinvar_release (is_active) WHERE is_active;

-- rsID -> coordinates lookup table (tabix indexes are position-based;
-- scanning ~250MB per rsID lookup at request time is a non-starter).
-- Deliberately NOT retained across releases -- only the current release's
-- rows are kept (pruned in the same ingestion transaction that activates
-- a new release, app/ingestion.py), since rsID resolution only ever needs
-- to answer against the current release; the actual annotation lookup
-- that follows always re-queries the current release's tabix file
-- regardless of which release originally indexed the rsID.
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
