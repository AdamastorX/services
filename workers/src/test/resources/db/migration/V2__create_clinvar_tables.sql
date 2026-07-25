-- Test-only mirror of api/src/main/resources/db/migration/V2__create_clinvar_tables.sql.
--
-- workers has no Flyway of its own (see that file's header comment --
-- api's Flyway history is the single schema owner). This module's
-- integration tests still need the clinvar_* tables to exist against
-- their own Testcontainers Postgres instance, and workers can't reach
-- across module boundaries to api's main/resources at test time -- so
-- this is a deliberate, test-scoped duplicate, not a second real Flyway
-- history. If the two ever drift, the fix is to update both, or (better,
-- if this bites in practice) promote clinvar_release/clinvar_variant_index
-- mapping into the `shared` module -- tracked as a known duplication,
-- same reasoning as ClinVarRelease.java's own class javadoc.

CREATE TABLE clinvar_release (
    release_id UUID PRIMARY KEY,
    source_url TEXT NOT NULL,
    file_sha256 CHAR(64) NOT NULL,
    published_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    variant_count BIGINT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT false
);

CREATE UNIQUE INDEX uq_clinvar_release_active ON clinvar_release (is_active) WHERE is_active;

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
