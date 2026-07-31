-- Backlog #53. A subscription names either a specific variant (by the same
-- coordinate-key format clinvar-service's clinvar.ingestion.completed event
-- already uses for changedKeys, e.g. "variantAnnotation:17:43057062:T:TG" --
-- see VariantAnnotationCacheService#key in the api module this format
-- originated in) or a gene symbol -- exactly one of the two, never both,
-- never neither.
--
-- gene_symbol is schema-ready but NOT resolved against incoming events in
-- this pass: clinvar-service's data model (clinvar_variant_index /
-- VariantAnnotation / this event's changedKeys) carries chrom/pos/ref/alt/
-- rsid/clinicalSignificance/reviewStatus only -- no gene symbol anywhere,
-- confirmed by reading clinvar-service/app/schemas.py and api's
-- VariantAnnotation.java before writing this migration. Extracting and
-- indexing ClinVar's VCF GENEINFO field is a real prerequisite change to
-- clinvar-service's own ingestion pipeline, out of scope for this item
-- (ADR 0021: don't build ahead of need) -- recorded here as a stated gap,
-- not silently dropped. See the watchlist-service README and PR
-- description for the same note.
CREATE TABLE subscriptions (
    id            UUID PRIMARY KEY,
    variant_key   VARCHAR(255),
    gene_symbol   VARCHAR(64),
    ntfy_topic    VARCHAR(255) NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT subscriptions_target_xor CHECK (
        (variant_key IS NOT NULL AND gene_symbol IS NULL)
        OR (variant_key IS NULL AND gene_symbol IS NOT NULL)
    )
);

-- Matching subscriptions for a changed variant_key is the hot path on every
-- ingestion event's fan-out (DeliveryResolutionService) -- a partial index
-- (only rows that actually use this column) keeps it small and cheap on a
-- table that will otherwise mix variant- and gene-keyed rows together.
CREATE INDEX idx_subscriptions_variant_key ON subscriptions (variant_key)
    WHERE variant_key IS NOT NULL;

CREATE INDEX idx_subscriptions_gene_symbol ON subscriptions (gene_symbol)
    WHERE gene_symbol IS NOT NULL;
