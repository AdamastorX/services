-- backlog #54: async job control plane for ClinVar ingestion.
--
-- Job state persisted here, not in memory -- in-memory state is exactly
-- what a pod restart destroys, which is the whole reason this table
-- exists (see app/ingestion.py). A row here is the durable source of
-- truth for a job's status/progress/outcome across the trigger request,
-- any number of GET polls, and a pod restart mid-run.
--
-- This table's own partial unique index below is now the concurrency
-- guard, replacing services#36's in-process `threading.Lock`
-- (`_ingestion_lock`, removed in this same change): the Lock only ever
-- protected one process's memory and reset to unlocked on every
-- restart -- exactly wrong for the one failure mode this item exists to
-- fix (a pod killed mid-ingestion). Postgres enforcing "at most one
-- queued/running row" survives a restart and is race-free under a real
-- concurrent request in a way an in-memory flag never was.

CREATE TABLE clinvar_ingestion_job (
    job_id UUID PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    trigger TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    -- Progress, reusing the exact per-250k-record checkpoint
    -- app/ingestion.py already logs at (backlog #54's own AC: "reusing
    -- the per-250k-record progress logging already added", not a new
    -- progress mechanism) -- these two columns are just that same
    -- checkpoint also written to Postgres instead of only to stdout.
    records_scanned BIGINT NOT NULL DEFAULT 0,
    index_rows_built BIGINT NOT NULL DEFAULT 0,
    -- The release this job produced (succeeded) or was in the middle of
    -- building when it stopped (cancelled/failed/orphaned) -- set as
    -- soon as the placeholder clinvar_release row exists, not only on
    -- success (app/repository.py's set_job_attempted_release). ON DELETE
    -- SET NULL: a cancelled/orphaned job's abandoned placeholder release
    -- row gets deleted (delete_pending_release), which must not be
    -- blocked by this job row still referencing it -- the job's own
    -- terminal status/failure_reason is the durable record of what
    -- happened, this column is just "which one" and losing that
    -- pointer's target on cleanup is fine.
    release_id UUID REFERENCES clinvar_release (release_id) ON DELETE SET NULL,
    failure_reason TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT false
);

-- At most one non-terminal (queued or running) job at a time -- see the
-- module comment above. `(true)` is a constant expression, the same
-- partial-unique-index idiom `uq_clinvar_release_active` already uses
-- one column over (this table has no single boolean column that means
-- "is the active job", so the predicate itself carries that meaning
-- instead of a column value).
CREATE UNIQUE INDEX uq_clinvar_ingestion_job_active ON clinvar_ingestion_job ((true))
    WHERE status IN ('queued', 'running');

CREATE INDEX idx_clinvar_ingestion_job_status ON clinvar_ingestion_job (status);
CREATE INDEX idx_clinvar_ingestion_job_created_at ON clinvar_ingestion_job (created_at DESC);
