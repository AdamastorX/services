"""Plain psycopg queries against clinvar_release / clinvar_variant_index /
clinvar_ingestion_job (ADR 0019 -- narrow data access, no ORM needed;
backlog #54 added the third table, same "plain psycopg" approach)."""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass

import psycopg
from psycopg import Connection

_BATCH_SIZE = 1000


@dataclass(frozen=True)
class ClinVarRelease:
    release_id: uuid.UUID
    source_url: str
    file_sha256: str
    published_date: datetime.date
    ingested_at: datetime.datetime
    variant_count: int
    is_active: bool


def current_active_release(conn: Connection) -> ClinVarRelease | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT release_id, source_url, file_sha256, published_date, "
            "ingested_at, variant_count, is_active "
            "FROM clinvar_release WHERE is_active = true"
        )
        row = cur.fetchone()
    if row is None:
        return None
    return ClinVarRelease(*row)


def insert_pending_release(
    conn: Connection,
    release_id: uuid.UUID,
    source_url: str,
    file_sha256: str,
    published_date: datetime.date,
) -> None:
    """Inserts a new release row, inactive, with a placeholder variant
    count -- inserted *before* building the variant index because
    ``clinvar_variant_index`` rows carry a foreign key to this row's
    ``release_id``, so it must already exist by the time index rows start
    being inserted. ``is_active`` stays false here, so this is invisible
    to ``current_active_release`` and doesn't touch the one-active-row
    partial unique index.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO clinvar_release "
            "(release_id, source_url, file_sha256, published_date, variant_count, is_active) "
            "VALUES (%s, %s, %s, %s, 0, false)",
            (release_id, source_url, file_sha256, published_date),
        )
    conn.commit()


def activate_release(conn: Connection, release_id: uuid.UUID, variant_count: int) -> None:
    """Sets the real variant count and flips this release active, deactivating
    whatever was active before -- the actual Postgres commit that must
    happen before the filesystem ``current`` pointer moves (see
    app/ingestion.py's ordering).
    """
    with conn.cursor() as cur:
        cur.execute("UPDATE clinvar_release SET is_active = false WHERE is_active = true")
        cur.execute(
            "UPDATE clinvar_release SET variant_count = %s, is_active = true WHERE release_id = %s",
            (variant_count, release_id),
        )
    conn.commit()


def insert_variant_index_rows(conn: Connection, rows: list[tuple[str, str, int, str, str, uuid.UUID]]) -> None:
    """Batch-inserts ``(rsid, chrom, pos, ref, alt, release_id)`` tuples."""
    if not rows:
        return
    with conn.cursor() as cur:
        for start in range(0, len(rows), _BATCH_SIZE):
            batch = rows[start : start + _BATCH_SIZE]
            cur.executemany(
                "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                batch,
            )
    conn.commit()


def prune_variant_index_other_than(conn: Connection, release_id: uuid.UUID) -> int:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM clinvar_variant_index WHERE clinvar_release_id <> %s", (release_id,))
        deleted = cur.rowcount
    conn.commit()
    return deleted


def find_coordinates_by_rsid(conn: Connection, rsid: str) -> list[tuple[str, int, str, str, uuid.UUID]]:
    """Resolves an rsID to *every* matching coordinate row via the index
    table -- tabix indexes are position-based, so this is the only
    feasible way to answer an rsID lookup without scanning the whole VCF
    (ADR 0018/0019).

    An rsID mapping to more than one allele/position is a real,
    non-edge-case occurrence in ClinVar's data (backlog #38) -- there is
    no ``LIMIT 1`` here on purpose. Callers must handle the multi-row
    case explicitly rather than assume a single result.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chrom, pos, ref, alt, clinvar_release_id FROM clinvar_variant_index WHERE rsid = %s",
            (rsid,),
        )
        return cur.fetchall()


# --- clinvar_ingestion_job (backlog #54) ------------------------------


@dataclass(frozen=True)
class ClinVarIngestionJob:
    job_id: uuid.UUID
    status: str
    trigger: str
    created_at: datetime.datetime
    started_at: datetime.datetime | None
    finished_at: datetime.datetime | None
    records_scanned: int
    index_rows_built: int
    # The release this job produced (status == 'succeeded') or was in the
    # middle of building when it stopped (cancelled/failed/orphaned) --
    # set as soon as the placeholder clinvar_release row exists, not only
    # on success, so cleanup (delete_pending_release) always knows which
    # row to check. is_active on that row -- not this field -- is what
    # actually distinguishes "live" from "abandoned".
    release_id: uuid.UUID | None
    failure_reason: str | None
    cancel_requested: bool


_JOB_COLUMNS = (
    "job_id, status, trigger, created_at, started_at, finished_at, "
    "records_scanned, index_rows_built, release_id, failure_reason, cancel_requested"
)


class ClinVarIngestionJobAlreadyActive(RuntimeError):
    pass


def create_queued_job(conn: Connection, job_id: uuid.UUID, trigger: str) -> None:
    """Inserts a new job row in ``queued`` state. This -- not the old
    in-process ``threading.Lock`` (services#36, removed by backlog #54)
    -- is now the concurrency guard: ``uq_clinvar_ingestion_job_active``
    (migrations/0002) rejects a second queued/running row with a real
    Postgres unique-violation, which is translated here into
    ``ClinVarIngestionJobAlreadyActive`` so callers don't need to know
    the underlying constraint name. Unlike an in-memory lock, this holds
    even across a pod restart and under a genuine concurrent request.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO clinvar_ingestion_job (job_id, status, trigger) VALUES (%s, 'queued', %s)",
                (job_id, trigger),
            )
        conn.commit()
    except psycopg.errors.UniqueViolation as exc:
        conn.rollback()
        raise ClinVarIngestionJobAlreadyActive(
            "An ingestion job is already queued or running"
        ) from exc


def set_job_attempted_release(conn: Connection, job_id: uuid.UUID, release_id: uuid.UUID) -> None:
    """Records which release this job is (or was) building, as soon as
    the placeholder ``clinvar_release`` row exists -- see the
    ``ClinVarIngestionJob.release_id`` field comment for why this isn't
    deferred until success.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET release_id = %s WHERE job_id = %s",
            (release_id, job_id),
        )
    conn.commit()


def mark_job_running(conn: Connection, job_id: uuid.UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET status = 'running', started_at = now() WHERE job_id = %s",
            (job_id,),
        )
    conn.commit()


def update_job_progress(conn: Connection, job_id: uuid.UUID, records_scanned: int, index_rows_built: int) -> None:
    """Writes the same per-250k-record checkpoint app/ingestion.py already
    logs (backlog #54's own AC: reuse it, don't invent a second progress
    mechanism) -- called at exactly the point ``_build_variant_index_rows``
    already emits its progress log line.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET records_scanned = %s, index_rows_built = %s WHERE job_id = %s",
            (records_scanned, index_rows_built, job_id),
        )
    conn.commit()


def mark_job_succeeded(conn: Connection, job_id: uuid.UUID, release_id: uuid.UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET status = 'succeeded', finished_at = now(), release_id = %s "
            "WHERE job_id = %s",
            (release_id, job_id),
        )
    conn.commit()


def mark_job_failed(conn: Connection, job_id: uuid.UUID, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET status = 'failed', finished_at = now(), failure_reason = %s "
            "WHERE job_id = %s",
            (reason, job_id),
        )
    conn.commit()


def mark_job_cancelled(conn: Connection, job_id: uuid.UUID, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET status = 'cancelled', finished_at = now(), failure_reason = %s "
            "WHERE job_id = %s",
            (reason, job_id),
        )
    conn.commit()


def request_job_cancel(conn: Connection, job_id: uuid.UUID) -> bool:
    """Sets ``cancel_requested`` if -- and only if -- the job is still
    queued/running. Returns whether the flag was actually set (false if
    the job had already reached a terminal state, e.g. a cancel request
    that loses a race against the job finishing naturally).
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE clinvar_ingestion_job SET cancel_requested = true "
            "WHERE job_id = %s AND status IN ('queued', 'running')",
            (job_id,),
        )
        updated = cur.rowcount > 0
    conn.commit()
    return updated


def get_ingestion_job(conn: Connection, job_id: uuid.UUID) -> ClinVarIngestionJob | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_JOB_COLUMNS} FROM clinvar_ingestion_job WHERE job_id = %s", (job_id,))
        row = cur.fetchone()
    if row is None:
        return None
    return ClinVarIngestionJob(*row)


def find_active_jobs(conn: Connection) -> list[ClinVarIngestionJob]:
    """Every job still in ``queued``/``running`` state -- used at startup
    (backlog #54) to find jobs orphaned by whatever killed the previous
    process, since in-memory job state (and any in-process cancel signal)
    cannot have survived the restart.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT {_JOB_COLUMNS} FROM clinvar_ingestion_job WHERE status IN ('queued', 'running')")
        rows = cur.fetchall()
    return [ClinVarIngestionJob(*row) for row in rows]


def delete_pending_release(conn: Connection, release_id: uuid.UUID) -> None:
    """Cleans up the placeholder ``clinvar_release`` row a cancelled (or
    orphaned-and-failed) job's ``insert_pending_release`` call already
    committed before the scan it was inserted ahead of got interrupted.
    Guarded by ``is_active = false`` so this can never touch a release
    that somehow already went live.
    """
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM clinvar_release WHERE release_id = %s AND is_active = false",
            (release_id,),
        )
    conn.commit()
