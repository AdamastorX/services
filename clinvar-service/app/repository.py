"""Plain psycopg queries against clinvar_release / clinvar_variant_index
(ADR 0019 -- narrow data access, two tables, no ORM needed)."""

from __future__ import annotations

import datetime
import uuid
from dataclasses import dataclass

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


def find_coordinates_by_rsid(conn: Connection, rsid: str) -> tuple[str, int, str, str, uuid.UUID] | None:
    """Resolves an rsID to coordinates via the index table -- tabix indexes
    are position-based, so this is the only feasible way to answer an
    rsID lookup without scanning the whole VCF (ADR 0018/0019)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT chrom, pos, ref, alt, clinvar_release_id "
            "FROM clinvar_variant_index WHERE rsid = %s LIMIT 1",
            (rsid,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row
