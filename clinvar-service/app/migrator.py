"""Boring, explicit migration runner (ADR 0019).

No Flyway (this is Python, not the JVM) and no Alembic/yoyo dependency
either -- clinvar-service owns exactly two tables and expects a handful of
migrations over its lifetime, so a numbered-``.sql``-files-in-a-directory
runner with a one-column tracking table is the whole mechanism. Each file
runs once, in filename order, inside its own transaction; a
``schema_migrations`` row is only inserted after that file's statements
commit, so a crash mid-migration never marks a partially-applied file as
done.
"""

from __future__ import annotations

import logging
from pathlib import Path

from psycopg import Connection

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

_ENSURE_TRACKING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def run_migrations(conn: Connection, migrations_dir: Path = MIGRATIONS_DIR) -> list[str]:
    """Applies every not-yet-applied ``*.sql`` file under ``migrations_dir``, in
    sorted (i.e. numeric-prefix) order. Returns the filenames actually applied.
    """
    with conn.cursor() as cur:
        cur.execute(_ENSURE_TRACKING_TABLE_SQL)
        cur.execute("SELECT filename FROM schema_migrations")
        already_applied = {row[0] for row in cur.fetchall()}
    conn.commit()

    applied_now: list[str] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        if path.name in already_applied:
            continue
        sql = path.read_text()
        logger.info("Applying migration %s", path.name)
        with conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s)", (path.name,)
            )
        conn.commit()
        applied_now.append(path.name)

    return applied_now
