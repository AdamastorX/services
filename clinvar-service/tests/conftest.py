"""Shared pytest fixtures (ADR 0019 -- real Postgres, not mocked, matching
the Java side's Testcontainers-based testing culture).

``postgres_dsn`` prefers ``CLINVAR_TEST_DATABASE_URL`` if set (lets this
suite run against a plain local/CI-provided Postgres without Docker
available -- this environment's own sandbox has no Docker daemon, so this
override is how the suite was actually exercised while writing it) and
otherwise spins up a real ``testcontainers`` Postgres container, exactly
matching what CI (which does have Docker) will use for this same suite.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import psycopg
import pysam
import pytest

from app.migrator import run_migrations
from app.paths import ClinVarRefdataPaths

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def bgzip_and_index(src_plain_vcf: Path, dest_dir: Path, filename: str = "clinvar.vcf") -> Path:
    """Turns a small, human-readable, checked-in plain-text VCF fixture into
    a bgzipped + tabix-indexed pair -- the same file shape ingestion
    actually deals with. Mirrors the Java side's
    ``ClinVarFixtureSupport.bgzipAndIndex`` for the same reason: tests
    start from a small, diffable, real-data-derived text fixture rather
    than an opaque binary blob.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    plain_copy = dest_dir / filename
    shutil.copyfile(src_plain_vcf, plain_copy)
    pysam.tabix_index(str(plain_copy), preset="vcf", force=True, keep_original=False)
    return dest_dir / f"{filename}.gz"


@pytest.fixture(scope="session")
def postgres_dsn():
    override = os.environ.get("CLINVAR_TEST_DATABASE_URL")
    if override:
        yield override
        return

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as container:
        yield container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture()
def db_conn(postgres_dsn):
    conn = psycopg.connect(postgres_dsn, autocommit=False)
    run_migrations(conn)
    # Isolate each test -- truncate rather than drop/recreate, cheaper and
    # this suite never needs to test the migrations themselves running
    # from empty more than once.
    with conn.cursor() as cur:
        cur.execute("TRUNCATE clinvar_variant_index, clinvar_release CASCADE")
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture()
def refdata_paths(tmp_path) -> ClinVarRefdataPaths:
    return ClinVarRefdataPaths(tmp_path / "refdata")
