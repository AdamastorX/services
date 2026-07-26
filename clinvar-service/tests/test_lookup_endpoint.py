"""Real HTTP test (FastAPI TestClient) proving GET /internal/clinvar/lookup
returns correct data for a known variant, resolves rsID lookups through
the Postgres index, 400s on ambiguous/missing query params, and 404s for
an unknown variant (ADR 0019's exact contract)."""

from __future__ import annotations

import shutil

import pysam
import pytest
from fastapi.testclient import TestClient

from app import ingestion
from tests.conftest import FIXTURES_DIR
from tests.helpers import FakeDownloader


@pytest.fixture()
def client(postgres_dsn, tmp_path, monkeypatch):
    refdata_dir = tmp_path / "refdata"

    monkeypatch.setenv("DATABASE_URL", postgres_dsn)
    monkeypatch.setenv("CLINVAR_REFDATA_PATH", str(refdata_dir))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:1")
    monkeypatch.setenv("CLINVAR_INGESTION_TOPIC", "clinvar.ingestion.completed.test")
    monkeypatch.setenv("OTLP_COLLECTOR_ENDPOINT", "http://127.0.0.1:1/v1/traces")
    # Far in the future relative to any real clock so the weekly scheduled
    # job never actually fires during a test run.
    monkeypatch.setenv("CLINVAR_INGESTION_CRON", "0 3 * * SUN")

    # Reset the app.db module-level pool singleton between tests -- each
    # test's `client` fixture starts a fresh lifespan against (possibly) a
    # differently-scoped tmp_path, and the pool must reconnect against
    # whatever DATABASE_URL this test just set.
    from app import db as db_module

    db_module.close_pool()

    # Pre-populate real data (Postgres rows + a queryable tabix file)
    # using the ingestion pipeline directly, via a connection independent
    # of the app's own pool, before the app itself starts up.
    import psycopg

    source_dir = tmp_path / "source"
    source_dir.mkdir()
    plain_copy = source_dir / "release1.vcf"
    shutil.copyfile(FIXTURES_DIR / "fixture-release-1.vcf", plain_copy)
    pysam.tabix_index(str(plain_copy), preset="vcf", force=True, keep_original=True)
    vcf_gz = source_dir / "release1.vcf.gz"
    tbi = source_dir / "release1.vcf.gz.tbi"

    from app.paths import ClinVarRefdataPaths

    setup_conn = psycopg.connect(postgres_dsn, autocommit=False)
    from app.migrator import run_migrations

    run_migrations(setup_conn)
    with setup_conn.cursor() as cur:
        cur.execute("TRUNCATE clinvar_variant_index, clinvar_release CASCADE")
    setup_conn.commit()

    downloader = FakeDownloader({"u://vcf": vcf_gz, "u://tbi": tbi})
    release_id = ingestion.ingest(
        setup_conn, ClinVarRefdataPaths(refdata_dir), downloader, None, "u://vcf", "u://tbi"
    )
    setup_conn.close()

    from app.main import create_app

    app = create_app()
    with TestClient(app) as test_client:
        test_client.release_id = str(release_id)  # type: ignore[attr-defined]
        yield test_client

    db_module.close_pool()


def test_lookup_by_coordinates_returns_known_variant(client):
    response = client.get("/internal/clinvar/lookup", params={"chrom": "17", "pos": 43057062, "ref": "T", "alt": "TG"})

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "chrom": "17",
        "pos": 43057062,
        "ref": "T",
        "alt": "TG",
        "rsid": "rs80357906",
        "clinicalSignificance": "Pathogenic",
        "clinicalReviewStatus": "reviewed_by_expert_panel",
        "gnomadAlleleFrequency": None,
        "clinvarReleaseId": client.release_id,
    }


def test_lookup_by_rsid_resolves_through_index(client):
    response = client.get("/internal/clinvar/lookup", params={"rsid": "rs80357906"})

    assert response.status_code == 200
    body = response.json()
    assert body["chrom"] == "17"
    assert body["pos"] == 43057062
    assert body["clinicalSignificance"] == "Pathogenic"
    assert body["clinvarReleaseId"] == client.release_id


def test_lookup_404s_for_unknown_coordinate(client):
    response = client.get("/internal/clinvar/lookup", params={"chrom": "1", "pos": 12345, "ref": "A", "alt": "T"})
    assert response.status_code == 404


def test_lookup_404s_for_unknown_rsid(client):
    response = client.get("/internal/clinvar/lookup", params={"rsid": "rs99999999"})
    assert response.status_code == 404


def test_lookup_400s_when_both_key_styles_given(client):
    response = client.get(
        "/internal/clinvar/lookup",
        params={"rsid": "rs80357906", "chrom": "17", "pos": 43057062, "ref": "T", "alt": "TG"},
    )
    assert response.status_code == 400


def test_lookup_400s_when_neither_key_style_given(client):
    response = client.get("/internal/clinvar/lookup")
    assert response.status_code == 400


def test_lookup_400s_for_partial_coordinates(client):
    response = client.get("/internal/clinvar/lookup", params={"chrom": "17"})
    assert response.status_code == 400


def test_lookup_by_unnormalized_coordinates_matches_normalized_form(client):
    """Backlog #39: a query carrying extra redundant shared context around
    an indel (not trimmed to ClinVar's own canonical representation) must
    resolve to the exact same record as the already-normalized query."""
    normalized = client.get(
        "/internal/clinvar/lookup", params={"chrom": "13", "pos": 32340300, "ref": "GT", "alt": "G"}
    )
    # One extra base of shared right-side context -- not left-aligned/
    # trimmed the way ClinVar's own VCF represents this deletion.
    unnormalized = client.get(
        "/internal/clinvar/lookup", params={"chrom": "13", "pos": 32340300, "ref": "GTA", "alt": "GA"}
    )

    assert normalized.status_code == 200
    assert unnormalized.status_code == 200
    assert unnormalized.json() == normalized.json()
    assert normalized.json()["rsid"] == "rs80359550"


def test_lookup_by_rsid_ambiguous_returns_409_naming_all_candidates(client, postgres_dsn):
    """Backlog #38: an rsID mapping to more than one real ClinVar record is
    a genuine ambiguity -- it must be reported loudly (409), never
    silently resolved to whichever index row happened to come back
    first."""
    import psycopg

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT clinvar_release_id FROM clinvar_variant_index WHERE rsid = %s LIMIT 1",
                ("rs80357906",),
            )
            (release_id,) = cur.fetchone()
            # Seed a second index row for the *same* rsID pointing at the
            # other real fixture variant -- both now have a real VCF hit.
            cur.execute(
                "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                ("rs80357906", "13", 32340300, "GT", "G", release_id),
            )

    response = client.get("/internal/clinvar/lookup", params={"rsid": "rs80357906"})

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "rs80357906" in detail
    assert "17:43057062:T:TG" in detail
    assert "13:32340300:GT:G" in detail


def test_lookup_by_rsid_with_stale_extra_index_row_still_resolves(client, postgres_dsn):
    """A second index row for the same rsID that does *not* correspond to a
    real VCF record (e.g. bogus/stale coordinates) must not turn an
    otherwise-unambiguous lookup into a false ambiguity -- only rows that
    resolve to a real ClinVar hit count as candidates."""
    import psycopg

    with psycopg.connect(postgres_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT clinvar_release_id FROM clinvar_variant_index WHERE rsid = %s LIMIT 1",
                ("rs80357906",),
            )
            (release_id,) = cur.fetchone()
            cur.execute(
                "INSERT INTO clinvar_variant_index (rsid, chrom, pos, ref, alt, clinvar_release_id) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                ("rs80357906", "1", 999999, "A", "T", release_id),
            )

    response = client.get("/internal/clinvar/lookup", params={"rsid": "rs80357906"})

    assert response.status_code == 200
    assert response.json()["chrom"] == "17"
    assert response.json()["pos"] == 43057062
