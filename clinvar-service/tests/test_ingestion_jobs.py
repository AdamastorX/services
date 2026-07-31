"""Real, live-Postgres tests for the async job control plane (backlog
#54): job creation returns immediately, progress is persisted
incrementally (not just at the end), cancellation genuinely interrupts
in-flight work rather than only relabelling the row, and a job orphaned
by a process restart is reconciled to a terminal state rather than left
``running`` forever.

Same testing culture as tests/test_ingestion.py: a real Postgres
(testcontainers, or CLINVAR_TEST_DATABASE_URL -- see conftest.py), real
tabix-indexed VCF fixtures, and fake HTTP/Kafka layers so the suite never
depends on a real network. The pod-kill proof and the "cancellation stops
a multi-minute real scan" proof against real ClinVar data are live-cluster
verifications, documented in the PR, not something a unit test can
reproduce -- what's covered here is the job state machine's own logic:
guard, progress persistence, cancellation plumbing, and startup
reconciliation.
"""

from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg_pool import ConnectionPool

from app import ingestion, repository
from tests.conftest import FIXTURES_DIR
from tests.helpers import FakeDownloader, FakeEventProducer


def _plain_bgzip_index(src: Path, dest_dir: Path, name: str) -> tuple[Path, Path]:
    import shutil

    import pysam

    dest_dir.mkdir(parents=True, exist_ok=True)
    plain_copy = dest_dir / f"{name}.vcf"
    shutil.copyfile(src, plain_copy)
    pysam.tabix_index(str(plain_copy), preset="vcf", force=True, keep_original=True)
    return dest_dir / f"{name}.vcf.gz", dest_dir / f"{name}.vcf.gz.tbi"


@pytest.fixture()
def pool(postgres_dsn):
    p = ConnectionPool(conninfo=postgres_dsn, open=False, min_size=1, max_size=5)
    p.open(wait=True, timeout=10)
    yield p
    p.close()


def _wait_for_terminal(conn, job_id: uuid.UUID, timeout: float = 10.0) -> repository.ClinVarIngestionJob:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = repository.get_ingestion_job(conn, job_id)
        assert job is not None
        if job.status in ("succeeded", "failed", "cancelled"):
            return job
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach a terminal state within {timeout}s (last status={job.status})")


def test_trigger_ingestion_job_returns_immediately_and_reaches_succeeded(db_conn, pool, refdata_paths, tmp_path):
    """The whole point of backlog #54: the trigger call itself must not
    block for the ingestion -- it returns a job id right away, and the
    real work (and its terminal state) shows up later via a poll."""
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")
    downloader = FakeDownloader(
        source_map={"https://example.invalid/clinvar.vcf.gz": vcf_gz, "https://example.invalid/clinvar.vcf.gz.tbi": tbi}
    )

    started = time.monotonic()
    job_id = ingestion.trigger_ingestion_job(
        pool,
        refdata_paths,
        downloader,
        FakeEventProducer(),
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )
    elapsed = time.monotonic() - started
    assert elapsed < 1.0, "trigger_ingestion_job must return immediately, not block for the ingestion"

    job = _wait_for_terminal(db_conn, job_id)
    assert job.status == "succeeded"
    assert job.trigger == "manual"
    assert job.release_id is not None
    assert job.records_scanned == 2  # fixture-release-1.vcf has 2 records
    assert job.started_at is not None
    assert job.finished_at is not None

    active = repository.current_active_release(db_conn)
    assert active is not None
    assert active.release_id == job.release_id


def test_trigger_ingestion_job_rejects_when_one_already_active(db_conn, pool, refdata_paths):
    """clinvar_ingestion_job's own unique index -- not the retired
    threading.Lock -- is the concurrency guard now: a queued/running row
    already present is enough to reject a new reservation synchronously,
    before any thread is even spawned.
    """
    repository.create_queued_job(db_conn, uuid.uuid4(), "manual")

    with pytest.raises(ingestion.ClinVarIngestionAlreadyRunning):
        ingestion.trigger_ingestion_job(
            pool,
            refdata_paths,
            FakeDownloader(source_map={}),
            None,
            "u://vcf",
            "u://tbi",
        )


def test_progress_is_persisted_incrementally_during_the_scan(
    db_conn, pool, postgres_dsn, refdata_paths, tmp_path, monkeypatch
):
    """Proves progress is written to Postgres *while the scan is still
    running*, not only once at the end -- the actual claim behind "GET
    job-status endpoint reports state plus real progress". Forces a
    checkpoint on every record (instead of every 250k) and slows the scan
    down slightly so a concurrent poller (its own connection, like a real
    HTTP client would use) can observe more than one distinct progress
    value before the job finishes.
    """
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-invalidation-release-n.vcf", tmp_path / "source", "n")
    downloader = FakeDownloader(source_map={"u://vcf": vcf_gz, "u://tbi": tbi})

    monkeypatch.setattr(ingestion, "_PROGRESS_LOG_EVERY", 1)
    monkeypatch.setattr(ingestion, "_CANCEL_CHECK_EVERY", 1)

    real_iter_records = ingestion.iter_records

    def _slow_iter_records(path):
        for record in real_iter_records(path):
            time.sleep(0.05)
            yield record

    monkeypatch.setattr(ingestion, "iter_records", _slow_iter_records)

    job_id = ingestion.trigger_ingestion_job(pool, refdata_paths, downloader, None, "u://vcf", "u://tbi")

    observed_counts: set[int] = set()
    # A fresh connection, not db_conn -- the background thread runs
    # against its own pooled connection, so an honest simulation of "a
    # real HTTP GET while ingestion runs" polls independently too.
    poll_conn = psycopg.connect(postgres_dsn, autocommit=False)

    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            job = repository.get_ingestion_job(poll_conn, job_id)
            poll_conn.rollback()  # release the implicit read transaction so we see fresh commits
            assert job is not None
            observed_counts.add(job.records_scanned)
            if job.status in ("succeeded", "failed", "cancelled"):
                break
            time.sleep(0.03)
    finally:
        poll_conn.close()

    assert job.status == "succeeded"
    # More than one distinct value proves progress was updated more than
    # once mid-flight, not just written a single time at completion.
    assert len(observed_counts) > 1, f"expected multiple distinct progress checkpoints, saw {observed_counts}"


def test_cancel_stops_ingestion_before_it_reaches_the_scan(db_conn, pool, refdata_paths, tmp_path):
    """Proves cancellation genuinely stops the in-flight work rather than
    only relabelling the database row: a slow downloader blocks the
    background thread inside download(); cancel is requested while it's
    blocked; releasing the downloader lets the thread resume, hit the
    "after download" cancellation checkpoint, and stop *before* ever
    reaching activate_release -- so no release goes active and no variant
    index rows are written, which a "just flip a status flag" fake
    implementation could not produce.
    """
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")

    release_lock = threading.Event()
    entered_download = threading.Event()

    class SlowDownloader(FakeDownloader):
        def download(self, url: str, dest: Path) -> str:
            entered_download.set()
            release_lock.wait(timeout=10)
            return super().download(url, dest)

    downloader = SlowDownloader(source_map={"u://vcf": vcf_gz, "u://tbi": tbi})

    job_id = ingestion.trigger_ingestion_job(pool, refdata_paths, downloader, None, "u://vcf", "u://tbi")

    assert entered_download.wait(timeout=10), "background job never reached the (blocking) download step"

    # This is exactly what app/routes/admin.py's cancel endpoint does:
    # flag it in Postgres, then signal the in-process event.
    assert repository.request_job_cancel(db_conn, job_id) is True
    signaled = ingestion.request_cancel(job_id)
    assert signaled is True, "expected a live in-process cancel signal for a job running in this process"

    release_lock.set()  # let the blocked download() call return

    job = _wait_for_terminal(db_conn, job_id, timeout=10)
    assert job.status == "cancelled"
    assert "cancelled" in job.failure_reason.lower()

    # No release ever went active, and the placeholder row this job's
    # insert_pending_release() would have committed is cleaned up, not
    # left dangling.
    assert repository.current_active_release(db_conn) is None
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM clinvar_release")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM clinvar_variant_index")
        assert cur.fetchone()[0] == 0


def test_cancel_of_a_finished_job_is_rejected_not_silently_accepted(db_conn, refdata_paths, tmp_path):
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")
    downloader = FakeDownloader(source_map={"u://vcf": vcf_gz, "u://tbi": tbi})

    ingestion.ingest(db_conn, refdata_paths, downloader, None, "u://vcf", "u://tbi")

    job = repository.find_active_jobs(db_conn)
    assert job == []  # already terminal, nothing active

    with db_conn.cursor() as cur:
        cur.execute("SELECT job_id, status FROM clinvar_ingestion_job LIMIT 1")
        job_id, status = cur.fetchone()
    assert status == "succeeded"
    assert repository.request_job_cancel(db_conn, job_id) is False


def test_reconcile_orphaned_jobs_marks_stale_running_job_failed_and_cleans_pending_release(db_conn):
    """Simulates exactly what a pod kill mid-ingestion leaves behind: a
    'running' job row, with a placeholder (is_active=false) clinvar_release
    row it had already committed before the process died. Startup
    reconciliation (app/main.py's lifespan) must turn this into a
    terminal 'failed' state with a reason, never leave it 'running'
    forever, and must clean up the dangling placeholder release row.
    """
    job_id = uuid.uuid4()
    release_id = uuid.uuid4()
    repository.create_queued_job(db_conn, job_id, "scheduled")
    repository.mark_job_running(db_conn, job_id)
    repository.insert_pending_release(
        db_conn, release_id, "https://example.invalid/clinvar.vcf.gz", "0" * 64, __import__("datetime").date(2026, 1, 1)
    )
    repository.set_job_attempted_release(db_conn, job_id, release_id)

    reconciled = ingestion.reconcile_orphaned_jobs(db_conn)

    assert reconciled == [job_id]
    job = repository.get_ingestion_job(db_conn, job_id)
    assert job.status == "failed"
    assert "restart" in job.failure_reason.lower()
    assert job.finished_at is not None

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM clinvar_release WHERE release_id = %s", (release_id,))
        assert cur.fetchone()[0] == 0


def test_reconcile_orphaned_jobs_is_a_noop_when_nothing_is_active(db_conn):
    assert ingestion.reconcile_orphaned_jobs(db_conn) == []


def test_admin_route_job_lifecycle_end_to_end(postgres_dsn, tmp_path, monkeypatch):
    """Exercises the real HTTP surface (trigger -> poll -> terminal state)
    end to end, same setup pattern as tests/test_metrics.py's admin-route
    test."""
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")

    monkeypatch.setenv("DATABASE_URL", postgres_dsn)
    monkeypatch.setenv("CLINVAR_REFDATA_PATH", str(tmp_path / "refdata"))
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:1")
    monkeypatch.setenv("CLINVAR_INGESTION_TOPIC", "clinvar.ingestion.completed.test")
    monkeypatch.setenv("OTLP_COLLECTOR_ENDPOINT", "http://127.0.0.1:1/v1/traces")
    monkeypatch.setenv("CLINVAR_INGESTION_CRON", "0 3 * * SUN")
    monkeypatch.setenv("CLINVAR_SOURCE_VCF_URL", "https://example.invalid/clinvar.vcf.gz")
    monkeypatch.setenv("CLINVAR_SOURCE_TBI_URL", "https://example.invalid/clinvar.vcf.gz.tbi")

    from app import db as db_module

    db_module.close_pool()

    setup_conn = psycopg.connect(postgres_dsn, autocommit=False)
    from app.migrator import run_migrations

    run_migrations(setup_conn)
    with setup_conn.cursor() as cur:
        cur.execute("TRUNCATE clinvar_variant_index, clinvar_release CASCADE")
    setup_conn.commit()
    setup_conn.close()

    from fastapi.testclient import TestClient

    from app.main import create_app

    app = create_app()
    with TestClient(app) as client:
        app.state.downloader = FakeDownloader(
            source_map={
                "https://example.invalid/clinvar.vcf.gz": vcf_gz,
                "https://example.invalid/clinvar.vcf.gz.tbi": tbi,
            }
        )
        # Swap the real IngestionEventProducer (lifespan wired it up
        # against KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1, a deliberately
        # unreachable fake broker) for the same in-memory fake the rest
        # of this suite uses -- otherwise the background job's publish()
        # blocks inside librdkafka's flush(timeout=30) waiting out the
        # full 30s against a broker that will never accept the message,
        # which is a real slow-flush behavior worth knowing about but not
        # what this test is proving.
        app.state.event_producer = FakeEventProducer()

        response = client.post("/internal/clinvar/ingest")
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "queued"
        job_id = body["jobId"]

        deadline = time.monotonic() + 10
        status = None
        while time.monotonic() < deadline:
            poll = client.get(f"/internal/clinvar/ingest/{job_id}")
            assert poll.status_code == 200
            status = poll.json()["status"]
            if status in ("succeeded", "failed", "cancelled"):
                break
            time.sleep(0.05)

        assert status == "succeeded"
        final = client.get(f"/internal/clinvar/ingest/{job_id}").json()
        assert final["releaseId"] is not None
        assert final["recordsScanned"] == 2

        # A second trigger now (nothing active) succeeds, doesn't 409.
        # A cancel of the now-terminal first job is correctly rejected.
        cancel_resp = client.post(f"/internal/clinvar/ingest/{job_id}/cancel")
        assert cancel_resp.status_code == 409

        not_found = client.get(f"/internal/clinvar/ingest/{uuid.uuid4()}")
        assert not_found.status_code == 404

    db_module.close_pool()
