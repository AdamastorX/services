"""Real metrics tests (ADR 0019/0020, observability#15): proves clinvar-
service's Prometheus surface actually reflects real ingestion/lookup
activity -- against a real Postgres (testcontainers, or
CLINVAR_TEST_DATABASE_URL override -- see conftest.py) and a real tabix-
indexed VCF fixture, same as tests/test_ingestion.py and
tests/test_lookup_endpoint.py, not mocked metric objects.

app.metrics' Histogram/Gauge/Counter instances are module-level singletons
registered once per process (see app/metrics.py's docstring), so every
test in this module reads the *current* value and asserts on the *delta*
its own action caused, rather than an absolute value -- other tests in
this same pytest session touch the same global registry.
"""

from __future__ import annotations

import re
import threading
from pathlib import Path

from fastapi.testclient import TestClient
from prometheus_client import generate_latest

from app import ingestion
from app.metrics import INGESTION_IN_PROGRESS
from tests.conftest import FIXTURES_DIR
from tests.helpers import FakeDownloader, FakeEventProducer
from tests.test_lookup_endpoint import client  # noqa: F401 -- reused fixture


def _plain_bgzip_index(src: Path, dest_dir: Path, name: str) -> tuple[Path, Path]:
    import shutil

    import pysam

    dest_dir.mkdir(parents=True, exist_ok=True)
    plain_copy = dest_dir / f"{name}.vcf"
    shutil.copyfile(src, plain_copy)
    pysam.tabix_index(str(plain_copy), preset="vcf", force=True, keep_original=True)
    return dest_dir / f"{name}.vcf.gz", dest_dir / f"{name}.vcf.gz.tbi"


def _metric_value(metric_name: str) -> float:
    """Parses the current value for a label-less metric line straight out
    of a real generate_latest() scrape, rather than reaching into
    prometheus_client's private ._value/._count internals -- proves the
    same text a real Prometheus scrape would see, not an implementation
    detail of the client library.
    """
    body = generate_latest().decode()
    match = re.search(rf"^{re.escape(metric_name)} (\S+)$", body, re.MULTILINE)
    assert match is not None, f"{metric_name} not found in /metrics output:\n{body}"
    return float(match.group(1))


def test_ingestion_duration_histogram_records_a_real_ingestion(db_conn, refdata_paths, tmp_path):
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")
    downloader = FakeDownloader(
        source_map={"https://example.invalid/clinvar.vcf.gz": vcf_gz, "https://example.invalid/clinvar.vcf.gz.tbi": tbi}
    )

    count_before = _metric_value("clinvar_ingestion_duration_seconds_count")
    sum_before = _metric_value("clinvar_ingestion_duration_seconds_sum")

    ingestion.ingest(
        db_conn,
        refdata_paths,
        downloader,
        FakeEventProducer(),
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )

    assert _metric_value("clinvar_ingestion_duration_seconds_count") == count_before + 1
    # A real ingestion against a real (if tiny) fixture always takes a
    # nonzero, measurable amount of wall-clock time -- the sum only ever
    # increases, proving this wraps real work and isn't a stub always
    # recording 0.
    assert _metric_value("clinvar_ingestion_duration_seconds_sum") > sum_before


def test_in_progress_gauge_is_one_during_ingestion_and_zero_after(db_conn, refdata_paths, tmp_path):
    """Simulates observing a scrape *during* a real ingestion the same way
    test_ingestion.py's overlapping-rejection test simulates a concurrent
    trigger: the fake downloader's first call re-enters and reads the
    gauge from inside the critical section, before the outer call
    finishes."""
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")

    observed_during: dict[str, float] = {}

    class ObservingDownloader(FakeDownloader):
        def download(self, url: str, dest: Path) -> str:
            if "during" not in observed_during:
                observed_during["during"] = INGESTION_IN_PROGRESS._value.get()
            return super().download(url, dest)

    assert INGESTION_IN_PROGRESS._value.get() == 0

    downloader = ObservingDownloader(
        source_map={"https://example.invalid/clinvar.vcf.gz": vcf_gz, "https://example.invalid/clinvar.vcf.gz.tbi": tbi}
    )
    ingestion.ingest(
        db_conn,
        refdata_paths,
        downloader,
        None,
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )

    assert observed_during["during"] == 1
    assert INGESTION_IN_PROGRESS._value.get() == 0


def test_in_progress_gauge_resets_to_zero_even_when_ingestion_fails(db_conn, refdata_paths, tmp_path):
    class FailingDownloader(FakeDownloader):
        def download(self, url: str, dest: Path) -> str:
            raise RuntimeError("simulated download failure")

    downloader = FailingDownloader(source_map={})

    try:
        ingestion.ingest(db_conn, refdata_paths, downloader, None, "u://vcf", "u://tbi")
    except ingestion.ClinVarIngestionError:
        pass

    assert INGESTION_IN_PROGRESS._value.get() == 0


def test_rejection_counter_increments_on_overlapping_ingestion(db_conn, refdata_paths, tmp_path):
    """Mirrors test_ingestion.py's overlapping-rejection test but asserts
    on the metric this issue actually adds: a rejection is itself a
    signal (ADR 0020), so it must be counted, not just returned as a 409
    somewhere upstream."""
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")

    reentrant_attempt: dict[str, object] = {}

    class ReentrantDownloader(FakeDownloader):
        def download(self, url: str, dest: Path) -> str:
            if not reentrant_attempt:
                try:
                    ingestion.ingest(db_conn, refdata_paths, FakeDownloader(source_map=self._source_map), None, url, url)
                except ingestion.ClinVarIngestionAlreadyRunning as exc:
                    reentrant_attempt["error"] = exc
            return super().download(url, dest)

    downloader = ReentrantDownloader(
        source_map={"https://example.invalid/clinvar.vcf.gz": vcf_gz, "https://example.invalid/clinvar.vcf.gz.tbi": tbi}
    )

    rejected_before = _metric_value("clinvar_ingestion_rejected_total")

    ingestion.ingest(
        db_conn,
        refdata_paths,
        downloader,
        None,
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )

    assert "error" in reentrant_attempt
    assert _metric_value("clinvar_ingestion_rejected_total") == rejected_before + 1


def test_direct_lock_contention_also_increments_rejection_counter(db_conn, refdata_paths, tmp_path):
    """Same signal, a different (simpler, thread-based) trigger than the
    reentrant-download trick above -- two real threads actually racing for
    _ingestion_lock, closer to the real incident's shape (two independent
    callers, not one call nested inside another)."""
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path / "source", "release1")

    release_lock = threading.Event()
    entered_download = threading.Event()

    class SlowDownloader(FakeDownloader):
        def download(self, url: str, dest: Path) -> str:
            entered_download.set()
            release_lock.wait(timeout=10)
            return super().download(url, dest)

    first = SlowDownloader(
        source_map={"https://example.invalid/clinvar.vcf.gz": vcf_gz, "https://example.invalid/clinvar.vcf.gz.tbi": tbi}
    )

    rejected_before = _metric_value("clinvar_ingestion_rejected_total")

    thread = threading.Thread(
        target=ingestion.ingest,
        args=(db_conn, refdata_paths, first, None, "https://example.invalid/clinvar.vcf.gz", "https://example.invalid/clinvar.vcf.gz.tbi"),
    )
    thread.start()
    assert entered_download.wait(timeout=10)

    try:
        ingestion.ingest(
            db_conn,
            refdata_paths,
            FakeDownloader(source_map={}),
            None,
            "https://example.invalid/clinvar.vcf.gz",
            "https://example.invalid/clinvar.vcf.gz.tbi",
        )
        raised = False
    except ingestion.ClinVarIngestionAlreadyRunning:
        raised = True
    finally:
        release_lock.set()
        thread.join(timeout=10)

    assert raised is True
    assert _metric_value("clinvar_ingestion_rejected_total") == rejected_before + 1


def test_admin_route_409_is_the_same_rejection_the_counter_recorded(postgres_dsn, tmp_path, monkeypatch):
    """Exercises the actual HTTP path (app/routes/admin.py's 409 catch)
    rather than only calling app.ingestion directly, proving the counter
    increments on the real request path the ACs describe, not just a
    unit-level call into the ingestion module. Setup mirrors
    tests/test_lookup_endpoint.py's own ``client`` fixture."""
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

    import psycopg

    from app.migrator import run_migrations

    setup_conn = psycopg.connect(postgres_dsn, autocommit=False)
    run_migrations(setup_conn)
    with setup_conn.cursor() as cur:
        cur.execute("TRUNCATE clinvar_variant_index, clinvar_release CASCADE")
    setup_conn.commit()
    setup_conn.close()

    from app.main import create_app
    from app.paths import ClinVarRefdataPaths

    app = create_app()
    with TestClient(app) as client:
        # Holds the real _ingestion_lock via a direct app.ingestion call in
        # a background thread (independent of the app's own db_pool, same
        # pattern the client fixture uses to pre-populate data) -- only
        # the *one* HTTP call below ever goes through TestClient, avoiding
        # any question of whether TestClient's own transport supports two
        # concurrent in-flight requests from separate threads.
        release_lock = threading.Event()
        entered_download = threading.Event()

        class SlowDownloader(FakeDownloader):
            def download(self, url: str, dest: Path) -> str:
                entered_download.set()
                release_lock.wait(timeout=10)
                return super().download(url, dest)

        background_conn = psycopg.connect(postgres_dsn, autocommit=False)
        background_downloader = SlowDownloader(
            source_map={
                "https://example.invalid/clinvar.vcf.gz": vcf_gz,
                "https://example.invalid/clinvar.vcf.gz.tbi": tbi,
            }
        )
        thread = threading.Thread(
            target=ingestion.ingest,
            args=(
                background_conn,
                ClinVarRefdataPaths(tmp_path / "refdata"),
                background_downloader,
                None,
                "https://example.invalid/clinvar.vcf.gz",
                "https://example.invalid/clinvar.vcf.gz.tbi",
            ),
        )
        thread.start()
        assert entered_download.wait(timeout=10)

        rejected_before = _metric_value("clinvar_ingestion_rejected_total")

        response = client.post("/internal/clinvar/ingest")

        release_lock.set()
        thread.join(timeout=10)
        background_conn.close()

        assert response.status_code == 409
        assert _metric_value("clinvar_ingestion_rejected_total") == rejected_before + 1

    db_module.close_pool()


def test_lookup_duration_histogram_records_a_real_successful_lookup(client):
    count_before = _metric_value("clinvar_lookup_duration_seconds_count")

    response = client.get(
        "/internal/clinvar/lookup", params={"chrom": "17", "pos": 43057062, "ref": "T", "alt": "TG"}
    )

    assert response.status_code == 200
    assert _metric_value("clinvar_lookup_duration_seconds_count") == count_before + 1


def test_lookup_duration_histogram_also_records_a_404(client):
    """The AC is "latency/count ... wrapping the endpoint", not "only
    successful lookups" -- a 404 still took real time to resolve (it
    reached the tabix point-query and came back empty) and should still
    count."""
    count_before = _metric_value("clinvar_lookup_duration_seconds_count")

    response = client.get("/internal/clinvar/lookup", params={"rsid": "rs99999999"})

    assert response.status_code == 404
    assert _metric_value("clinvar_lookup_duration_seconds_count") == count_before + 1


def test_lookup_duration_histogram_also_records_a_400(client):
    count_before = _metric_value("clinvar_lookup_duration_seconds_count")

    response = client.get("/internal/clinvar/lookup")

    assert response.status_code == 400
    assert _metric_value("clinvar_lookup_duration_seconds_count") == count_before + 1


def test_metrics_endpoint_exposes_all_four_new_metrics(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    body = response.text
    for metric_name in (
        "clinvar_ingestion_duration_seconds",
        "clinvar_ingestion_in_progress",
        "clinvar_ingestion_rejected_total",
        "clinvar_lookup_duration_seconds",
    ):
        assert metric_name in body
