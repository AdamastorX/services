"""Real ingestion tests (ADR 0019): proves the Postgres tables get
populated and the tabix index is queryable end-to-end, against a real
Postgres (testcontainers, or CLINVAR_TEST_DATABASE_URL override -- see
conftest.py), a real tabix-indexed VCF fixture, and a fake HTTP layer
(tests.helpers.FakeDownloader) so the suite never depends on NCBI's FTP
server being reachable.
"""

from __future__ import annotations

from pathlib import Path

from app import ingestion, repository
from app.vcf_query import query
from tests.conftest import FIXTURES_DIR
from tests.helpers import FakeDownloader, FakeEventProducer


def _plain_bgzip_index(src: Path, dest_dir: Path, name: str) -> tuple[Path, Path]:
    """Builds a bgzipped+tabix-indexed pair from a plain-text fixture and
    returns (vcf_gz_path, tbi_path) without deleting the plain source (so
    the same source fixture can be reused to build multiple release
    directories in one test)."""
    import shutil

    import pysam

    dest_dir.mkdir(parents=True, exist_ok=True)
    plain_copy = dest_dir / f"{name}.vcf"
    shutil.copyfile(src, plain_copy)
    pysam.tabix_index(str(plain_copy), preset="vcf", force=True, keep_original=True)
    return dest_dir / f"{name}.vcf.gz", dest_dir / f"{name}.vcf.gz.tbi"


def test_first_ingestion_populates_tables_and_queryable_tabix(db_conn, refdata_paths, tmp_path):
    source_dir = tmp_path / "source"
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", source_dir, "release1")

    downloader = FakeDownloader(
        source_map={
            "https://example.invalid/clinvar.vcf.gz": vcf_gz,
            "https://example.invalid/clinvar.vcf.gz.tbi": tbi,
        }
    )
    producer = FakeEventProducer()

    release_id = ingestion.ingest(
        db_conn,
        refdata_paths,
        downloader,
        producer,
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )

    active = repository.current_active_release(db_conn)
    assert active is not None
    assert active.release_id == release_id
    assert active.is_active is True
    assert active.variant_count == 2

    # rsID index populated for both fixture variants. find_coordinates_by_rsid
    # returns every matching row (backlog #38), not just one -- both of
    # these rsIDs are unambiguous here, so each resolves to a single-row list.
    resolved = repository.find_coordinates_by_rsid(db_conn, "rs80357906")
    assert resolved == [("17", 43057062, "T", "TG", release_id)]
    resolved2 = repository.find_coordinates_by_rsid(db_conn, "rs80359550")
    assert resolved2 == [("13", 32340300, "GT", "G", release_id)]

    # current symlink points at a queryable tabix file.
    hit = query(refdata_paths.current_vcf_path(), "17", 43057062, "T", "TG")
    assert hit is not None
    assert hit.clinical_significance == "Pathogenic"

    # First-ever ingestion: no previous release to diff against, so no
    # invalidation event is expected to carry any changed keys, but the
    # completion event itself is still published.
    assert len(producer.published) == 1
    event = producer.published[0]
    assert event.new_release_id == release_id
    assert event.previous_release_id is None
    assert event.changed_keys == []


def test_missing_checksum_sidecar_triggers_tbi_rebuild_and_stays_queryable(db_conn, refdata_paths, tmp_path):
    source_dir = tmp_path / "source"
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", source_dir, "release1")

    # No checksums dict entry at all -> fetch_optional_text returns None ->
    # validate_tbi treats it as invalid -> ingestion rebuilds the index
    # via pysam rather than trusting an unverifiable one.
    downloader = FakeDownloader(
        source_map={
            "https://example.invalid/clinvar.vcf.gz": vcf_gz,
            "https://example.invalid/clinvar.vcf.gz.tbi": tbi,
        },
        checksums={},
    )

    release_id = ingestion.ingest(
        db_conn,
        refdata_paths,
        downloader,
        None,
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )

    hit = query(refdata_paths.current_vcf_path(), "13", 32340300, "GT", "G")
    assert hit is not None
    assert hit.rsid == "rs80359550"
    active = repository.current_active_release(db_conn)
    assert active.release_id == release_id


def test_second_ingestion_activates_new_release_and_prunes_old_index_rows(
    db_conn, refdata_paths, tmp_path
):
    source_dir_1 = tmp_path / "source1"
    vcf_gz_1, tbi_1 = _plain_bgzip_index(FIXTURES_DIR / "fixture-invalidation-release-n.vcf", source_dir_1, "n")
    source_dir_2 = tmp_path / "source2"
    vcf_gz_2, tbi_2 = _plain_bgzip_index(FIXTURES_DIR / "fixture-invalidation-release-n2.vcf", source_dir_2, "n2")

    downloader_1 = FakeDownloader(
        source_map={"u://vcf": vcf_gz_1, "u://tbi": tbi_1},
    )
    first_release_id = ingestion.ingest(db_conn, refdata_paths, downloader_1, None, "u://vcf", "u://tbi")

    downloader_2 = FakeDownloader(
        source_map={"u://vcf": vcf_gz_2, "u://tbi": tbi_2},
    )
    producer = FakeEventProducer()
    second_release_id = ingestion.ingest(db_conn, refdata_paths, downloader_2, producer, "u://vcf", "u://tbi")

    assert second_release_id != first_release_id

    active = repository.current_active_release(db_conn)
    assert active.release_id == second_release_id

    # Old release's rsID-index rows are pruned; only the current release's
    # rows remain (ADR 0018/0019: this index only ever needs to answer
    # against the current release).
    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM clinvar_variant_index WHERE clinvar_release_id = %s", (first_release_id,))
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT COUNT(*) FROM clinvar_variant_index WHERE clinvar_release_id = %s", (second_release_id,))
        assert cur.fetchone()[0] == 1

    # Both releases' VCF files still survive on disk (retention keeps
    # current + immediately-previous, since diff computation needs both).
    assert refdata_paths.vcf_path(first_release_id).exists()
    assert refdata_paths.vcf_path(second_release_id).exists()

    # The reclassified CFTR variant is exactly the one changed key.
    assert len(producer.published) == 1
    event = producer.published[0]
    assert event.previous_release_id == first_release_id
    assert event.new_release_id == second_release_id
    assert event.changed_keys == ["variantAnnotation:7:117559600:C:T"]


def test_overlapping_ingestion_is_rejected_not_run_concurrently(db_conn, refdata_paths, tmp_path):
    """A real double-trigger (two manual POSTs hitting the endpoint close
    together) once ran two full VCF scans at the same time in production --
    nothing enforced only one ingestion in flight. Simulates the second
    call arriving while the first is still inside its VCF scan, by having
    the fake downloader itself attempt (and fail) the reentrant call."""
    source_dir = tmp_path / "source"
    vcf_gz, tbi = _plain_bgzip_index(FIXTURES_DIR / "fixture-release-1.vcf", source_dir, "release1")

    reentrant_attempt = {}

    class ReentrantDownloader(FakeDownloader):
        def download(self, url: str, dest: Path) -> str:
            if not reentrant_attempt:
                try:
                    ingestion.ingest(db_conn, refdata_paths, FakeDownloader(source_map=self._source_map), None, url, url)
                except ingestion.ClinVarIngestionAlreadyRunning as exc:
                    reentrant_attempt["error"] = exc
            return super().download(url, dest)

    downloader = ReentrantDownloader(
        source_map={
            "https://example.invalid/clinvar.vcf.gz": vcf_gz,
            "https://example.invalid/clinvar.vcf.gz.tbi": tbi,
        }
    )

    ingestion.ingest(
        db_conn,
        refdata_paths,
        downloader,
        None,
        "https://example.invalid/clinvar.vcf.gz",
        "https://example.invalid/clinvar.vcf.gz.tbi",
    )

    assert "error" in reentrant_attempt
    assert isinstance(reentrant_attempt["error"], ingestion.ClinVarIngestionAlreadyRunning)
