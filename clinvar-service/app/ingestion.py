"""Orchestrates one full ClinVar ingestion (ADR 0019, reimplemented from
ADR 0018's Java ``ClinVarIngestionService``).

Ordering is the whole point of this module (ADR 0018's "readers never see
a half-written release", carried over unchanged): everything that can
fail happens against the *new* release's own private directory first;
only ``activate_release`` touches anything a reader could already be
looking at (the ``clinvar_release`` table), and the filesystem ``current``
symlink -- the other thing readers actually consult -- only moves after
that transaction has committed. A failure at any earlier step leaves
``current`` pointing exactly where it did before this function was ever
called.
"""

from __future__ import annotations

import datetime
import logging
import threading
import uuid
from pathlib import Path

from psycopg import Connection

from app import repository
from app.diff import compute_changed_keys
from app.download import Downloader, validate_tbi
from app.kafka_producer import IngestionCompletedEvent, IngestionEventProducer
from app.metrics import INGESTION_DURATION_SECONDS, INGESTION_IN_PROGRESS, INGESTION_REJECTED_TOTAL
from app.paths import ClinVarRefdataPaths
from app.vcf_query import iter_records, read_published_date, rebuild_tabix_index

logger = logging.getLogger(__name__)

_PROGRESS_LOG_EVERY = 250_000

# Guards both the admin-triggered and the scheduled entry point (app/main.py
# calls the same `ingest()` from each) -- a real double-ingestion ran
# concurrently in production once (two overlapping manual triggers, each
# doing its own full VCF scan at the same time) before this existed.
_ingestion_lock = threading.Lock()


class ClinVarIngestionError(RuntimeError):
    pass


class ClinVarIngestionAlreadyRunning(RuntimeError):
    pass


def _build_variant_index_rows(
    vcf_path: Path, release_id: uuid.UUID
) -> tuple[list[tuple[str, str, int, str, str, uuid.UUID]], int]:
    """Only variants that carry an RS (dbSNP rs-number) are indexed -- this
    table exists specifically to make rsID lookups possible; a variant with
    no rsID is only ever reachable via the coordinate-based lookup path.
    Returns (rows, total_records_scanned).
    """
    logger.info("Scanning %s for variant index rows", vcf_path)
    rows: list[tuple[str, str, int, str, str, uuid.UUID]] = []
    total = 0
    for record in iter_records(vcf_path):
        total += 1
        if total % _PROGRESS_LOG_EVERY == 0:
            logger.info("Scanned %s records so far (%s index rows built)", total, len(rows))
        rs_values = record.info.get("RS")
        if not rs_values:
            continue
        rs_ids = [str(v) for v in rs_values] if isinstance(rs_values, tuple) else [str(rs_values)]
        if not record.alts:
            continue
        for alt in record.alts:
            for raw_rs in rs_ids:
                rsid = raw_rs if raw_rs.lower().startswith("rs") else f"rs{raw_rs}"
                rows.append((rsid, record.chrom, record.pos, record.ref, alt, release_id))
    logger.info("Finished scanning %s: %s records, %s index rows", vcf_path, total, len(rows))
    return rows, total


def ingest(
    conn: Connection,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
) -> uuid.UUID:
    """Runs one full ingestion. Returns the new release's id.

    Raises ClinVarIngestionAlreadyRunning instead of starting a second,
    overlapping scan/download if one is already in flight.
    """
    if not _ingestion_lock.acquire(blocking=False):
        # A rejection is itself a signal (ADR 0020, observability#15), not
        # just a defensive no-op that happens to return a 409 -- this is
        # the real double-ingestion incident's failure mode, so every
        # rejection (admin-triggered or scheduled -- both entry points call
        # this same function) is counted here, at the one place the lock
        # is actually contended, rather than only where the exception
        # happens to be caught for HTTP translation (app/routes/admin.py).
        INGESTION_REJECTED_TOTAL.inc()
        raise ClinVarIngestionAlreadyRunning("An ingestion is already running")
    # Sampled instantaneously by a scrape rather than reconstructed after
    # the fact from a log line -- set around the same critical section the
    # lock itself guards (services#36), reset in the finally alongside the
    # lock release so a crash mid-ingestion can't leave this stuck at 1.
    INGESTION_IN_PROGRESS.set(1)
    try:
        release_id = uuid.uuid4()
        try:
            # Wraps only the actual work (download through activation),
            # not lock acquisition -- a run taking several multiples of the
            # ~90s real-data baseline is itself alert-worthy (ADR 0020's
            # ingestion-duration-anomaly SLI).
            with INGESTION_DURATION_SECONDS.time():
                return _do_ingest(conn, paths, downloader, producer, source_vcf_url, source_tbi_url, release_id)
        except Exception as exc:
            logger.error("ClinVar ingestion failed (attempted release %s)", release_id, exc_info=exc)
            raise ClinVarIngestionError(f"ClinVar ingestion failed for attempted release {release_id}") from exc
    finally:
        INGESTION_IN_PROGRESS.set(0)
        _ingestion_lock.release()


def _do_ingest(
    conn: Connection,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
    release_id: uuid.UUID,
) -> uuid.UUID:
    vcf_path = paths.vcf_path(release_id)
    tbi_path = paths.tbi_path(release_id)

    logger.info("Starting ClinVar ingestion, release %s", release_id)

    file_sha256 = downloader.download(source_vcf_url, vcf_path)
    downloader.download(source_tbi_url, tbi_path)

    tbi_checksum_url = source_tbi_url + ".md5"
    published_checksum = downloader.fetch_optional_text(tbi_checksum_url)
    if not validate_tbi(tbi_path, published_checksum):
        logger.warning("Published .tbi failed validation for release %s -- rebuilding via pysam", release_id)
        rebuild_tabix_index(vcf_path)

    published_date = read_published_date(vcf_path)

    previous_release = repository.current_active_release(conn)
    previous_release_id = previous_release.release_id if previous_release else None

    repository.insert_pending_release(conn, release_id, source_vcf_url, file_sha256, published_date)

    rows, variant_count = _build_variant_index_rows(vcf_path, release_id)
    repository.insert_variant_index_rows(conn, rows)

    repository.activate_release(conn, release_id, variant_count)

    paths.flip_current(release_id)

    keep = {release_id}
    if previous_release_id is not None:
        keep.add(previous_release_id)
    paths.prune_other_than(keep)
    repository.prune_variant_index_other_than(conn, release_id)

    old_vcf_path = paths.vcf_path(previous_release_id) if previous_release_id is not None else None
    if old_vcf_path is not None and not old_vcf_path.exists():
        # Previous release's file didn't survive on disk for some reason
        # (e.g. a prior prune ran before this ingestion, or first-ever run
        # after a volume was reset) -- treat as "nothing to diff against"
        # rather than failing the whole ingestion over a diff that's a
        # cache-invalidation nicety, not a correctness requirement of the
        # ingestion itself.
        logger.warning(
            "Previous release %s has no VCF on disk -- skipping changed-key diff", previous_release_id
        )
        old_vcf_path = None

    changed_keys = compute_changed_keys(old_vcf_path, vcf_path)

    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    if producer is not None:
        producer.publish(
            IngestionCompletedEvent(
                new_release_id=release_id,
                previous_release_id=previous_release_id,
                published_date=published_date.isoformat(),
                variant_count=variant_count,
                ingested_at=ingested_at,
                changed_keys=changed_keys,
            )
        )

    logger.info(
        "Completed ClinVar ingestion: release=%s previousRelease=%s publishedDate=%s "
        "variantCount=%s changedKeys=%s",
        release_id,
        previous_release_id,
        published_date,
        variant_count,
        len(changed_keys),
    )
    return release_id


__all__ = ["ingest", "ClinVarIngestionError", "ClinVarIngestionAlreadyRunning"]
