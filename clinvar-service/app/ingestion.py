"""Orchestrates one full ClinVar ingestion (ADR 0019, reimplemented from
ADR 0018's Java ``ClinVarIngestionService``; backlog #54 turned it into a
real, restart-safe, cancellable job).

Ordering is the whole point of this module (ADR 0018's "readers never see
a half-written release", carried over unchanged): everything that can
fail happens against the *new* release's own private directory first;
only ``activate_release`` touches anything a reader could already be
looking at (the ``clinvar_release`` table), and the filesystem ``current``
symlink -- the other thing readers actually consult -- only moves after
that transaction has committed. A failure at any earlier step leaves
``current`` pointing exactly where it did before this function was ever
called.

Job control plane (backlog #54): every ingestion -- admin-triggered or
scheduled -- is now tracked as a row in ``clinvar_ingestion_job``, not
just a synchronous call that blocks the caller for its whole multi-minute
duration. ``clinvar_ingestion_job``'s own partial unique index
(migrations/0002) is the concurrency guard; services#36's in-process
``threading.Lock`` (``_ingestion_lock``) is retired, because it only ever
protected one process's memory and reset to unlocked on every restart --
exactly wrong for the failure mode this item exists to fix (a pod killed
mid-ingestion). ``threading.Event`` is still used here, but for a
different, legitimate purpose: a per-job cooperative cancellation signal
checked from inside the running scan loop, not a mutex gating concurrent
ingestions.
"""

from __future__ import annotations

import datetime
import logging
import threading
import uuid
from pathlib import Path

from psycopg import Connection
from psycopg_pool import ConnectionPool

from app import repository
from app.diff import compute_changed_keys
from app.download import Downloader, validate_tbi
from app.kafka_producer import IngestionCompletedEvent, IngestionEventProducer
from app.metrics import (
    INGESTION_DURATION_SECONDS,
    INGESTION_IN_PROGRESS,
    INGESTION_JOBS_TOTAL,
    INGESTION_REJECTED_TOTAL,
)
from app.paths import ClinVarRefdataPaths
from app.vcf_query import iter_records, read_published_date, rebuild_tabix_index

logger = logging.getLogger(__name__)

_PROGRESS_LOG_EVERY = 250_000
# Checked more often than the progress log line so a cancel request is
# responsive without the overhead of checking on every single record.
_CANCEL_CHECK_EVERY = 10_000

# job_id -> a live cancellation signal, but only for a job whose
# background execution is running in *this* process. Populated for the
# duration of _run_job_with_tracking() and always removed in its
# finally, whether the job succeeded, failed, or was cancelled. A pod
# restart loses this dict entirely, same as it loses everything else in
# memory -- that's exactly why Postgres, not this dict, is the durable
# record of a job's state (reconcile_orphaned_jobs() below is what
# handles that case).
_cancel_events: dict[uuid.UUID, threading.Event] = {}
_cancel_events_lock = threading.Lock()


class ClinVarIngestionError(RuntimeError):
    pass


class ClinVarIngestionAlreadyRunning(RuntimeError):
    pass


class ClinVarIngestionCancelled(RuntimeError):
    pass


def request_cancel(job_id: uuid.UUID) -> bool:
    """Sets the in-process cancel signal for job_id's background thread,
    if one is live in this process. Returns whether a live signal was
    found. The caller (app/routes/admin.py) also flags
    ``cancel_requested`` in Postgres unconditionally, so a request against
    a job this process has no live signal for (there is only ever one
    replica here, so in practice this only happens if the job already
    finished, or in the narrow window before its thread has registered
    the signal) is still recorded, even though nothing is listening for
    it right now.
    """
    with _cancel_events_lock:
        event = _cancel_events.get(job_id)
    if event is None:
        return False
    event.set()
    return True


def _check_cancelled(cancel_event: threading.Event | None, job_id: uuid.UUID, when: str) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise ClinVarIngestionCancelled(f"Ingestion job {job_id} cancelled {when}")


def _build_variant_index_rows(
    vcf_path: Path,
    release_id: uuid.UUID,
    job_id: uuid.UUID,
    progress_cb=None,
    cancel_event: threading.Event | None = None,
) -> tuple[list[tuple[str, str, int, str, str, uuid.UUID]], int]:
    """Only variants that carry an RS (dbSNP rs-number) are indexed -- this
    table exists specifically to make rsID lookups possible; a variant with
    no rsID is only ever reachable via the coordinate-based lookup path.
    Returns (rows, total_records_scanned).

    ``progress_cb(total, len(rows))``, if given, is called at the exact
    same 250k-record checkpoint the log line already uses (backlog #54's
    own AC: reuse the existing progress logging, don't invent a second
    mechanism) -- this is what lets ``GET .../ingest/{job_id}`` report
    real progress instead of only "running".
    """
    logger.info("Scanning %s for variant index rows (job %s)", vcf_path, job_id)
    rows: list[tuple[str, str, int, str, str, uuid.UUID]] = []
    total = 0
    for record in iter_records(vcf_path):
        total += 1
        if cancel_event is not None and total % _CANCEL_CHECK_EVERY == 0 and cancel_event.is_set():
            raise ClinVarIngestionCancelled(f"Ingestion job {job_id} cancelled after {total} records scanned")
        if total % _PROGRESS_LOG_EVERY == 0:
            logger.info("Scanned %s records so far (%s index rows built)", total, len(rows))
            if progress_cb is not None:
                progress_cb(total, len(rows))
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
    if progress_cb is not None:
        progress_cb(total, len(rows))
    return rows, total


def _reserve_job(conn: Connection, trigger: str) -> uuid.UUID:
    """Inserts a new ``queued`` job row. This -- not an in-process lock --
    is the actual concurrency guard (migrations/0002's partial unique
    index): a second reservation attempt while one is already queued/
    running gets a real Postgres unique-violation, translated here into
    the same ``ClinVarIngestionAlreadyRunning`` callers already handled
    before this item existed.
    """
    job_id = uuid.uuid4()
    try:
        repository.create_queued_job(conn, job_id, trigger)
    except repository.ClinVarIngestionJobAlreadyActive as exc:
        # A rejection is itself a signal (ADR 0020, observability#15), not
        # just a defensive no-op that happens to return a 409 -- counted
        # here, the one place the guard is actually contended, same as
        # before services#36's Lock was retired.
        INGESTION_REJECTED_TOTAL.inc()
        raise ClinVarIngestionAlreadyRunning("An ingestion job is already queued or running") from exc
    return job_id


def _execute_reserved_job(
    conn: Connection,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
    job_id: uuid.UUID,
    cancel_event: threading.Event | None,
) -> uuid.UUID:
    """Marks an already-reserved (``queued``) job ``running``, does the
    real work, and records the terminal DB state + outcome metric --
    shared by both the fully synchronous ``ingest()`` (used directly by
    tests and by the scheduled trigger, which already runs on its own
    APScheduler worker thread) and ``trigger_ingestion_job()`` (spawns its
    own background thread so the 202 HTTP handler never blocks). Exactly
    one code path does the actual work either way.
    """
    repository.mark_job_running(conn, job_id)
    try:
        release_id = _do_ingest(conn, paths, downloader, producer, source_vcf_url, source_tbi_url, job_id, cancel_event)
    except ClinVarIngestionCancelled as exc:
        logger.warning("ClinVar ingestion job %s cancelled: %s", job_id, exc)
        repository.mark_job_cancelled(conn, job_id, str(exc))
        INGESTION_JOBS_TOTAL.labels(status="cancelled").inc()
        raise
    except Exception as exc:
        logger.error("ClinVar ingestion job %s failed", job_id, exc_info=exc)
        repository.mark_job_failed(conn, job_id, str(exc)[:2000])
        INGESTION_JOBS_TOTAL.labels(status="failed").inc()
        raise ClinVarIngestionError(f"ClinVar ingestion failed for job {job_id}") from exc
    else:
        repository.mark_job_succeeded(conn, job_id, release_id)
        INGESTION_JOBS_TOTAL.labels(status="succeeded").inc()
        return release_id


def _run_job_with_tracking(
    conn: Connection,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
    job_id: uuid.UUID,
) -> uuid.UUID:
    """Registers this job's cancellation signal, runs it to completion via
    ``_execute_reserved_job``, and always de-registers the signal
    afterward -- regardless of whether the caller is running this
    synchronously (``ingest()``) or on a background thread
    (``trigger_ingestion_job()``).
    """
    cancel_event = threading.Event()
    with _cancel_events_lock:
        _cancel_events[job_id] = cancel_event
    try:
        INGESTION_IN_PROGRESS.set(1)
        try:
            # Wraps only the actual work (download through activation), not
            # job reservation -- a run taking several multiples of the
            # ~90s real-data baseline is itself alert-worthy (ADR 0020's
            # ingestion-duration-anomaly SLI).
            with INGESTION_DURATION_SECONDS.time():
                return _execute_reserved_job(
                    conn, paths, downloader, producer, source_vcf_url, source_tbi_url, job_id, cancel_event
                )
        finally:
            INGESTION_IN_PROGRESS.set(0)
    finally:
        with _cancel_events_lock:
            _cancel_events.pop(job_id, None)


def ingest(
    conn: Connection,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
    trigger: str = "manual",
) -> uuid.UUID:
    """Runs one full ingestion synchronously against the given connection
    and returns the new release's id. Used directly by the test suite and
    by the scheduled trigger (already running on its own APScheduler
    worker thread, so a second background-thread hop would add nothing).
    The HTTP-facing manual trigger uses ``trigger_ingestion_job`` instead,
    which returns a job id immediately (backlog #54).

    Raises ClinVarIngestionAlreadyRunning instead of starting a second,
    overlapping scan/download if one is already in flight -- enforced by
    ``clinvar_ingestion_job``'s own unique index now, not an in-process
    lock (see module docstring).
    """
    job_id = _reserve_job(conn, trigger)
    return _run_job_with_tracking(conn, paths, downloader, producer, source_vcf_url, source_tbi_url, job_id)


def trigger_ingestion_job(
    pool: ConnectionPool,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
    trigger: str = "manual",
) -> uuid.UUID:
    """Reserves a job and returns its id immediately (backlog #54) --
    the actual work runs on a background thread against its own pooled
    connection, so the caller (the 202 HTTP handler) never blocks for the
    ingestion itself. Reservation itself (and therefore the 409 case)
    still happens synchronously, before this function returns, so a
    rejected trigger is reported to the caller directly rather than only
    discoverable later via a job-status poll.
    """
    with pool.connection() as conn:
        job_id = _reserve_job(conn, trigger)

    def _run() -> None:
        try:
            with pool.connection() as thread_conn:
                _run_job_with_tracking(thread_conn, paths, downloader, producer, source_vcf_url, source_tbi_url, job_id)
        except Exception:
            # _run_job_with_tracking -> _execute_reserved_job already
            # recorded the terminal DB state and metric for every
            # ingestion-shaped failure; this outer catch exists only so a
            # truly unexpected failure (e.g. the pool itself unreachable)
            # can't kill the thread with no trace anywhere.
            logger.error("Background ClinVar ingestion job %s thread exited abnormally", job_id, exc_info=True)

    threading.Thread(target=_run, daemon=True, name=f"clinvar-ingestion-{job_id}").start()
    return job_id


def reconcile_orphaned_jobs(conn: Connection) -> list[uuid.UUID]:
    """Called once at startup (app/main.py's lifespan), before the
    scheduler starts or any new trigger can be accepted. Any job still
    ``queued``/``running`` here can only mean the previous process died
    mid-ingestion -- in-memory state (this process's own cancel-event
    dict, any partially-scanned VCF) cannot have survived that restart,
    so there is nothing left in memory to resume from.

    Marked ``failed`` with an explicit reason rather than left ``running``
    forever (backlog #54's own AC). True mid-scan resume -- persisting a
    scan checkpoint and restarting the VCF iterator from it -- was
    considered and rejected as scope this item didn't ask for: the same
    "don't build ahead of need" discipline #54's own purpose text invokes
    against a resurrection of the closed M6 milestone applies here too.
    """
    orphaned = repository.find_active_jobs(conn)
    reconciled: list[uuid.UUID] = []
    for job in orphaned:
        reason = f"orphaned: process restarted while this job was still {job.status}"
        repository.mark_job_failed(conn, job.job_id, reason)
        INGESTION_JOBS_TOTAL.labels(status="failed").inc()
        if job.release_id is not None:
            # Cleans up the placeholder clinvar_release row this job's
            # insert_pending_release() already committed before the
            # restart interrupted it -- guarded by is_active=false inside
            # delete_pending_release, so a release that had actually
            # already gone live (an even narrower race: the process died
            # after activate_release committed but before this job's row
            # was marked succeeded) can never be deleted here.
            repository.delete_pending_release(conn, job.release_id)
        logger.warning(
            "Reconciled orphaned ClinVar ingestion job %s (was %s) -> failed: %s", job.job_id, job.status, reason
        )
        reconciled.append(job.job_id)
    return reconciled


def _do_ingest(
    conn: Connection,
    paths: ClinVarRefdataPaths,
    downloader: Downloader,
    producer: IngestionEventProducer | None,
    source_vcf_url: str,
    source_tbi_url: str,
    job_id: uuid.UUID,
    cancel_event: threading.Event | None,
) -> uuid.UUID:
    release_id = uuid.uuid4()
    vcf_path = paths.vcf_path(release_id)
    tbi_path = paths.tbi_path(release_id)

    logger.info("Starting ClinVar ingestion, release %s (job %s)", release_id, job_id)

    _check_cancelled(cancel_event, job_id, "before download")

    file_sha256 = downloader.download(source_vcf_url, vcf_path)
    downloader.download(source_tbi_url, tbi_path)

    _check_cancelled(cancel_event, job_id, "after download")

    tbi_checksum_url = source_tbi_url + ".md5"
    published_checksum = downloader.fetch_optional_text(tbi_checksum_url)
    if not validate_tbi(tbi_path, published_checksum):
        logger.warning("Published .tbi failed validation for release %s -- rebuilding via pysam", release_id)
        rebuild_tabix_index(vcf_path)

    published_date = read_published_date(vcf_path)

    previous_release = repository.current_active_release(conn)
    previous_release_id = previous_release.release_id if previous_release else None

    repository.insert_pending_release(conn, release_id, source_vcf_url, file_sha256, published_date)
    # Tracked on the job row from this point on so a cancellation or an
    # orphaned-by-restart reconciliation knows which placeholder
    # clinvar_release row to clean up -- this release isn't visible to
    # readers (is_active stays false) until activate_release commits
    # below, but it must not be left dangling if this job never gets that
    # far.
    repository.set_job_attempted_release(conn, job_id, release_id)

    try:
        rows, variant_count = _build_variant_index_rows(
            vcf_path,
            release_id,
            job_id,
            progress_cb=lambda scanned, built: repository.update_job_progress(conn, job_id, scanned, built),
            cancel_event=cancel_event,
        )
    except ClinVarIngestionCancelled:
        repository.delete_pending_release(conn, release_id)
        raise

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


__all__ = [
    "ingest",
    "trigger_ingestion_job",
    "reconcile_orphaned_jobs",
    "request_cancel",
    "ClinVarIngestionError",
    "ClinVarIngestionAlreadyRunning",
    "ClinVarIngestionCancelled",
]
