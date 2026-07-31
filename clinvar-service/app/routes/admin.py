"""Health probe + async ClinVar ingestion job control plane (backlog #54).

The manual trigger returns ``202`` with a job id immediately instead of
blocking for the whole multi-minute ingestion (the fragile shape
``docs/SESSION_STATE.md`` named directly: a long-running operation behind
a synchronous request, at the mercy of every client/proxy/Ingress timeout
between the caller and the process). ``GET .../ingest/{job_id}`` polls
real state persisted in Postgres (``app/repository.py``'s
``clinvar_ingestion_job``), not in memory -- in-memory state is exactly
what a pod restart destroys. ``POST .../ingest/{job_id}/cancel`` actually
stops the in-flight scan (``app/ingestion.py``'s cancellation check), not
just relabels the row.
"""

from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Request

from app import ingestion, repository

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "UP"}


@router.post("/internal/clinvar/ingest", status_code=202)
def trigger_ingestion(request: Request) -> dict:
    """Manual admin-triggered re-ingestion, for dev/CI use. Returns
    immediately with a job id (backlog #54) -- poll
    ``GET /internal/clinvar/ingest/{job_id}`` for state and progress; the
    ingestion itself runs on a background thread against its own pooled
    connection.
    """
    settings = request.app.state.settings
    pool = request.app.state.db_pool
    paths = request.app.state.refdata_paths
    downloader = request.app.state.downloader
    producer = request.app.state.event_producer

    try:
        job_id = ingestion.trigger_ingestion_job(
            pool,
            paths,
            downloader,
            producer,
            settings.clinvar_source_vcf_url,
            settings.clinvar_source_tbi_url,
            trigger="manual",
        )
    except ingestion.ClinVarIngestionAlreadyRunning as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    return {"jobId": str(job_id), "status": "queued"}


@router.get("/internal/clinvar/ingest/{job_id}")
def get_ingestion_job(job_id: uuid.UUID, request: Request) -> dict:
    pool = request.app.state.db_pool
    with pool.connection() as conn:
        job = repository.get_ingestion_job(conn, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"No ingestion job {job_id}")
    return _job_response(job)


@router.post("/internal/clinvar/ingest/{job_id}/cancel", status_code=202)
def cancel_ingestion_job(job_id: uuid.UUID, request: Request) -> dict:
    """Requests cancellation of a queued/running job. Proven live (backlog
    #54) to actually stop the in-flight scan, not just relabel the row:
    the DB flag is set unconditionally, and the in-process signal
    (``app.ingestion.request_cancel``) is what the running scan loop
    itself checks every 10k records.
    """
    pool = request.app.state.db_pool
    with pool.connection() as conn:
        job = repository.get_ingestion_job(conn, job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"No ingestion job {job_id}")
        if job.status not in ("queued", "running"):
            raise HTTPException(
                status_code=409,
                detail=f"Job {job_id} is already {job.status}, nothing to cancel",
            )
        repository.request_job_cancel(conn, job_id)

    signaled = ingestion.request_cancel(job_id)
    if not signaled:
        logger.warning(
            "Cancel requested for job %s but no live in-process signal was found "
            "(cancel_requested flagged in Postgres regardless)",
            job_id,
        )

    return {"jobId": str(job_id), "status": "cancel_requested"}


def _job_response(job: repository.ClinVarIngestionJob) -> dict:
    return {
        "jobId": str(job.job_id),
        "status": job.status,
        "trigger": job.trigger,
        "createdAt": job.created_at.isoformat(),
        "startedAt": job.started_at.isoformat() if job.started_at else None,
        "finishedAt": job.finished_at.isoformat() if job.finished_at else None,
        "recordsScanned": job.records_scanned,
        "indexRowsBuilt": job.index_rows_built,
        "releaseId": str(job.release_id) if job.release_id else None,
        "failureReason": job.failure_reason,
        "cancelRequested": job.cancel_requested,
    }
