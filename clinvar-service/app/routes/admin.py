"""Health probe + manual ingestion trigger (dev/CI use, same continuity
ADR 0018 established for its Java equivalent)."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request

from app import ingestion

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "UP"}


@router.post("/internal/clinvar/ingest", status_code=202)
def trigger_ingestion(request: Request) -> dict:
    """Manual admin-triggered re-ingestion, for dev/CI use -- runs
    synchronously and returns the new release id (or a 500 on failure),
    the same continuity ADR 0018 established for its Java equivalent."""
    settings = request.app.state.settings
    pool = request.app.state.db_pool
    paths = request.app.state.refdata_paths
    downloader = request.app.state.downloader
    producer = request.app.state.event_producer

    try:
        with pool.connection() as conn:
            release_id = ingestion.ingest(
                conn,
                paths,
                downloader,
                producer,
                settings.clinvar_source_vcf_url,
                settings.clinvar_source_tbi_url,
            )
    except ingestion.ClinVarIngestionAlreadyRunning as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ingestion.ClinVarIngestionError as exc:
        logger.error("Manual ingestion trigger failed", exc_info=exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {"releaseId": str(release_id)}
