"""GET /healthz -- liveness/readiness probe target (kubernetes/sentiment-analyzer/deployment.yaml),
same shape and path as clinvar-service's own `app/routes/admin.py::healthz`.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "UP"}
