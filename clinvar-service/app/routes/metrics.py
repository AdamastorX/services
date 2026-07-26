"""GET /metrics (ADR 0019/0020, observability#15): clinvar-service's
Prometheus scrape endpoint, its own route module alongside admin.py's
/healthz -- the two are conceptually distinct (one's an operability signal
for humans/probes reading a status, the other's a scrape target for
Prometheus), matching the existing lookup.py/admin.py split by concern
rather than folding this into either.
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter()


@router.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
