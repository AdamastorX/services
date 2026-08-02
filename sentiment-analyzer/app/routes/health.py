"""GET /healthz -- liveness/readiness probe target (kubernetes/sentiment-analyzer/deployment.yaml),
same shape and path as clinvar-service's own `app/routes/admin.py::healthz`.

Checks the real background consumer thread (`app.state.consumer_worker`,
wired in `app/main.py`'s lifespan), not just "the FastAPI process is
still scheduling requests" -- found live during review: those are not
the same thing for this service. `SentimentConsumerWorker.is_alive()`'s
own docstring has the full incident this guards against (an uncaught
exception silently killing the daemon thread while this route kept
returning UP unconditionally). A dead consumer thread now fails this
probe for real, so Kubernetes' own liveness check restarts the pod
instead of leaving a silently-broken-but-Healthy one running.
"""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

router = APIRouter()


@router.get("/healthz")
def healthz(request: Request) -> Response:
    worker = getattr(request.app.state, "consumer_worker", None)
    if worker is not None and not worker.is_alive():
        return Response(content='{"status":"DOWN","reason":"consumer thread not running"}', status_code=503, media_type="application/json")
    return Response(content='{"status":"UP"}', status_code=200, media_type="application/json")
