"""GET /healthz reflects the real consumer-thread state (found live
during review, see app/routes/health.py's own docstring and
SentimentConsumerWorker.is_alive()'s): a dead background thread must
fail this probe, not report UP unconditionally.
"""

from __future__ import annotations

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.routes import health


def _client_with_worker(worker) -> TestClient:
    app = FastAPI()
    app.include_router(health.router)
    app.state = SimpleNamespace(consumer_worker=worker)
    return TestClient(app)


def test_healthz_is_up_when_consumer_thread_is_alive():
    client = _client_with_worker(SimpleNamespace(is_alive=lambda: True))

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["status"] == "UP"


def test_healthz_is_down_when_consumer_thread_has_died():
    client = _client_with_worker(SimpleNamespace(is_alive=lambda: False))

    response = client.get("/healthz")

    assert response.status_code == 503
    assert response.json()["status"] == "DOWN"


def test_healthz_is_up_when_no_worker_wired_yet():
    # Defensive: request.app.state.consumer_worker may not exist yet in a
    # context that doesn't go through app/main.py's lifespan (e.g. a bare
    # FastAPI() in a test) -- must not crash the probe.
    app = FastAPI()
    app.include_router(health.router)
    client = TestClient(app)

    response = client.get("/healthz")

    assert response.status_code == 200
