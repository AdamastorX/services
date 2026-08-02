"""Shared pytest fixtures.

``kafka_bootstrap_servers`` prefers
``SENTIMENT_ANALYZER_TEST_KAFKA_BOOTSTRAP_SERVERS`` if set (same
"point at something already running" escape hatch
``clinvar-service/tests/conftest.py``'s ``postgres_dsn`` fixture offers
via ``CLINVAR_TEST_DATABASE_URL``) and otherwise spins up a real
``testcontainers`` Kafka broker -- what CI (which has Docker, per
``.github/workflows/ci.yml``'s ``clinvar_service``/``workload_generator``
jobs' own precedent) actually uses for this same suite.

If neither a live override nor a reachable Docker daemon is available
(this implementation session's own sandbox has no Docker daemon), the
Kafka-backed integration test is skipped with an explicit reason rather
than erroring ambiguously or being silently omitted from the test file
altogether -- the test itself is real and will run for real the first
time this suite executes somewhere Docker is actually available (this
project's CI, or a developer's machine with Docker running).
"""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def kafka_bootstrap_servers():
    override = os.environ.get("SENTIMENT_ANALYZER_TEST_KAFKA_BOOTSTRAP_SERVERS")
    if override:
        yield override
        return

    try:
        from testcontainers.kafka import KafkaContainer
    except Exception as exc:  # pragma: no cover - import-time only
        pytest.skip(f"testcontainers[kafka] not usable in this environment: {exc}")

    try:
        container = KafkaContainer()
        container.start()
    except Exception as exc:
        pytest.skip(f"No reachable Docker daemon to run a real Kafka broker for this test: {exc}")

    try:
        yield container.get_bootstrap_server()
    finally:
        container.stop()
