"""Prometheus metrics (ADR 0019/0020, observability#15).

clinvar-service shipped with OTel tracing only (``app/telemetry.py``) and
zero Prometheus metrics -- the real incident that motivated this (and
services#36's ``_ingestion_lock``) was two overlapping manual ingestion
triggers running two full VCF scans concurrently, invisible in logs for the
~90 real-data seconds their slowest step normally takes, ending in a
SIGKILL with no OOM evidence anywhere. None of that was ever observable as
a metric, only as a log line read after the fact. These four are the
metric surface that incident needed, per ADR 0020:

- ``clinvar_ingestion_duration_seconds``: wraps ``app/ingestion.py``'s
  ``_do_ingest`` (the actual work, not lock acquisition/release around
  it) -- a run taking several multiples of the ~90s real-data baseline is
  itself alert-worthy (ADR 0020's ingestion-duration-anomaly SLI, the
  exact signal that would have made the double-ingestion incident visible
  as a metric instead of a log line).
- ``clinvar_ingestion_in_progress``: a 0/1 gauge sampled instantaneously
  by a scrape, set around the same ``_ingestion_lock`` critical section
  services#36 already added, rather than reconstructed after the fact
  from a log line.
- ``clinvar_ingestion_rejected_total``: incremented every time
  ``ClinVarIngestionAlreadyRunning`` is actually raised (admin-triggered
  or scheduled) -- a rejection is itself a signal (ADR 0020), not just a
  defensive no-op that happens to return a 409. Backlog #54 moved the
  underlying guard from the in-process ``_ingestion_lock`` to the
  ``clinvar_ingestion_job`` table's partial unique index, but the same
  counter still fires at the one place the rejection actually happens.
- ``clinvar_lookup_duration_seconds``: latency/count for
  ``GET /internal/clinvar/lookup``, the raw HTTP-call latency/error rate
  from ``api``'s perspective (ADR 0020) -- a cache-outcome split stays
  ``api``'s job (ADR 0016's Redis layer), not this service's.
- ``clinvar_ingestion_jobs_total{status}`` (backlog #54): count of
  ingestion jobs reaching a terminal state (``succeeded``/``failed``/
  ``cancelled``). This is also what closes backlog #21e's ingestion-side
  gap: ``status="succeeded"`` is a real success-only signal distinct
  from ``clinvar_ingestion_duration_seconds_count`` (which increments on
  both success and failure), so ``ClinVarIngestionFreshnessBreach``
  (``platform/argocd/apps/prometheus.yaml``) can key off actual job
  outcomes instead of "an attempt happened, regardless of outcome". Every
  real status value is explicitly zero-initialized below (``.inc(0)``) --
  backlog #122's own real incident: ``prometheus_client`` only exposes a
  labeled child series after its first real ``.inc()``, so a freshly-
  started process that has never yet had a real success omits
  ``status="succeeded"`` from ``/metrics`` entirely rather than reporting
  it as ``0``, leaving ``increase()`` with no real baseline sample once
  the first success finally happens -- confirmed live to keep the alert
  firing 30+ minutes after a real, successful ingestion.

A single module-level registration (the process default
``prometheus_client.REGISTRY``) rather than a custom ``CollectorRegistry``
-- clinvar-service runs uvicorn with no ``--workers`` flag (see
``Dockerfile``), so there is exactly one process and no need for
``prometheus_client``'s multiprocess mode. Importing this module more than
once (e.g. across test modules) is safe: Python caches the module after
first import, so these ``Histogram``/``Gauge``/``Counter`` objects are only
constructed -- and registered -- once per process, even though
``tests/test_lookup_endpoint.py``'s ``client`` fixture calls
``create_app()`` fresh for every test.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

INGESTION_DURATION_SECONDS = Histogram(
    "clinvar_ingestion_duration_seconds",
    "Wall-clock time for one full ClinVar ingestion run (download through activation).",
)

INGESTION_IN_PROGRESS = Gauge(
    "clinvar_ingestion_in_progress",
    "1 while an ingestion is running (admin-triggered or scheduled), 0 otherwise.",
)

INGESTION_REJECTED_TOTAL = Counter(
    "clinvar_ingestion_rejected_total",
    "Ingestion attempts rejected because one was already running (services#36's lock).",
)

LOOKUP_DURATION_SECONDS = Histogram(
    "clinvar_lookup_duration_seconds",
    "Latency of GET /internal/clinvar/lookup, regardless of outcome (hit, 400, 404).",
)

INGESTION_JOBS_TOTAL = Counter(
    "clinvar_ingestion_jobs_total",
    "Count of ClinVar ingestion jobs reaching a terminal state, by outcome.",
    ["status"],
)
# Real, live incident (backlog #122, 2026-08-12): prometheus_client only
# exposes a labeled child series in /metrics after its first .inc() call
# -- a freshly-started (or freshly-rebuilt) process that has never yet had
# a job reach status="succeeded" simply omits that series entirely, not
# "reports 0". ClinVarIngestionFreshnessBreach's own increase(...[8d]) < 1
# query then has no real "0" baseline sample to compare against once the
# first real success finally happens: the series is born already at 1,
# increase() over any window correctly reports 0 real change within it,
# and the alert stays firing until a *second* real success creates an
# actual detectable rise -- confirmed live, this stayed firing 30+ minutes
# after a real, successful ingestion. Explicit zero-init at import time
# (a standard, documented prometheus_client pattern for exactly this
# class of counter-freshness alert) makes every real status value exist
# from the process's first scrape, not just whichever ones happen to have
# already occurred.
for _status in ("succeeded", "failed", "cancelled"):
    INGESTION_JOBS_TOTAL.labels(status=_status).inc(0)

__all__ = [
    "INGESTION_DURATION_SECONDS",
    "INGESTION_IN_PROGRESS",
    "INGESTION_REJECTED_TOTAL",
    "LOOKUP_DURATION_SECONDS",
    "INGESTION_JOBS_TOTAL",
]
