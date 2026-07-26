"""Weekly in-process ingestion trigger (ADR 0019).

Deliberately APScheduler, not a Kubernetes ``CronJob`` -- a ``CronJob``
spawns a ``Job`` under the hood, which on a literal reading would violate
this project's "no Kubernetes Jobs for this milestone" boundary (ADR
0018/0019), and brings a K8s primitive this project has never used
(missed-schedule handling, concurrency policy, job-history GC, RBAC for
job creation) for no benefit here. Same reasoning as the Java
``@Scheduled`` trigger it replaces, just a Python-native equivalent.

A failure here is caught and logged rather than left to propagate --
``app.ingestion.ingest`` has already logged the underlying cause before
this catches it; the scheduler just needs to survive to try again next
week.
"""

from __future__ import annotations

import logging

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)


def start_scheduler(cron_expression: str, job) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()

    def _guarded_job() -> None:
        try:
            job()
        except Exception:
            logger.error("Scheduled ClinVar ingestion failed -- will retry on the next scheduled run", exc_info=True)

    trigger = CronTrigger.from_crontab(cron_expression)
    scheduler.add_job(_guarded_job, trigger=trigger, id="clinvar-ingestion", replace_existing=True)
    scheduler.start()
    return scheduler
