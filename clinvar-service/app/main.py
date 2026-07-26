"""clinvar-service entrypoint (ADR 0019): FastAPI app wiring config, the
Postgres pool + migrations, the OTel SDK, the weekly ingestion scheduler,
and the two route groups (public lookup, internal admin)."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import db
from app.config import get_settings
from app.download import Downloader
from app.ingestion import ingest as run_ingestion
from app.kafka_producer import IngestionEventProducer
from app.migrator import run_migrations
from app.paths import ClinVarRefdataPaths
from app.routes import admin, lookup, metrics
from app.scheduler import start_scheduler
from app.telemetry import configure_tracing, instrument_app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings

    # Must run before instrument_app(): FastAPIInstrumentor resolves its
    # Tracer from whatever TracerProvider is globally registered at the
    # moment instrument_app() is called (verified by reading its source --
    # it is not resolved lazily per-span), so instrumenting first would
    # permanently bind every span this app creates to the SDK's default
    # no-op provider, silently breaking cross-service trace correlation.
    configure_tracing(settings.service_name, settings.otlp_collector_endpoint)
    instrument_app(app)

    pool = db.init_pool(settings.database_url)
    app.state.db_pool = pool
    with pool.connection() as conn:
        applied = run_migrations(conn)
        if applied:
            logger.info("Applied migrations: %s", applied)

    app.state.refdata_paths = ClinVarRefdataPaths(settings.clinvar_refdata_path)
    app.state.downloader = Downloader()
    app.state.event_producer = IngestionEventProducer(
        settings.kafka_bootstrap_servers, settings.clinvar_ingestion_topic
    )

    def _scheduled_ingest() -> None:
        with app.state.db_pool.connection() as conn:
            run_ingestion(
                conn,
                app.state.refdata_paths,
                app.state.downloader,
                app.state.event_producer,
                settings.clinvar_source_vcf_url,
                settings.clinvar_source_tbi_url,
            )

    app.state.scheduler = start_scheduler(settings.clinvar_ingestion_cron, _scheduled_ingest)

    yield

    app.state.scheduler.shutdown(wait=False)
    app.state.event_producer.close()
    db.close_pool()


def create_app() -> FastAPI:
    app = FastAPI(title="clinvar-service", lifespan=lifespan)
    app.include_router(lookup.router)
    app.include_router(admin.router)
    app.include_router(metrics.router)
    return app


app = create_app()

__all__ = ["app", "create_app"]
