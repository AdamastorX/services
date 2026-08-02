"""sentiment-analyzer entrypoint (backlog #80, ADR 0029): FastAPI app
wiring config, OTel tracing, the VADER scorer, the Kafka producer, and
the background consumer thread (see `app/kafka_consumer.py` for why a
thread, not the APScheduler `clinvar-service` uses for its own
periodic ingestion job). Same "uvicorn app + a background worker"
process shape `clinvar-service/app/main.py` established, with the
background piece swapped from a cron-triggered job to a continuous
consume loop -- the shape the work itself calls for.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import get_settings
from app.kafka_consumer import SentimentConsumerWorker
from app.kafka_producer import SentimentEventProducer
from app.routes import health, metrics
from app.scoring import VaderScorer
from app.telemetry import configure_tracing, instrument_app

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings

    # Same ordering constraint clinvar-service/app/main.py documents:
    # configure_tracing() before instrument_app(), since
    # FastAPIInstrumentor resolves its Tracer from whatever
    # TracerProvider is globally registered at the moment
    # instrument_app() is called, not lazily per-request.
    configure_tracing(settings.service_name, settings.otlp_collector_endpoint)
    instrument_app(app)

    producer = SentimentEventProducer(settings.kafka_bootstrap_servers, settings.news_sentiment_scored_topic)
    scorer = VaderScorer()
    worker = SentimentConsumerWorker(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        consume_topic=settings.news_article_published_topic,
        group_id=settings.consumer_group_id,
        producer=producer,
        scorer=scorer,
    )
    app.state.event_producer = producer
    app.state.consumer_worker = worker
    worker.start()

    yield

    worker.stop()
    producer.close()


def create_app() -> FastAPI:
    app = FastAPI(title="sentiment-analyzer", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(metrics.router)
    return app


app = create_app()

__all__ = ["app", "create_app"]
