"""OpenTelemetry SDK wiring (ADR 0013, applied here per backlog #80).

Same cross-service trace-correlation story every other component here
already participates in (`clinvar-service/app/telemetry.py`, this
project's Python precedent) -- OTLP over HTTP to the shared Collector,
W3C traceparent propagation. `ConfluentKafkaInstrumentor` covers both
this service's consumer (`app/kafka_consumer.py`) and its producer
(`app/kafka_producer.py`), so a trace started in `news-ingestor` when it
publishes `news.article.published` continues through this service's
consume-score-publish path into `news.sentiment.scored`, not two
disconnected traces either side of the topic.

`OTLP_COLLECTOR_ENDPOINT` reuses the exact env var name and full-URL-
including-`/v1/traces` convention `clinvar-service`'s own
`app/telemetry.py` documents and every Java service already follows --
see that module's docstring for the `OTLPSpanExporter(endpoint=...)`
literal-use detail this convention exists to satisfy.
"""

from __future__ import annotations

import logging

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

logger = logging.getLogger(__name__)

_configured = False


def configure_tracing(service_name: str, otlp_endpoint: str) -> None:
    global _configured
    if _configured:
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)

    ConfluentKafkaInstrumentor().instrument()

    _configured = True
    logger.info("OpenTelemetry tracing configured: service=%s otlp_endpoint=%s", service_name, otlp_endpoint)


def instrument_app(app) -> None:
    FastAPIInstrumentor.instrument_app(app)
