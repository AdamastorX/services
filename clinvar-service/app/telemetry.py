"""OpenTelemetry SDK wiring (ADR 0019).

This project already has a working, cross-correlated tracing story across
api/workers (observability#1, ADR 0013): OTLP over HTTP to a
Collector, W3C traceparent propagation carrying trace context across
service boundaries. clinvar-service participates in the exact same story
using the language-appropriate SDK instead of reinventing anything --
FastAPI's instrumentation extracts an inbound ``traceparent`` header
automatically (the OTel SDK's default global propagator is
already W3C tracecontext), so a trace started in ``api`` and continued
into a call to ``GET /internal/clinvar/lookup`` shows up as one
continuous trace, not two disconnected ones.

``OTLP_COLLECTOR_ENDPOINT`` reuses the exact env var name (and, notably,
the exact *full* endpoint value including the ``/v1/traces`` path) the
Java services already use -- deliberately not the SDK's own
``OTEL_EXPORTER_OTLP_ENDPOINT``/``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT``,
for the identical reason documented in api/application.yml: passing an
explicit ``endpoint=`` to ``OTLPSpanExporter`` is used *literally* (no
``/v1/traces`` auto-append the SDK only does for its own env-var-driven
default), so this env var's value must already carry the full path --
verified by reading ``OTLPSpanExporter.__init__`` directly, not assumed.
"""

from __future__ import annotations

import logging

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
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
    # BatchSpanProcessor exports asynchronously in the background and
    # swallows/logs exporter errors internally -- fire-and-forget, same
    # as the Java services' OTLP exporter: an unreachable Collector must
    # never block a request or fail startup.
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint)))
    trace.set_tracer_provider(provider)

    PsycopgInstrumentor().instrument()
    ConfluentKafkaInstrumentor().instrument()

    _configured = True
    logger.info("OpenTelemetry tracing configured: service=%s otlp_endpoint=%s", service_name, otlp_endpoint)


def instrument_app(app) -> None:
    FastAPIInstrumentor.instrument_app(app)
