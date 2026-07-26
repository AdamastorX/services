"""Environment-driven configuration (ADR 0019).

Convention, stated once here rather than left implicit: ``DATABASE_URL``
(a plain ``postgresql://user:password@host:port/dbname`` DSN) is the
primary way to point this service at its own, dedicated Postgres instance
-- ADR 0019's whole reason for existing is that this service's Postgres
credential must never need to be shared across a namespace boundary, so
there is exactly one consumer of this value and one Secret to wire it
from. If ``DATABASE_URL`` isn't set, the five discrete
``POSTGRES_HOST``/``POSTGRES_PORT``/``POSTGRES_USER``/``POSTGRES_PASSWORD``/
``POSTGRES_DB`` variables are composed into one instead -- useful when a
platform-side Secret is more natural to wire as separate keys than as one
pre-built URL (e.g. a Helm chart that generates host/user/password
separately). Whichever a human wires up on the platform side, document it
in the values file -- both are supported here so that choice doesn't have
to be made in this code.

Every other env var name deliberately reuses the exact names the Java
``workers``/``api`` modules already established for the same concept
(``KAFKA_BOOTSTRAP_SERVERS``, ``OTLP_COLLECTOR_ENDPOINT``,
``CLINVAR_SOURCE_VCF_URL``, ``CLINVAR_SOURCE_TBI_URL``,
``CLINVAR_REFDATA_PATH``) -- no reason to invent a second naming
convention for the same infrastructure just because this component
happens to be Python.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _database_dsn() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "clinvar")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")

    if not user or not password:
        # Same reasoning as api/application.yml's SPRING_DATASOURCE_PASSWORD:
        # a Service DNS name has a sane public default, a real credential
        # does not -- fail loudly rather than silently connecting with an
        # empty password.
        raise RuntimeError(
            "No DATABASE_URL set and POSTGRES_USER/POSTGRES_PASSWORD are "
            "missing -- clinvar-service has no way to reach its own "
            "Postgres instance (ADR 0019: a dedicated instance, not "
            "api's work_items database)."
        )
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=_database_dsn)

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka.kafka.svc.cluster.local:9092"
        )
    )
    clinvar_ingestion_topic: str = field(
        default_factory=lambda: os.environ.get(
            "CLINVAR_INGESTION_TOPIC", "clinvar.ingestion.completed"
        )
    )

    otlp_collector_endpoint: str = field(
        default_factory=lambda: os.environ.get(
            "OTLP_COLLECTOR_ENDPOINT",
            "http://otel-collector.otel.svc.cluster.local:4318/v1/traces",
        )
    )

    clinvar_refdata_path: str = field(
        default_factory=lambda: os.environ.get("CLINVAR_REFDATA_PATH", "/data/clinvar")
    )
    clinvar_source_vcf_url: str = field(
        default_factory=lambda: os.environ.get(
            "CLINVAR_SOURCE_VCF_URL",
            "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz",
        )
    )
    clinvar_source_tbi_url: str = field(
        default_factory=lambda: os.environ.get(
            "CLINVAR_SOURCE_TBI_URL",
            "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi",
        )
    )
    # Standard 5-field cron (APScheduler's CronTrigger.from_crontab), not
    # Spring's 6-field-with-seconds syntax -- weekly, Monday 03:00, same
    # off-peak slot ADR 0018 originally picked, no significance beyond
    # continuity with that choice.
    clinvar_ingestion_cron: str = field(
        default_factory=lambda: os.environ.get("CLINVAR_INGESTION_CRON", "0 3 * * MON")
    )

    service_name: str = "clinvar-service"


def get_settings() -> Settings:
    return Settings()
