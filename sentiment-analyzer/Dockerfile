# sentiment-analyzer (backlog #80, ADR 0029): this project's second
# non-JVM component (after clinvar-service, ADR 0019). Mirrors
# clinvar-service/Dockerfile's shape exactly -- same toolchain (Python),
# same build-stage-venv/slim-non-root-runtime pattern, same reasons
# (see that file's own comments for the CVE-uninstall step below) --
# there is nothing about this service that would justify inventing a
# second Python Dockerfile shape.
#
# Base image digest reused from clinvar-service/Dockerfile (pinned there
# 2026-07-26, same week as this file) -- not re-verified independently
# against the registry API in this session; re-pin both together next
# time either is bumped.

# --- Build stage: install dependencies into a venv ------------------------
# python:3.12-slim
FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS build

WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt \
 # Same finding clinvar-service/Dockerfile and workload-generator/Dockerfile
 # both already made independently: pip's own vendored msgpack (CVE
 # GHSA-6v7p-g79w-8964) and the venv's ensurepip-seeded setuptools (CVE-
 # 2025-47273) are flagged by Trivy but never imported by this runtime
 # (`uvicorn app.main:app`, confluent-kafka, vaderSentiment, and their
 # own dependencies import none of pip/setuptools/pkg_resources) --
 # removing them here removes the vulnerable surface instead of leaving
 # this image's required CI check red for a reason unrelated to this
 # service's own code.
 && /opt/venv/bin/pip uninstall -y pip setuptools wheel

# --- Runtime stage: slim, non-root ------------------------------------------
FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea

# Picks up any security patches published for this base image tag past
# whatever snapshot the pinned digest above represents (same reasoning
# clinvar-service/Dockerfile documents for its own apt upgrade).
RUN apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 10001 -r app \
 && useradd -r -u 10001 -g app -s /usr/sbin/nologin app

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY app ./app

USER 10001
EXPOSE 8000
ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
