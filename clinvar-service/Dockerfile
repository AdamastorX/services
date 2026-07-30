# clinvar-service (ADR 0019): this project's first non-JVM component, so
# it gets its own Dockerfile rather than a MODULE build-arg branch in the
# shared top-level one -- a different base image, a different dependency
# manager, nothing in common with the Maven multi-module build the
# top-level Dockerfile is scoped to.
#
# Base image digest verified against the Docker Hub registry API on
# 2026-07-26 (same practice the top-level Dockerfile already established
# for the Java images).

# --- Build stage: install dependencies into a venv ------------------------
# python:3.12-slim
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de AS build

WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt \
 # Pre-existing, unrelated to whatever else this commit touches --
 # Trivy's vulnerability DB caught up and started flagging pip's own
 # vendored msgpack (CVE GHSA-6v7p-g79w-8964) and the venv's
 # ensurepip-seeded setuptools (CVE-2025-47273) here too, same root
 # cause as services/workload-generator/Dockerfile hit first (see that
 # file's comment for the confirmation that runtime code never imports
 # either). Neither is needed once `uvicorn app.main:app` is actually
 # running -- removing them here removes the vulnerable surface instead
 # of leaving this image's required CI check red for a reason that has
 # nothing to do with clinvar-service's own code.
 && /opt/venv/bin/pip uninstall -y pip setuptools wheel

# --- Runtime stage: slim, non-root ------------------------------------------
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

# Picks up any security patches published for this base image tag past
# whatever snapshot the pinned digest above represents (same reasoning
# the top-level Dockerfile documents for its Alpine `apk upgrade`).
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
COPY migrations ./migrations

USER 10001
EXPOSE 8000
ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
