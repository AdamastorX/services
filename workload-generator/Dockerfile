# workload-generator (backlog #45): deliberately the simplest thing that
# works -- a container running a script, not a service (the AC explicitly
# allows this). Mirrors clinvar-service/Dockerfile's shape (this project's
# established pattern for a non-JVM component: a build stage installing
# into a venv, a slim non-root runtime stage) since it's the same
# toolchain (Python), not because this component has anything else in
# common with clinvar-service.
#
# Base image digest reused from clinvar-service/Dockerfile, verified
# against the Docker Hub registry API 2026-07-26 (days-old, same week --
# not re-verified independently here; re-pin both together next time
# either is bumped).

# --- Build stage: install dependencies into a venv ------------------------
# python:3.12-slim
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de AS build

WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt \
 # Found by this component's own CI run, not copied from clinvar-service
 # (whose Dockerfile stops after the line above): Trivy's python-pkg scan
 # flagged pip's own vendored msgpack (CVE GHSA-6v7p-g79w-8964) and the
 # venv's ensurepip-seeded setuptools (CVE-2025-47273), neither of which
 # this runtime needs -- `python -m generator`'s only real dependency is
 # `requests` (confirmed: modern requests/urllib3/certifi/idna/
 # charset-normalizer import none of pip/setuptools/pkg_resources at
 # runtime). Uninstalling them here, before the venv is copied into the
 # runtime stage below, removes the vulnerable surface entirely rather
 # than accepting it as an unfixable transitive dependency it isn't.
 && /opt/venv/bin/pip uninstall -y pip setuptools wheel

# --- Runtime stage: slim, non-root ------------------------------------------
FROM python:3.12-slim@sha256:57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de

# Picks up any security patches published for this base image tag past
# whatever snapshot the pinned digest above represents (same reasoning
# clinvar-service/Dockerfile documents for its Alpine/Debian apt upgrade).
RUN apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 10001 -r app \
 && useradd -r -u 10001 -g app -s /usr/sbin/nologin app

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY generator ./generator

# Heartbeat/config default paths (generator/run.py, generator/config.py)
# live under paths this user can always write/read regardless of what's
# mounted where -- /tmp is always writable by a non-root user without any
# extra volume; the config path is expected to be a ConfigMap volume mount
# supplied by the Deployment (platform/kubernetes/workload-generator/
# deployment.yaml), not baked in here.
USER 10001
ENTRYPOINT ["python", "-m", "generator"]
