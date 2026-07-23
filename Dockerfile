# Shared multi-stage build for all services modules (ADR 0008).
# The module to build/run is selected with a build arg:
#
#   docker build --build-arg MODULE=gateway -t ghcr.io/adamastorx/gateway:<sha> .
#
# Base image digests verified against the Docker Hub registry API on
# 2026-07-23.
#
# Runtime stage uses the Alpine variant, not the default Ubuntu/rockcraft-built
# `eclipse-temurin:25-jre` we shipped until platform#6: that image is built
# through Canonical's rockcraft pipeline and bundles `pebble`, Canonical's
# Go-based process supervisor for "rock" images. We don't need a supervisor
# (our ENTRYPOINT is a direct `java -jar`, no init/process-management), and
# pebble's bundled Go runtime (golang.org/x/net + stdlib) carried 5 HIGH CVEs
# with no newer patched digest published yet under that tag. The Alpine
# variant comes from a different pipeline entirely (Alpine minirootfs + apk
# packages + the GPG-verified Adoptium JRE release tarball) and doesn't
# bundle pebble at all, so it has none of these findings.
#
# Build stage keeps the Ubuntu-based `eclipse-temurin:25-jdk`: it's discarded
# after `package` runs (only the built jar is COPYed into the runtime stage
# below), so pebble living in that stage's OS never reaches the shipped
# image and doesn't need to be avoided the same way.

ARG MODULE

# --- Build stage: full JDK + the repo's Maven wrapper ---------------------
# eclipse-temurin:25-jdk
FROM eclipse-temurin:25-jdk@sha256:201fbb8886b2d273218aa3a192f0afbf7b5ff65ee8cc6ef47f5dce2171f013ea AS build
ARG MODULE

WORKDIR /workspace
COPY . .
RUN ./mvnw --batch-mode --no-transfer-progress -pl "${MODULE}" -am package -DskipTests

# --- Runtime stage: JRE only, non-root, Alpine (no pebble) -----------------
# eclipse-temurin:25-jre-alpine
FROM eclipse-temurin:25-jre-alpine@sha256:28db6fdf60e38945e43d840c0333aeaec66c15943070104f7586fd3c9d1665b0
ARG MODULE

# This image tag's apk package snapshot (2026-06-22) lags Alpine 3.23's own
# security repo: Trivy caught libexpat and p11-kit CVEs with fixes already
# published upstream (CVE-2026-56131/56407/56408, CVE-2026-2100). Rather than
# wait for Adoptium to republish the tag, pull the fixes in at build time.
RUN apk update && apk upgrade --no-cache

# Alpine ships busybox's adduser/addgroup, not shadow-utils' useradd.
RUN addgroup -g 10001 -S app \
 && adduser -S -D -H -u 10001 -G app -s /bin/false app

WORKDIR /app
# The Spring Boot Maven plugin repackages the module jar in place, so
# <module>/target/<module>-<version>.jar is the executable fat jar (the thin
# pre-repackage jar keeps a .jar.original suffix and does not match the glob).
COPY --from=build /workspace/${MODULE}/target/${MODULE}-*.jar /app/app.jar

USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
