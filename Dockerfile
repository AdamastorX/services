# Shared multi-stage build for all services modules (ADR 0008).
# The module to build/run is selected with a build arg:
#
#   docker build --build-arg MODULE=gateway -t ghcr.io/adamastorx/gateway:<sha> .
#
# Base image digests verified against the Docker Hub registry API on
# 2026-07-19; both correspond to the multi-arch manifest lists pushed
# 2026-07-16 (Temurin 25.0.3+9, Ubuntu Noble).

ARG MODULE

# --- Build stage: full JDK + the repo's Maven wrapper ---------------------
# eclipse-temurin:25-jdk
FROM eclipse-temurin:25-jdk@sha256:201fbb8886b2d273218aa3a192f0afbf7b5ff65ee8cc6ef47f5dce2171f013ea AS build
ARG MODULE

WORKDIR /workspace
COPY . .
RUN ./mvnw --batch-mode --no-transfer-progress -pl "${MODULE}" -am package -DskipTests

# --- Runtime stage: JRE only, non-root ------------------------------------
# eclipse-temurin:25-jre
FROM eclipse-temurin:25-jre@sha256:681c543d6f36c50f45e9b5226930a46203dcfa351d3670e9d0bdf0dabae53539
ARG MODULE

RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app
# The Spring Boot Maven plugin repackages the module jar in place, so
# <module>/target/<module>-<version>.jar is the executable fat jar (the thin
# pre-repackage jar keeps a .jar.original suffix and does not match the glob).
COPY --from=build /workspace/${MODULE}/target/${MODULE}-*.jar /app/app.jar

USER 10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
