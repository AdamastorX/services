# services

The application layer of AdamastorX: gateway, API, workers, shared
libraries. Spring Boot, wired to Kafka/PostgreSQL/Redis per
`.claude/PROJECT.md` in `adamastorx`. Infrastructure to run this lives in
[platform](https://github.com/AdamastorX/platform).

## Build

Single Maven multi-module reactor
([ADR 0007](https://github.com/AdamastorX/adamastorx/blob/main/docs/adr/0007-services-maven-multi-module-build.md)):
the root `pom.xml` pins Spring Boot, Java, and plugin versions exactly once,
and every module builds together.

```sh
./mvnw verify
```

Requires **Java 25** (Temurin recommended). The Maven wrapper is committed —
no local Maven install needed.

Container images are built from the single multi-stage `Dockerfile` at the
repo root, with the module name as a build arg
([ADR 0008](https://github.com/AdamastorX/adamastorx/blob/main/docs/adr/0008-container-images-dockerfile-ghcr-sha-tags.md)):

```sh
docker build --build-arg MODULE=gateway -t ghcr.io/adamastorx/gateway:<sha> .
```

## Layout

| Dir | Contents |
|---|---|
| `gateway/` | Entrypoint service, routes external traffic (module) |
| `api/` | Core business logic, PostgreSQL + Redis backed (module arrives with services#2) |
| `workers/` | Kafka consumers, async processing (module arrives with services#3) |
| `shared/` | Libraries shared across the above — placeholder, extracted only when duplication actually hurts (ADR 0007) |

## Engineering context

Full project context, workflow, and agent roles: see the `adamastorx` repo's
`.claude/PROJECT.md` and `.claude/WORKFLOW.md`.
