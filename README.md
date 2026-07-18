# services

The application layer of AdamastorX: gateway, API, workers, shared
libraries. Spring Boot, wired to Kafka/PostgreSQL/Redis per
`.claude/PROJECT.md` in `adamastorx`. Infrastructure to run this lives in
[platform](https://github.com/AdamastorX/platform).

Empty scaffold as of M0. First real content lands with milestone **M2
Distributed Application** — see backlog issues #11–#15 in the
`adamastorx` repo's `docs/roadmap/backlog.md`.

## Layout

| Dir | Contents |
|---|---|
| `gateway/` | Entrypoint service, routes external traffic |
| `api/` | Core business logic, PostgreSQL + Redis backed |
| `workers/` | Kafka consumers, async processing |
| `shared/` | Libraries shared across the above — kept small, extracted only when duplication actually hurts |

## Engineering context

Full project context, workflow, and agent roles: see the `adamastorx` repo's
`.claude/PROJECT.md` and `.claude/WORKFLOW.md`.
