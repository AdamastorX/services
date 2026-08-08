# schemas

Backlog #96 / ADR 0033 (adamastorx repo): one JSON Schema per Kafka
topic that has a cross-language or cross-module consumer, describing
the real wire shape as it's actually produced -- not the AC text, not
a Java/Python type, the literal JSON.

- `<topic>.schema.json` -- the contract itself (JSON Schema, Draft
  2020-12).
- `examples/<topic>.valid.json` -- a real, representative payload that
  must pass.
- `examples/<topic>.invalid.json` -- a real, plausible drift shape (a
  renamed field, an out-of-range value, a stale enum value) that must
  fail -- the actual proof this check has teeth, not just that a
  schema file exists.

`scripts/validate_contracts.py` runs both checks for every topic in
CI (`event_contracts` job) -- it fails the build if a schema itself
isn't valid JSON Schema, if the golden example doesn't pass its own
schema, or if the broken example *does* pass (meaning the schema
would have let a real drift through undetected).

## Coverage

`stock.price.tick`, `news.article.published`, `news.sentiment.scored`
(the three M13 topics), `clinvar.ingestion.completed`, and
`work-items` -- the five topics named by backlog #96's own AC.

## What this is not, yet

These schemas are validated against checked-in example payloads, not
against each producer's own real serialized output at test time. A
producer's own test suite asserting its actual `ObjectMapper`/
`json.dumps` output validates against the same schema file (the
stronger half of "producer publishes the schema, consumers test
against it") is real, valuable follow-on work -- not done in this
pass. If a producer's real wire shape ever silently drifts from what's
committed here, this CI job won't catch it until someone updates the
example fixtures to match and notices the mismatch; it catches drift
in the schema/consumer-expectation relationship, not (yet) drift
between the schema and the producer's own live code.
