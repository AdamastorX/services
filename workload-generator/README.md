# workload-generator

Python, standalone script (backlog #45). This project's second non-JVM
component (after `clinvar-service`), and deliberately the simplest shape
the AC allows: "a container running a script is a valid answer; it does
not need to be a Spring Boot service." No HTTP server, no Kafka consumer,
no database -- one process, one loop, one HTTP client against `api`.

## Why this exists

Every SLO, alert rule, dashboard, and chaos fact pack in this project so
far was produced against a cluster that is idle except when someone types
a `curl`. Both live chaos scenarios recorded the consequence in writing:
`ApiHighErrorRate` needed roughly five minutes of sustained non-zero
traffic and did not fire until several minutes of failing traffic were
generated *by hand* on purpose. This is a permanent, always-running,
low-key traffic generator so every other signal in the system has
something real to measure continuously.

This is explicitly **not** a resurrection of backlog #34 (a one-off
k6/vegeta "capacity baseline" run, correctly cut by ADR 0021/S7 -- see
`docs/roadmap/backlog.md`'s own item #45 text for why measuring a single
laptop node's throughput ceiling was never a useful number here). This
item derives no number and claims no baseline. Stated as plainly as
backlog #45 states it: traffic this generator authors itself is still not
real user demand, which is why the burn-rate policy work (#21b) stays
closed.

## What it drives, and how

Every loop tick (`generator/run.py`) picks one of:

- `POST /work-items` (real Kafka produce + PostgreSQL write), remembering
  the created id.
- `GET /work-items/{id}` against a remembered id (real cache-aside hit,
  `WorkItemCacheService`/ADR 0016) -- falls back to `GET /work-items` (the
  list) on a fresh process with nothing remembered yet.
- `GET /variants/lookup?rsid=...` with a skewed key distribution
  (`generator/keys.py`): a small hot set of real ClinVar GRCh38
  pathogenic variants already used elsewhere in this project's own tests
  (`rs80357906` BRCA1, `rs80359550` BRCA2), plus an occasional
  random-looking long-tail rsID.

On top of that normal mix, a configurable fraction of ticks
(`error_fraction`) are instead a **deliberate** guaranteed-miss request --
either `FAKE_RSID` (`rs00000000`, the same not-found fixture value
`VariantLookupIntegrationTest` already established) or a `GET
/work-items/{random-uuid}` that was never created.

## Rate shaping

`generator/rate.py` computes a diurnal (24h, cosine-shaped) curve between
`min_rps_fraction * target_rps` (the daily trough) and `target_rps` (the
daily peak) -- a flat rate teaches nothing about saturation, per the AC.

`target_rps` (and every other knob -- `error_fraction`, `hot_key_weight`,
the action weights) live in one JSON file, mounted from a ConfigMap
(`platform/kubernetes/workload-generator/configmap.yaml`) and re-read once
per loop tick (`generator/config.py`). Editing the ConfigMap in git and
letting ArgoCD sync it changes the running generator's behaviour with no
pod restart -- this is what makes the rate "a single configurable value
that can be turned down to near-zero without a redeploy" true in practice,
not just in theory. A bad edit (invalid JSON, an out-of-range value, a
wrong type) is logged and ignored -- the generator keeps running the last
known-good config rather than crashing.

## The distinguishing signal

Every request carries `User-Agent: AdamastorX-WorkloadGenerator/1.0 (...)`
(`generator/client.py`, `USER_AGENT`). `api` recognizes the same prefix
(`SyntheticTrafficObservationConvention.SYNTHETIC_USER_AGENT_PREFIX`,
`services/api/src/main/java/com/adamastorx/api/observability/
SyntheticTrafficObservationConvention.java`) and tags every HTTP server
metric with `traffic_source="synthetic"` vs `"real"` -- queryable in
Prometheus the same way any other label is, e.g.:

```promql
sum(rate(http_server_requests_seconds_count{traffic_source="synthetic"}[5m]))
sum(rate(http_server_requests_seconds_count{traffic_source="real"}[5m]))
```

Independently, every request this generator makes is also logged as a
single JSON line on stdout with `"synthetic": true` (`generator/run.py`'s
`log_event`) -- Alloy already ships container stdout to Loki with no
extra wiring, so the same distinction is queryable there too.

## Access pattern

Talks to `api` over in-cluster Kubernetes Service DNS
(`http://api.api.svc.cluster.local`, `API_BASE_URL` env var) -- the same
in-cluster pattern every other component uses, not the public Ingress
hostname. This is an internal, always-on workload, not a client
simulating an external user.

## Health

No HTTP server, so no `httpGet` liveness probe. `generator/run.py`'s main
loop touches a heartbeat file (`HEARTBEAT_PATH`, default `/tmp/
workload-generator-heartbeat`) at least once a second regardless of the
configured rate -- a near-zero rate still ticks, it just skips the actual
HTTP request that tick. `generator/healthcheck.py` (invoked via an `exec`
liveness probe) fails only if that heartbeat goes stale, which means the
process is genuinely stuck, never confusable with "rate turned down".

## Local verification

```
API_BASE_URL=http://localhost:8080 \
CONFIG_PATH=./local-config.json \
python -m generator
```

against a `kubectl port-forward svc/api -n api 8080:80` (or any other real
`api` instance) -- see the PR description for a real run's output and
what it confirmed.
