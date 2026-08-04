# visualizer

A small, static live view of `aggregator`'s (#81) real windowed
price/sentiment output for a fixed 5-ticker watchlist (AAPL, MSFT,
GOOGL, AMZN, TSLA) — built to make M13's pipeline (real Finnhub trade
ticks + real WSJ/MarketWatch news, VADER-scored, windowed by Kafka
Streams) visible as something other than logs, metrics, and traces.
Same spirit and shape as `clinvar-viewer` (#56/services#24): backlog
#82's own AC deliberately reuses that pattern rather than inventing a
sixth frontend architecture for this milestone.

No backend of its own: plain HTML/CSS/JS, served by nginx, polling
`aggregator`'s real `GET /aggregates` every 30 seconds over CORS
(scoped to this origin only — see
`platform/kubernetes/aggregator/ingress.yaml`). Chart.js loads from a
CDN in the visitor's own browser. No mocked or bundled data — every
number shown is whatever `aggregator`'s current 15-minute tumbling
window actually holds right now.

**The price/sentiment trend lines are built client-side, not read from
history the server has.** `aggregator`'s `GET /aggregates` is
deliberately "current window only, no history, no pagination" (see
`TickerAggregateResponse`'s own javadoc) — there is no history endpoint
to read. This page keeps its own in-memory buffer of up to the last 120
polls (60 minutes at the 30s interval) per ticker and charts that. It
reflects this browser tab's own session and resets on reload — stated
here plainly, the same honesty discipline every other real/no-mock
claim in this project uses.

**Both "no data yet" states are real and rendered honestly, not as an
error or a zero:**
- A ticker with no price tick in the current window (market closed,
  window just rolled over) shows "No price data yet for TICKER this
  window."
- A ticker with a price but no scored news this window (news is
  sparser than ticks) shows "no sentiment data" instead of a fabricated
  neutral/zero score.

## Local development

Just open `index.html` in a browser, or serve the directory with any
static file server. It calls `https://aggregator.local.adamastorx.test`
by default (see `app.js`'s `DEFAULT_API_BASE`, overridden by
`config.js` at deploy time) — that hostname needs to resolve and its CA
needs to be trusted (see
`platform/kubernetes/cert-manager-issuers/README.md`).

## Deployment

`platform/kubernetes/visualizer/` + `argocd/apps/visualizer.yaml`. Own
Ingress at `visualizer.local.adamastorx.test` (same
`adamastorx-ca`/Traefik pattern every other service here uses).
`config.js` is deploy-time-mounted from the `visualizer-config` Secret,
same mechanism `clinvar-viewer`'s own `config.js` uses (backlog #56) —
carries `ADAMASTORX_API_BASE` (aggregator's Ingress hostname) and,
if one is ever provisioned, `ADAMASTORX_API_KEY`. No key is provisioned
for this v1 — see `platform/kubernetes/aggregator/ingress.yaml`'s own
comment for the real, stated reasoning (aggregator has exactly one real
caller today, unlike `api`'s multi-tenant situation that motivated
backlog #56's per-tenant auth/rate-limit chain).

## Same-milestone sync discipline (M13)

Like every other M13 service, this is built and CI-verified here but
**not deployed live by this PR** — a human syncs
`argocd/apps/visualizer.yaml` after a fresh `kubectl describe node`
headroom check, matching #78-#81's own incremental-rollout discipline
(the M13 gate was owner-overridden 2026-08-02 to build-now/
deploy-incrementally, not all-at-once).
