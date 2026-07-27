# clinvar-viewer

A small, static IGV.js page that visualizes a real ClinVar variant
lookup — chromosome, position, and clinical significance, marked on a
real genome-browser track — built to make `clinvar-service`/`api`'s
work visible as something other than logs, metrics, and traces.

No backend of its own: plain HTML/CSS/JS, served by nginx, calling
`api`'s real `GET /variants/lookup` over CORS (scoped to this origin
only, `VariantLookupController`'s `@CrossOrigin`). IGV.js itself loads
from a CDN in the visitor's own browser. No mocked or bundled data —
every result is whatever `clinvar-service` has actually ingested right
now.

## Local development

Just open `index.html` in a browser, or serve the directory with any
static file server. It calls `https://api.local.adamastorx.dev` — that
hostname needs to resolve and its CA needs to be trusted (see
`platform/kubernetes/cert-manager-issuers/README.md`).

## Deployment

`platform/kubernetes/clinvar-viewer/` + `argocd/apps/clinvar-viewer.yaml`.
Its own Ingress at `clinvar-viewer.local.adamastorx.dev` (same
`adamastorx-ca`/Traefik pattern every other service here uses).
