# visualizer (backlog #82, M13, ADR 0029): static live price/sentiment
# chart for aggregator's (#81) watchlist -- no build step, plain
# HTML/CSS/JS served by nginx. Chart.js loads from a CDN in the
# visitor's own browser (dist/chart.umd.min.js), not bundled here --
# same "no backend, no build tool" shape as clinvar-viewer's own
# Dockerfile (services/clinvar-viewer/Dockerfile), reused deliberately
# per this item's own AC ("no heavier a build chain than clinvar-viewer's
# existing precedent"), not reinvented.
#
# Identical base image pin to clinvar-viewer's own Dockerfile, reused
# rather than re-justified from scratch: same non-root
# nginxinc/nginx-unprivileged variant (listens on 8080, not root, not
# stock nginx:alpine), same already-CVE-checked -slim tag. Digest
# re-confirmed live against the Docker Hub registry API on 2026-08-04
# (this PR's own session) -- still the exact same manifest-list digest
# clinvar-viewer already pins, i.e. nothing has moved since 2026-07-27.
FROM nginxinc/nginx-unprivileged:1.31.3-alpine3.24-slim@sha256:22f839c5fb4007dc24d203a170a9e03fc185d660bfefc34ac6823a7aef085cbc

COPY index.html style.css app.js /usr/share/nginx/html/

EXPOSE 8080
