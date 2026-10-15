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
FROM nginxinc/nginx-unprivileged:1.31.6-alpine3.24-slim@sha256:c81a27f28bc2d9c2da8998444e653c7b85b9bbbaa92e44ef18d8920784e06507

# The pinned base lags Alpine's own security fixes: the image scan gate
# (HIGH/CRITICAL, fails the PR) found libcrypto3/libssl3 3.5.7-r0
# (CVE-2026-14456, fixed 3.5.8-r0) and pcre2 10.47-r1 (CVE-2026-103111,
# -86145, -89157, -89161, fixed 10.48/10.49-r0) in it. Same remedy the
# top-level Java Dockerfile already uses: `apk upgrade` in the runtime
# stage, then drop back to the image's own non-root user (101).
USER root
RUN apk upgrade --no-cache
USER 101

COPY index.html style.css app.js /usr/share/nginx/html/

EXPOSE 8080
