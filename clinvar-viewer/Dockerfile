# clinvar-viewer: static IGV.js variant explorer, no build step -- plain
# HTML/CSS/JS served by nginx. IGV.js itself loads from a CDN in the
# visitor's own browser (dist/igv.min.js), not bundled here.
#
# nginxinc/nginx-unprivileged, not plain nginx:alpine -- verified (not
# assumed) that stock nginx:alpine listens on 80 and runs as root by
# default; this image is the maintained non-root variant, listening on
# 8080 out of the box, matching this project's existing non-root
# container posture elsewhere.
#
# 1.31.3-alpine3.24-slim, not 1.27-alpine: the first pin (CI-confirmed,
# not just assumed) carried real, already-fixed CRITICAL/HIGH CVEs
# (openssl, libxml2, musl, nghttp2, zlib) against a stale Alpine base --
# the same base-image-CVE-gate friction the top-level Java Dockerfile
# already hit once (platform#6). -slim trims extra packages this static
# site doesn't need, on top of picking the most recently-patched tag.
#
# Base image digest verified against the Docker Hub registry API on
# 2026-07-27, same practice every other Dockerfile in this repo follows.
FROM nginxinc/nginx-unprivileged:1.31.3-alpine3.24-slim@sha256:22f839c5fb4007dc24d203a170a9e03fc185d660bfefc34ac6823a7aef085cbc

COPY index.html style.css app.js /usr/share/nginx/html/

EXPOSE 8080
