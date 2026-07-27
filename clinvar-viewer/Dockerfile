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
# Base image digest verified against the Docker Hub registry API on
# 2026-07-27, same practice every other Dockerfile in this repo follows.
FROM nginxinc/nginx-unprivileged:1.27-alpine@sha256:65e3e85dbaed8ba248841d9d58a899b6197106c23cb0ff1a132b7bfe0547e4c0

COPY index.html style.css app.js /usr/share/nginx/html/

EXPOSE 8080
