// ClinVar Variant Explorer -- calls api's real GET /variants/lookup
// (services#24, ADR 0018/0021) over CORS from clinvar-viewer's own
// origin (VariantLookupController is the only endpoint that allows
// this cross-origin caller, everything else in `api` stays
// same-origin-only). No mock data, no bundled fixtures -- every result
// shown here is whatever the live cluster's clinvar-service actually
// has ingested right now.
//
// backlog #56: every call to api now also carries this tenant's own key
// as HTTP Basic auth, checked by the api-key-auth Traefik middleware on
// api's Ingress before the request ever reaches the pod.
// ADAMASTORX_API_KEY comes from config.js (loaded below, before this
// script -- see index.html), which is rendered from the
// clinvar-viewer-api-key Secret at deploy time
// (platform/kubernetes/clinvar-viewer/deployment.yaml). Stated plainly,
// not glossed over: a key shipped inside a static page served to any
// browser that loads it is not a real secret boundary -- anyone can read
// it via view-source. It still does real, honest work here (per-tenant
// attribution and rate limiting at the edge, matching this item's AC),
// it just isn't -- and cannot be, for a client-side-only app with no
// backend of its own -- a confidentiality control. See the decision note
// (docs/adr/0027 in the adamastorx repo) for the full reasoning.

const API_BASE = "https://api.local.adamastorx.test";
const API_KEY = typeof ADAMASTORX_API_KEY !== "undefined" ? ADAMASTORX_API_KEY : null;

function authHeaders(extra = {}) {
  const headers = { ...extra };
  if (API_KEY) {
    headers.Authorization = "Basic " + btoa(`clinvar-viewer:${API_KEY}`);
  }
  return headers;
}

const connDot = document.getElementById("conn-dot");
const connLabel = document.getElementById("conn-label");
const form = document.getElementById("search-form");
const input = document.getElementById("rsid-input");
const resultPanel = document.getElementById("result-panel");
const errorPanel = document.getElementById("error-panel");
const errorMessage = document.getElementById("error-message");

let igvBrowser = null;

const SIG_CLASS = {
  Pathogenic: "pathogenic",
  "Likely pathogenic": "likely-pathogenic",
  "Uncertain significance": "uncertain",
  "Likely benign": "likely-benign",
  Benign: "benign",
};

function sigClassFor(sig) {
  if (!sig) return "";
  const key = Object.keys(SIG_CLASS).find((k) => sig.toLowerCase().includes(k.toLowerCase()));
  return key ? SIG_CLASS[key] : "";
}

async function lookup(rsid) {
  hideError();
  setConn("connecting", `looking up ${rsid}…`);

  let response;
  try {
    response = await fetch(`${API_BASE}/variants/lookup?rsid=${encodeURIComponent(rsid)}`, {
      headers: authHeaders({ Accept: "application/json" }),
    });
  } catch (err) {
    setConn("err", "unreachable");
    showError(
      `Could not reach api.local.adamastorx.test. This page needs the ` +
        `AdamastorX cluster's Ingress resolvable and its CA trusted -- ` +
        `see platform/kubernetes/cert-manager-issuers/README.md.`
    );
    return;
  }

  if (response.status === 404) {
    setConn("ok", "clinvar-service reachable");
    showError(`No ClinVar record found for ${rsid} in the currently-ingested release.`);
    return;
  }
  if (response.status === 401) {
    setConn("err", "HTTP 401");
    showError(
      `api rejected this page's key (HTTP 401, backlog #56's api-key-auth ` +
        `Traefik middleware). config.js may be missing or stale -- see ` +
        `platform/kubernetes/clinvar-viewer/deployment.yaml.`
    );
    return;
  }
  if (!response.ok) {
    setConn("err", `HTTP ${response.status}`);
    showError(`api returned HTTP ${response.status} for ${rsid}.`);
    return;
  }

  const data = await response.json();
  setConn("ok", "clinvar-service reachable");
  renderResult(data);
}

function renderResult(v) {
  errorPanel.hidden = true;
  resultPanel.hidden = false;

  const badge = document.getElementById("sig-badge");
  badge.textContent = v.clinicalSignificance || "Unknown";
  badge.className = "sig-badge " + sigClassFor(v.clinicalSignificance);

  document.getElementById("result-rsid").textContent = v.rsid || `${v.chrom}:${v.pos}`;
  document.getElementById("fact-locus").textContent = `chr${v.chrom}:${v.pos.toLocaleString("en-US")}`;
  document.getElementById("fact-alleles").textContent = `${v.ref} → ${v.alt}`;
  document.getElementById("fact-review").textContent = (v.reviewStatus || "—").replaceAll("_", " ");
  document.getElementById("fact-release").textContent = v.clinvarReleaseId || "—";

  renderLocus(v);
}

async function renderLocus(v) {
  const chrom = `chr${v.chrom}`;
  const flank = 150;
  const locus = `${chrom}:${Math.max(1, v.pos - flank)}-${v.pos + flank}`;

  const trackColor =
    {
      pathogenic: "#e5484d",
      "likely-pathogenic": "#f0883e",
      uncertain: "#e3b341",
      "likely-benign": "#67c98f",
      benign: "#3fb950",
    }[sigClassFor(v.clinicalSignificance)] || "#64748b";

  const feature = {
    chr: chrom,
    start: v.pos - 1,
    end: v.pos - 1 + Math.max(v.ref.length, 1),
    name: `${v.rsid || ""} ${v.clinicalSignificance || ""}`.trim(),
    color: trackColor,
  };

  if (igvBrowser) {
    igvBrowser.removeTrackByName("clinvar-viewer-variant");
    await igvBrowser.search(locus);
  } else {
    igvBrowser = await igv.createBrowser(document.getElementById("igv-container"), {
      genome: "hg38",
      locus,
      showKaryo: false,
      showCenterGuide: true,
    });
  }

  igvBrowser.loadTrack({
    name: "clinvar-viewer-variant",
    type: "annotation",
    displayMode: "EXPANDED",
    color: trackColor,
    features: [feature],
  });
}

function setConn(state, label) {
  connDot.className = "dot " + (state === "ok" ? "ok" : state === "err" ? "err" : "");
  connLabel.textContent = label;
}

function showError(msg) {
  resultPanel.hidden = true;
  errorPanel.hidden = false;
  errorMessage.textContent = msg;
}

function hideError() {
  errorPanel.hidden = true;
}

form.addEventListener("submit", (e) => {
  e.preventDefault();
  const rsid = input.value.trim();
  if (rsid) lookup(rsid);
});

document.querySelectorAll(".chip").forEach((chip) => {
  chip.addEventListener("click", () => {
    const rsid = chip.dataset.rsid;
    input.value = rsid;
    lookup(rsid);
  });
});

// Ping /healthz-equivalent on load so the connection dot reflects
// reality immediately, not just "unknown" until the first search.
//
// Deliberately NOT sending the backlog #56 key here, and left on
// no-cors: /actuator/health has no @CrossOrigin config on the api side
// (only VariantLookupController does), so a normal 'cors'-mode request
// would fail the browser's CORS check outright rather than return a
// real status -- switching mode to add the header would make this
// *less* accurate, not more. `no-cors` mode also cannot carry a custom
// Authorization header at all (browsers restrict no-cors requests to
// CORS-safelisted headers), so this ping was never going to be
// authenticated either way. Net effect, stated plainly: this dot means
// "the network path to api's Ingress is up," not "this page is
// authorized" -- the real authorization signal is whatever `lookup()`
// above gets back on an actual search (including a real 401 if the key
// is missing/stale, handled explicitly there).
fetch(`${API_BASE}/actuator/health`, { mode: "no-cors" })
  .then(() => setConn("ok", "clinvar-service reachable"))
  .catch(() => setConn("err", "api unreachable"));
