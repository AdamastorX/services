// Market Sentiment Pipeline visualizer -- polls aggregator's real
// GET /aggregates (backlog #81, ADR 0029/M13) over CORS from this page's
// own origin. No mock data, no bundled fixtures: every point plotted
// here came from a real HTTP response aggregator actually returned.
//
// backlog #82: API_BASE (and, if ever provisioned, API_KEY) come from
// config.js (loaded before this script, see index.html), rendered at
// deploy time from the visualizer-config Secret
// (platform/kubernetes/visualizer/deployment.yaml) -- same
// deploy-time-mounted-file mechanism backlog #56 established for
// clinvar-viewer, not a second one.
//
// No API key is actually provisioned for this v1 (see
// platform/kubernetes/aggregator/ingress.yaml's own comment for the
// full reasoning: aggregator has exactly one real caller, unlike api's
// two-plus-tenant situation that motivated backlog #56's per-tenant
// auth/rate-limit middleware chain in the first place; CORS alone is
// what's actually required for a browser to call it at all). The
// authHeaders() plumbing below still exists and is still exercised by
// the `typeof` guard -- if a key is ever provisioned later (a second
// real caller, real abuse observed), it starts working with a platform
// change only, no app.js change needed, the same forward-compatible
// shape clinvar-viewer's own app.js uses today.
const DEFAULT_API_BASE = "https://aggregator.local.adamastorx.test";
const API_BASE =
  typeof ADAMASTORX_API_BASE !== "undefined" && ADAMASTORX_API_BASE ? ADAMASTORX_API_BASE : DEFAULT_API_BASE;
const API_KEY = typeof ADAMASTORX_API_KEY !== "undefined" ? ADAMASTORX_API_KEY : null;

function authHeaders(extra = {}) {
  const headers = { ...extra };
  if (API_KEY) {
    headers.Authorization = "Basic " + btoa(`visualizer:${API_KEY}`);
  }
  return headers;
}

// Same 5-ticker watchlist aggregator/market-data-ingestor/news-ingestor
// already use (app.aggregator.watchlist in aggregator's own
// application.yml). Duplicated here deliberately, not shared config --
// same ADR 0007 "no shared config across modules/languages" precedent
// AggregatorProperties' own javadoc states for itself, extended to this
// static page (which has no module boundary to share across at all).
// Rendering the full watchlist here (not just whatever GET /aggregates
// happens to return) is what lets a ticker with no data yet this window
// show an honest "no data yet" card instead of just not existing on the
// page.
const WATCHLIST = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"];

// Poll interval: 30s. Reasoned against aggregator's own 15-minute
// tumbling window (app.aggregator.window = PT15M), not picked
// arbitrarily: 30s gives ~30 samples across one window, frequent enough
// that a real price/sentiment change during the window is visible
// within a poll or two (a genuinely "live" feel), while staying two
// orders of magnitude looser than the window itself -- polling at,
// say, 1s would just re-fetch an unchanged aggregate almost every time
// (real trade-tick/news volume here is sparse, see the hint text in
// index.html) for no new information, the same "don't build ahead of a
// real need" bar this project's other polling intervals use (e.g.
// api-key-ratelimit's own 5 req/s, sized against real observed traffic
// not a round number).
const POLL_INTERVAL_MS = 30_000;

// Client-side-only trend buffer: aggregator's API has no history
// endpoint (TickerAggregateResponse's own javadoc: "current tumbling
// window only -- no history, no pagination"), so any "live chart" has
// to be built from this page's own successive polls, not read from the
// server. Capped at 120 points (60 minutes at the 30s interval above)
// so a tab left open overnight doesn't grow its in-memory history
// unbounded.
const HISTORY_MAX_POINTS = 120;

const connDot = document.getElementById("conn-dot");
const connLabel = document.getElementById("conn-label");
const lastPollEl = document.getElementById("last-poll");
const restoringBanner = document.getElementById("restoring-banner");
const unreachableBanner = document.getElementById("unreachable-banner");
const grid = document.getElementById("ticker-grid");

document.getElementById("unreachable-host").textContent = API_BASE.replace(/^https?:\/\//, "");

// ticker -> { priceHistory: [{t,v}], sentimentHistory: [{t,v}], chart, card elements, hasData }
const tickers = new Map();

function fmtMoney(v) {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString("en-US", { style: "currency", currency: "USD", minimumFractionDigits: 2 });
}

function fmtPct(v) {
  if (v === null || v === undefined) return "—";
  const n = Number(v);
  return (n >= 0 ? "+" : "") + n.toFixed(2) + "%";
}

function fmtTime(d) {
  return d.toLocaleTimeString("en-US", { hour12: false });
}

function sentimentBucket(score) {
  // VADER's own documented compound-score thresholds (ADR 0029 decision
  // 3: sentiment-analyzer uses VADER) -- +-0.05 is VADER's own
  // established neutral band, not a value invented for this page.
  if (score > 0.05) return "positive";
  if (score < -0.05) return "negative";
  return "neutral";
}

function buildCard(ticker) {
  const card = document.createElement("article");
  card.className = "ticker-card";
  card.innerHTML = `
    <div class="ticker-head">
      <h2>${ticker}</h2>
      <span class="movement-badge" data-el="movement-badge">—</span>
    </div>
    <div class="ticker-body" data-el="body" hidden>
      <div class="price-row">
        <span class="price" data-el="price">—</span>
        <span class="price-window" data-el="window">—</span>
      </div>
      <canvas class="price-chart" data-el="chart" height="90"></canvas>
      <dl class="ticker-facts">
        <div><dt>Ticks this window</dt><dd data-el="tickCount">—</dd></div>
        <div><dt>First → Last</dt><dd data-el="firstLast">—</dd></div>
      </dl>
      <div class="sentiment-row" data-el="sentimentRow">
        <span class="sentiment-badge" data-el="sentimentBadge">—</span>
        <span class="sentiment-detail" data-el="sentimentDetail"></span>
      </div>
    </div>
    <div class="no-data" data-el="noData">No price data yet for ${ticker} this window.</div>
  `;
  grid.appendChild(card);

  const chartCanvas = card.querySelector('[data-el="chart"]');
  const chart = new Chart(chartCanvas, {
    type: "line",
    data: {
      datasets: [
        {
          label: `${ticker} last price (this session)`,
          data: [],
          borderColor: "#4fd1c5",
          backgroundColor: "rgba(79, 209, 197, 0.12)",
          borderWidth: 2,
          pointRadius: 2,
          tension: 0.25,
          fill: true,
        },
      ],
    },
    options: {
      // A plain category scale with pre-formatted HH:MM:SS labels
      // (below), not Chart.js's "time" scale -- that needs a separate
      // date-adapter script (chartjs-adapter-date-fns or luxon) on top
      // of chart.js itself, a second CDN dependency this page doesn't
      // need just to draw a wall-clock x-axis. Matches this backlog
      // item's own "no heavier a build chain than clinvar-viewer's
      // precedent" bar -- clinvar-viewer loads exactly one CDN script
      // (igv.min.js); this page loads exactly one too (chart.js).
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          ticks: { color: "#8894a6", maxTicksLimit: 5, autoSkip: true },
          grid: { color: "#232d3a" },
        },
        y: {
          ticks: { color: "#8894a6" },
          grid: { color: "#232d3a" },
        },
      },
      plugins: { legend: { display: false } },
    },
  });

  tickers.set(ticker, {
    card,
    chart,
    priceHistory: [],
    els: {
      movementBadge: card.querySelector('[data-el="movement-badge"]'),
      body: card.querySelector('[data-el="body"]'),
      price: card.querySelector('[data-el="price"]'),
      window: card.querySelector('[data-el="window"]'),
      tickCount: card.querySelector('[data-el="tickCount"]'),
      firstLast: card.querySelector('[data-el="firstLast"]'),
      sentimentBadge: card.querySelector('[data-el="sentimentBadge"]'),
      sentimentDetail: card.querySelector('[data-el="sentimentDetail"]'),
      noData: card.querySelector('[data-el="noData"]'),
    },
  });
}

WATCHLIST.forEach(buildCard);

function setConn(state, label) {
  connDot.className = "dot " + (state === "ok" ? "ok" : state === "err" ? "err" : "");
  connLabel.textContent = label;
}

function renderTicker(ticker, data) {
  const t = tickers.get(ticker);
  t.els.noData.hidden = true;
  t.els.body.hidden = false;

  const movement = Number(data.priceMovement);
  const badge = t.els.movementBadge;
  badge.textContent = `${movement >= 0 ? "▲" : "▼"} ${fmtPct(data.priceMovementPct)}`;
  badge.className = "movement-badge " + (movement > 0 ? "up" : movement < 0 ? "down" : "flat");

  t.els.price.textContent = fmtMoney(data.lastPrice);
  const windowStart = new Date(data.windowStart);
  const windowEnd = new Date(data.windowEnd);
  t.els.window.textContent = `window ${fmtTime(windowStart)}–${fmtTime(windowEnd)}`;
  t.els.tickCount.textContent = data.tickCount != null ? data.tickCount.toLocaleString("en-US") : "—";
  t.els.firstLast.textContent = `${fmtMoney(data.firstPrice)} → ${fmtMoney(data.lastPrice)}`;

  if (data.avgSentiment === null || data.avgSentiment === undefined) {
    // Real, common state (aggregator's own TickerAggregateResponse
    // javadoc: null when no news.sentiment.scored event landed for this
    // ticker in the current window, since news is sparse relative to
    // price ticks) -- rendered honestly as "no data", never coerced to
    // 0, which would misrepresent an actually-neutral score as "no
    // data" happened to compute.
    t.els.sentimentBadge.textContent = "no sentiment data";
    t.els.sentimentBadge.className = "sentiment-badge unknown";
    t.els.sentimentDetail.textContent = "this window";
  } else {
    const bucket = sentimentBucket(data.avgSentiment);
    t.els.sentimentBadge.textContent = `${bucket} (${data.avgSentiment.toFixed(2)})`;
    t.els.sentimentBadge.className = "sentiment-badge " + bucket;
    const n = data.sentimentSampleCount;
    t.els.sentimentDetail.textContent = `from ${n} article${n === 1 ? "" : "s"} this window`;
  }

  // Append to this session's own trend buffer -- see HISTORY_MAX_POINTS'
  // own comment for why this is client-side-only, not server history.
  if (data.lastPrice !== null && data.lastPrice !== undefined) {
    t.priceHistory.push({ label: fmtTime(new Date()), y: Number(data.lastPrice) });
    if (t.priceHistory.length > HISTORY_MAX_POINTS) {
      t.priceHistory.shift();
    }
    try {
      t.chart.data.labels = t.priceHistory.map((p) => p.label);
      t.chart.data.datasets[0].data = t.priceHistory.map((p) => p.y);
      t.chart.update("none");
    } catch (err) {
      // Degrade to "this tick's chart redraw skipped" rather than take
      // the whole page down -- the numeric price/movement above already
      // rendered correctly and is the primary signal, not the chart.
      console.error(`visualizer: chart redraw failed for ${ticker}`, err);
    }
  }
}

function renderNoData(ticker) {
  const t = tickers.get(ticker);
  t.els.body.hidden = true;
  t.els.noData.hidden = false;
}

async function poll() {
  let response;
  try {
    response = await fetch(`${API_BASE}/aggregates`, {
      headers: authHeaders({ Accept: "application/json" }),
    });
  } catch (err) {
    setConn("err", "unreachable");
    unreachableBanner.hidden = false;
    restoringBanner.hidden = true;
    return;
  }

  if (response.status === 503) {
    setConn("err", "restoring state");
    restoringBanner.hidden = false;
    unreachableBanner.hidden = true;
    return;
  }

  if (!response.ok) {
    setConn("err", `HTTP ${response.status}`);
    unreachableBanner.hidden = false;
    restoringBanner.hidden = true;
    return;
  }

  restoringBanner.hidden = true;
  unreachableBanner.hidden = true;
  setConn("ok", "aggregator reachable");
  lastPollEl.textContent = `last polled ${fmtTime(new Date())}`;

  const data = await response.json();
  const byTicker = new Map(data.map((d) => [d.ticker, d]));

  WATCHLIST.forEach((ticker) => {
    const d = byTicker.get(ticker);
    if (d) {
      renderTicker(ticker, d);
    } else {
      // Real, common state per aggregator's own AggregateQueryService
      // javadoc: no price tick has landed for this ticker in the
      // current window yet (outside real US market hours, the window
      // just rolled over, etc). Not an error -- rendered as an honest
      // "no data yet" card, not a blank/broken one.
      renderNoData(ticker);
    }
  });
}

// No separate /actuator/health "on load" ping, unlike clinvar-viewer's
// own app.js: clinvar-viewer's connection dot needs that ping because
// its main lookup() call only fires when a visitor actually searches,
// leaving a real gap (page load -> first search) the dot would
// otherwise sit at "unknown" through. This page's poll() fires
// immediately on load and every POLL_INTERVAL_MS after -- there is no
// such gap, and a second, independently-timed probe running concurrently
// with poll() would race it: caught live in this PR's own local
// verification, an in-flight health-ping success landing *after* a real
// poll() 503 briefly overwrote an accurate "restoring state" dot back to
// a false "reachable" one. Removed rather than patched around, since
// poll() alone is already a complete, accurate, real connectivity
// signal for this page's actual polling shape.
poll();
setInterval(poll, POLL_INTERVAL_MS);
