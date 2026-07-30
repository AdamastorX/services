"""Main loop (backlog #45): continuously drives POST/GET /work-items and
GET /variants/lookup against `api`, at a rate that follows the diurnal
curve in generator.rate, with the key skew in generator.keys and the
deliberate error fraction in generator.config, until the process is
killed. No exit condition -- "permanent demand, not a load test" is the
whole point (backlog #45's title).

Rate control is decoupled from the request cadence on purpose: the loop
ticks at least once a second (see the `time.sleep` at the bottom)
regardless of how low the configured rate is, so (a) `write_heartbeat`
keeps the liveness probe (generator/healthcheck.py) satisfied even when
the rate is turned down to near-zero -- exactly the AC's "can be turned
down to near-zero without a redeploy" case, which must not look like a
hang -- and (b) a config file edit (generator.config.load_config) is
picked up within about a second, not only between requests that might be
minutes apart at a low rate.
"""

from __future__ import annotations

import collections
import json
import logging
import os
import random
import sys
import time
import uuid
from pathlib import Path

from generator import client as client_mod
from generator import config as config_mod
from generator import keys as keys_mod
from generator import rate as rate_mod

log = logging.getLogger("generator")

DEFAULT_CONFIG_PATH = "/etc/workload-generator/config.json"
DEFAULT_HEARTBEAT_PATH = "/tmp/workload-generator-heartbeat"
SUMMARY_INTERVAL_SECONDS = 60.0
# Remembered created work-item ids, for real GET /work-items/{id} reads
# (see ApiClient.create_work_item's docstring). Bounded so a long-running
# process doesn't grow this unboundedly -- old ids age out, which is fine,
# they were only ever a means to exercise the read path, not data anyone
# needs to keep.
CREATED_IDS_MAXLEN = 500


def configure_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(message)s",
        stream=sys.stdout,
    )


def log_event(**fields) -> None:
    # One JSON object per line on stdout -- Alloy already ships every
    # container's stdout to Loki (observability#3/ADR 0013) with no extra
    # wiring needed, so `synthetic: true` here is queryable in Loki
    # exactly the same way `traffic_source="synthetic"` is queryable in
    # Prometheus on the api side (SyntheticTrafficObservationConvention) --
    # the AC's "log field" half of "a metric label or log field", covered
    # independently of that api-side change.
    log.info(json.dumps({"synthetic": True, **fields}, default=str))


def weighted_choice(rng: random.Random, options):
    total = sum(max(0.0, w) for _, w in options)
    if total <= 0:
        return rng.choice([label for label, _ in options])
    pick = rng.random() * total
    upto = 0.0
    for label, weight in options:
        upto += max(0.0, weight)
        if pick <= upto:
            return label
    return options[-1][0]


def choose_action(rng: random.Random, cfg: config_mod.Config) -> str:
    if rng.random() < cfg.error_fraction:
        return rng.choice(["error_variant_lookup", "error_work_item_read"])
    return weighted_choice(
        rng,
        [
            ("work_item_write", cfg.work_item_write_weight),
            ("work_item_read", cfg.work_item_read_weight),
            ("variant_lookup", cfg.variant_lookup_weight),
        ],
    )


def perform_action(
    action: str,
    client: client_mod.ApiClient,
    rng: random.Random,
    cfg: config_mod.Config,
    created_ids: "collections.deque[str]",
) -> client_mod.RequestOutcome:
    if action == "work_item_write":
        outcome = client.create_work_item(client_mod.random_work_item_message(rng))
        if outcome.work_item_id:
            created_ids.append(outcome.work_item_id)
        return outcome

    if action == "work_item_read":
        if created_ids:
            work_item_id = rng.choice(created_ids)
            return client.get_work_item(work_item_id)
        # Nothing remembered yet (fresh process) -- list instead of a
        # guaranteed miss, since a miss here would be an accident of
        # startup timing, not the deliberate error_fraction path below.
        return client.list_work_items()

    if action == "variant_lookup":
        rsid = keys_mod.pick_rsid(rng, cfg.hot_key_weight)
        variant_action = "variant_lookup_hot" if rsid in keys_mod.HOT_RSIDS else "variant_lookup_tail"
        return client.lookup_variant(rsid, action=variant_action)

    if action == "error_variant_lookup":
        return client.lookup_variant(keys_mod.FAKE_RSID, action="error_variant_lookup")

    if action == "error_work_item_read":
        return client.get_work_item(str(uuid.uuid4()))

    raise ValueError(f"unknown action: {action}")  # pragma: no cover - exhaustive above


def write_heartbeat(path: Path) -> None:
    try:
        path.write_text(str(time.time()))
    except OSError as exc:
        log.warning("failed to write heartbeat file %s: %s", path, exc)


def main() -> None:
    configure_logging()

    api_base_url = os.environ.get("API_BASE_URL", client_mod.DEFAULT_API_BASE_URL)
    config_path = Path(os.environ.get("CONFIG_PATH", DEFAULT_CONFIG_PATH))
    heartbeat_path = Path(os.environ.get("HEARTBEAT_PATH", DEFAULT_HEARTBEAT_PATH))
    seed_env = os.environ.get("GENERATOR_SEED")

    rng = random.Random(int(seed_env)) if seed_env else random.Random()
    client = client_mod.ApiClient(base_url=api_base_url)
    created_ids: "collections.deque[str]" = collections.deque(maxlen=CREATED_IDS_MAXLEN)

    cfg = config_mod.load_config(config_path, None)
    log_event(event="generator_started", api_base_url=api_base_url, user_agent=client_mod.USER_AGENT, config=cfg.__dict__)

    next_request_at = time.monotonic()
    last_summary_at = time.monotonic()
    counters: "collections.Counter[str]" = collections.Counter()

    while True:
        cfg = config_mod.load_config(config_path, cfg)
        now_monotonic = time.monotonic()
        current_rate = rate_mod.current_rate_rps(cfg.target_rps, cfg.cycle_seconds, cfg.min_rps_fraction, time.time())

        if current_rate > 0 and now_monotonic >= next_request_at:
            action = choose_action(rng, cfg)
            outcome = perform_action(action, client, rng, cfg, created_ids)
            log_event(
                event="generator_request",
                action=outcome.action,
                method=outcome.method,
                path=outcome.path,
                status=outcome.status_code,
                error=outcome.error,
                rsid=outcome.rsid,
                work_item_id=outcome.work_item_id,
            )
            counters[f"{outcome.action}:{'ok' if outcome.ok else 'error'}"] += 1
            next_request_at = now_monotonic + (1.0 / current_rate)
        elif current_rate <= 0:
            # Rate turned down to (near) zero -- recheck periodically
            # rather than spin, without ever exiting the process.
            next_request_at = now_monotonic + 5.0

        write_heartbeat(heartbeat_path)

        if now_monotonic - last_summary_at >= SUMMARY_INTERVAL_SECONDS:
            log_event(event="generator_summary", rate_rps=round(current_rate, 4), counts=dict(counters))
            counters.clear()
            last_summary_at = now_monotonic

        time.sleep(max(0.05, min(1.0, next_request_at - now_monotonic)))


if __name__ == "__main__":
    main()
