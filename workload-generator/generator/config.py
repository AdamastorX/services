"""Live-reloadable configuration for the workload generator (backlog #45).

The AC's one hard requirement on configurability: "Rate is a single
configurable value that can be turned down to near-zero without a
redeploy." A plain environment variable can't do that -- changing an env
var on a Kubernetes Deployment means editing the manifest, which means a
new pod, which is a redeploy by any reasonable definition. Instead every
knob here (not just the rate) lives in one JSON file mounted from a
ConfigMap (`platform/kubernetes/workload-generator/configmap.yaml`):
editing the ConfigMap in git and letting ArgoCD sync it updates the
mounted file in place (kubelet's periodic sync, default within ~60s) with
no pod restart at all. `load_config` is called once per loop tick in
`generator.run`, not once at startup, so a lowered rate takes effect on
the generator's own next tick.

Kept deliberately fault-tolerant: a ConfigMap edit can be read mid-write
(kubelet swaps a symlink, but a straight read during that swap can still
occasionally hit a transient error), and a typo'd value should never crash
a long-running process. `load_config` falls back to the last known-good
`Config` (or `DEFAULT_CONFIG` if none has loaded yet) on any parse/read
failure, logging a warning instead of raising.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Config:
    # Peak requests/second of the diurnal curve in generator.rate --
    # "peak" because the curve itself never exceeds this value, it only
    # scales it down toward min_rps_fraction * target_rps over the day.
    # This is *the* single value the AC calls out: set it near 0.0 (not
    # exactly 0, see generator.rate) to pause real traffic without
    # touching anything else.
    target_rps: float = 0.5

    # Floor of the diurnal curve, as a fraction of target_rps (0..1). 0.15
    # means the trough of the day still runs at 15% of peak -- never
    # fully flat, never fully silent, so alert rules that need *some*
    # continuous signal (backlog #45's whole purpose) always have one.
    min_rps_fraction: float = 0.15

    # Period of one full diurnal cycle, in seconds. 86400 (24h) in
    # production. Overridable to something short (e.g. 300) only for a
    # local verification run, to observe the shape complete a full cycle
    # in minutes instead of a day -- see workload-generator/README.md.
    cycle_seconds: float = 86400.0

    # Fraction (0..1) of actions that are *deliberately* a guaranteed-miss
    # request (a fabricated rsID or a random work-item id) instead of the
    # normal weighted action below. This is the AC's "configurable
    # non-zero error/404 fraction" -- separate from the normal long-tail
    # variant lookups, which usually miss too but aren't guaranteed to.
    error_fraction: float = 0.05

    # Fraction (0..1) of *non-error* variant lookups that hit the small
    # hot rsID set (generator.keys.HOT_RSIDS) rather than a long-tail
    # random-looking rsID. This is the skew backlog #29's hot-key panel
    # needs something real to plot.
    hot_key_weight: float = 0.8

    # Relative weights (need not sum to 1 -- normalized in generator.run)
    # across the three real paths this generator drives.
    work_item_write_weight: float = 0.4
    work_item_read_weight: float = 0.25
    variant_lookup_weight: float = 0.35


DEFAULT_CONFIG = Config()


def load_config(path: Path, previous: Optional[Config] = None) -> Config:
    """Read `path` (a JSON object, any subset of Config's fields) and merge
    it over `previous` (or DEFAULT_CONFIG). Never raises -- a missing file,
    invalid JSON, or an out-of-range value logs a warning and returns
    `previous` (or the default) unchanged, so a bad edit degrades to "keep
    doing what it was already doing" rather than crashing the process.
    """
    base = previous if previous is not None else DEFAULT_CONFIG
    try:
        raw = path.read_text()
    except OSError as exc:
        log.warning("config file %s unreadable (%s) -- keeping previous config", path, exc)
        return base

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("config file %s is not valid JSON (%s) -- keeping previous config", path, exc)
        return base

    if not isinstance(data, dict):
        log.warning("config file %s did not contain a JSON object -- keeping previous config", path)
        return base

    known_fields = {f for f in Config.__dataclass_fields__}
    updates = {}
    for key, value in data.items():
        if key not in known_fields:
            log.warning("config file %s has unknown key %r -- ignoring", path, key)
            continue
        updates[key] = value

    try:
        candidate = replace(base, **updates)
        sane = _is_sane(candidate)
    except (TypeError, ValueError) as exc:
        # Covers both replace() rejecting a bad kwarg shape and _is_sane's
        # own comparisons raising on a wrong-typed value (e.g. a string
        # where a number belongs) -- either way, an invalid edit must
        # never crash a permanently-running process.
        log.warning("config file %s has an invalid value (%s) -- keeping previous config", path, exc)
        return base

    if not sane:
        log.warning("config file %s produced an out-of-range config -- keeping previous config", path)
        return base

    return candidate


def _is_sane(cfg: Config) -> bool:
    return (
        cfg.target_rps >= 0
        and 0.0 <= cfg.min_rps_fraction <= 1.0
        and cfg.cycle_seconds > 0
        and 0.0 <= cfg.error_fraction <= 1.0
        and 0.0 <= cfg.hot_key_weight <= 1.0
        and cfg.work_item_write_weight >= 0
        and cfg.work_item_read_weight >= 0
        and cfg.variant_lookup_weight >= 0
    )
