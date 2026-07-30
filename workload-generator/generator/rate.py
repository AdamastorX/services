"""Diurnal request-rate curve (backlog #45 AC: "Request rate follows a
shaped, non-flat pattern over the day... a flat rate teaches nothing about
saturation").

A single cosine cycle between `min_rps_fraction * target_rps` (the trough)
and `target_rps` (the peak), repeating every `cycle_seconds`. Deliberately
smooth rather than a step schedule -- a continuous curve gives every
dashboard time-series a visibly varying shape at any zoom level, not just
at the moment of a step change, and needs no extra "which hour is the
step" configuration on top of the two numbers already in Config.
"""

from __future__ import annotations

import math


def current_rate_rps(target_rps: float, cycle_seconds: float, min_rps_fraction: float, now: float) -> float:
    """Return the instantaneous target rate (requests/second) at wall-clock
    time `now` (seconds, e.g. `time.time()`).

    `phase` is `now`'s position within the current cycle, in [0, 1).
    `factor` maps that to [min_rps_fraction, 1.0] via a raised cosine, so
    the curve peaks once and troughs once per cycle_seconds with a smooth
    (not linear) transition between them -- shaped like real diurnal
    traffic, one broad daily hump, not a sawtooth.
    """
    if target_rps <= 0:
        return 0.0
    if cycle_seconds <= 0:
        return target_rps

    phase = (now % cycle_seconds) / cycle_seconds
    factor = min_rps_fraction + (1.0 - min_rps_fraction) * (0.5 - 0.5 * math.cos(2 * math.pi * phase))
    return target_rps * factor
