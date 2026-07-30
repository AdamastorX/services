"""Proves the diurnal curve backlog #45 requires actually varies (AC: "a
flat rate teaches nothing about saturation") rather than trusting the
cosine math by inspection.
"""

from generator.rate import current_rate_rps


def test_zero_target_is_always_zero():
    assert current_rate_rps(0.0, 86400, 0.15, now=12345.0) == 0.0
    assert current_rate_rps(-1.0, 86400, 0.15, now=12345.0) == 0.0


def test_non_positive_cycle_returns_flat_target():
    assert current_rate_rps(2.0, 0, 0.15, now=12345.0) == 2.0
    assert current_rate_rps(2.0, -10, 0.15, now=12345.0) == 2.0


def test_trough_at_start_of_cycle():
    target, cycle, floor = 2.0, 100.0, 0.15
    rate = current_rate_rps(target, cycle, floor, now=0.0)
    assert abs(rate - target * floor) < 1e-9


def test_peak_at_half_cycle():
    target, cycle, floor = 2.0, 100.0, 0.15
    rate = current_rate_rps(target, cycle, floor, now=cycle / 2)
    assert abs(rate - target) < 1e-9


def test_curve_stays_within_bounds_across_a_full_cycle():
    target, cycle, floor = 3.0, 200.0, 0.2
    samples = [current_rate_rps(target, cycle, floor, now=t) for t in range(0, 220, 5)]
    assert min(samples) >= target * floor - 1e-9
    assert max(samples) <= target + 1e-9


def test_curve_is_not_flat():
    # The whole point of this module (backlog #45's "flat rate teaches
    # nothing" line) -- sampling across a cycle must show real variance,
    # not a constant.
    target, cycle, floor = 1.0, 3600.0, 0.1
    samples = {round(current_rate_rps(target, cycle, floor, now=t), 6) for t in range(0, 3600, 300)}
    assert len(samples) > 1


def test_repeats_after_one_full_cycle():
    target, cycle, floor = 1.5, 500.0, 0.25
    a = current_rate_rps(target, cycle, floor, now=123.0)
    b = current_rate_rps(target, cycle, floor, now=123.0 + cycle)
    assert abs(a - b) < 1e-9
