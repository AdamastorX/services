"""Proves the hot/long-tail skew backlog #45's hot-key AC actually needs."""

import random

from generator.keys import FAKE_RSID, HOT_RSIDS, pick_rsid


def test_hot_key_weight_one_always_returns_hot_set():
    rng = random.Random(1)
    for _ in range(50):
        assert pick_rsid(rng, hot_key_weight=1.0) in HOT_RSIDS


def test_hot_key_weight_zero_never_returns_hot_set():
    rng = random.Random(2)
    for _ in range(50):
        assert pick_rsid(rng, hot_key_weight=0.0) not in HOT_RSIDS


def test_long_tail_looks_like_an_rsid():
    rng = random.Random(3)
    value = pick_rsid(rng, hot_key_weight=0.0)
    assert value.startswith("rs")
    assert value[2:].isdigit()


def test_mixed_weight_produces_both_hot_and_tail_over_many_draws():
    rng = random.Random(4)
    draws = [pick_rsid(rng, hot_key_weight=0.8) for _ in range(200)]
    hot_count = sum(1 for d in draws if d in HOT_RSIDS)
    # Not an exact statistical assertion -- just proves the skew is real
    # (mostly hot, but not exclusively) rather than accidentally always
    # one or the other.
    assert 100 < hot_count < 200


def test_fake_rsid_is_not_a_hot_rsid():
    assert FAKE_RSID not in HOT_RSIDS
