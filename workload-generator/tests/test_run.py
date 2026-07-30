"""Proves the action-selection logic (weighted choice, error_fraction
override, remembered work-item ids) behaves the way backlog #45's AC
requires, against a fake ApiClient -- no real HTTP, no responses library
needed here, this is pure selection-logic behaviour.
"""

import collections
import random

from generator.client import RequestOutcome
from generator.config import Config
from generator.keys import FAKE_RSID, HOT_RSIDS
from generator.run import choose_action, perform_action, weighted_choice


class FakeApiClient:
    """Records every call it receives; returns a canned RequestOutcome per
    method so perform_action's branching can be asserted without a real
    network call.
    """

    def __init__(self):
        self.calls = []

    def create_work_item(self, message):
        self.calls.append(("create_work_item", message))
        return RequestOutcome(
            action="work_item_write", method="POST", path="/work-items", status_code=202, error=None, work_item_id="new-id"
        )

    def get_work_item(self, work_item_id):
        self.calls.append(("get_work_item", work_item_id))
        return RequestOutcome(
            action="work_item_read", method="GET", path=f"/work-items/{work_item_id}", status_code=200, error=None
        )

    def list_work_items(self):
        self.calls.append(("list_work_items",))
        return RequestOutcome(action="work_item_list", method="GET", path="/work-items", status_code=200, error=None)

    def lookup_variant(self, rsid, action="variant_lookup"):
        self.calls.append(("lookup_variant", rsid, action))
        return RequestOutcome(action=action, method="GET", path="/variants/lookup", status_code=200, error=None, rsid=rsid)


def test_weighted_choice_never_picks_a_zero_weight_option():
    rng = random.Random(1)
    options = [("a", 1.0), ("b", 0.0)]
    picks = {weighted_choice(rng, options) for _ in range(100)}
    assert picks == {"a"}


def test_weighted_choice_covers_all_positive_weight_options():
    rng = random.Random(2)
    options = [("a", 1.0), ("b", 1.0), ("c", 1.0)]
    picks = {weighted_choice(rng, options) for _ in range(200)}
    assert picks == {"a", "b", "c"}


def test_choose_action_error_fraction_one_is_always_an_error_action():
    rng = random.Random(3)
    cfg = Config(error_fraction=1.0)
    for _ in range(20):
        assert choose_action(rng, cfg) in ("error_variant_lookup", "error_work_item_read")


def test_choose_action_error_fraction_zero_never_picks_error_action():
    rng = random.Random(4)
    cfg = Config(error_fraction=0.0)
    for _ in range(50):
        assert choose_action(rng, cfg) in ("work_item_write", "work_item_read", "variant_lookup")


def test_perform_work_item_write_remembers_the_created_id():
    client = FakeApiClient()
    rng = random.Random(5)
    cfg = Config()
    created_ids = collections.deque(maxlen=10)

    outcome = perform_action("work_item_write", client, rng, cfg, created_ids)

    assert outcome.work_item_id == "new-id"
    assert "new-id" in created_ids


def test_perform_work_item_read_uses_a_remembered_id_when_available():
    client = FakeApiClient()
    rng = random.Random(6)
    cfg = Config()
    created_ids = collections.deque(["known-id"], maxlen=10)

    perform_action("work_item_read", client, rng, cfg, created_ids)

    assert client.calls == [("get_work_item", "known-id")]


def test_perform_work_item_read_falls_back_to_list_when_nothing_remembered():
    client = FakeApiClient()
    rng = random.Random(7)
    cfg = Config()
    created_ids = collections.deque(maxlen=10)

    perform_action("work_item_read", client, rng, cfg, created_ids)

    assert client.calls == [("list_work_items",)]


def test_perform_variant_lookup_marks_hot_vs_tail():
    client = FakeApiClient()
    cfg = Config(hot_key_weight=1.0)
    rng = random.Random(8)
    created_ids = collections.deque(maxlen=10)

    outcome = perform_action("variant_lookup", client, rng, cfg, created_ids)

    assert outcome.rsid in HOT_RSIDS
    assert outcome.action == "variant_lookup_hot"


def test_perform_error_variant_lookup_uses_fake_rsid():
    client = FakeApiClient()
    rng = random.Random(9)
    cfg = Config()
    created_ids = collections.deque(maxlen=10)

    outcome = perform_action("error_variant_lookup", client, rng, cfg, created_ids)

    assert outcome.rsid == FAKE_RSID


def test_perform_error_work_item_read_uses_a_random_id_not_a_remembered_one():
    client = FakeApiClient()
    rng = random.Random(10)
    cfg = Config()
    created_ids = collections.deque(["known-id"], maxlen=10)

    outcome = perform_action("error_work_item_read", client, rng, cfg, created_ids)

    assert outcome.path != "/work-items/known-id"
