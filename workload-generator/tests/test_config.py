"""Proves the ConfigMap-hot-reload mechanism backlog #45's rate AC depends
on ("Rate is a single configurable value that can be turned down to
near-zero without a redeploy") actually behaves the way
platform/kubernetes/workload-generator/deployment.yaml assumes: valid
edits apply, and anything else degrades to "keep the last known-good
config" rather than crashing the process.
"""

import json

from generator.config import DEFAULT_CONFIG, Config, load_config


def test_missing_file_returns_default(tmp_path):
    cfg = load_config(tmp_path / "does-not-exist.json")
    assert cfg == DEFAULT_CONFIG


def test_missing_file_keeps_previous_config(tmp_path):
    previous = Config(target_rps=0.01)
    cfg = load_config(tmp_path / "does-not-exist.json", previous)
    assert cfg == previous


def test_valid_partial_update_overrides_only_given_fields(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"target_rps": 0.02}))
    cfg = load_config(path, DEFAULT_CONFIG)
    assert cfg.target_rps == 0.02
    # Untouched fields keep the previous config's values.
    assert cfg.error_fraction == DEFAULT_CONFIG.error_fraction


def test_rate_can_be_turned_down_to_near_zero(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"target_rps": 0.0001}))
    cfg = load_config(path, DEFAULT_CONFIG)
    assert cfg.target_rps == 0.0001


def test_invalid_json_keeps_previous_config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text("{not valid json")
    previous = Config(target_rps=0.3)
    cfg = load_config(path, previous)
    assert cfg == previous


def test_non_object_json_keeps_previous_config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps([1, 2, 3]))
    previous = Config(target_rps=0.3)
    cfg = load_config(path, previous)
    assert cfg == previous


def test_unknown_key_is_ignored_not_fatal(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"target_rps": 0.5, "totally_made_up_field": 1}))
    cfg = load_config(path, DEFAULT_CONFIG)
    assert cfg.target_rps == 0.5


def test_out_of_range_value_keeps_previous_config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"error_fraction": 5.0}))
    previous = Config(target_rps=0.3)
    cfg = load_config(path, previous)
    assert cfg == previous


def test_wrong_type_value_keeps_previous_config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"target_rps": "not-a-number"}))
    previous = Config(target_rps=0.3)
    cfg = load_config(path, previous)
    assert cfg == previous
