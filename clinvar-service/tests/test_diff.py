"""Release-diff computation tests (ADR 0019): a constructed two-release
fixture pair, same coordinate, classification changes between them, must
produce exactly the expected `changedKeys` list -- the CFTR variant at
7:117559600 C>T reclassified from Uncertain_significance (release n) to
Pathogenic (release n2), the same fixture pair ADR 0018's Java
`VariantInvalidationIntegrationTest` used.
"""

from __future__ import annotations

from app.diff import compute_changed_keys, redis_key
from tests.conftest import FIXTURES_DIR, bgzip_and_index


def test_changed_keys_detects_reclassified_variant(tmp_path):
    old_vcf = bgzip_and_index(FIXTURES_DIR / "fixture-invalidation-release-n.vcf", tmp_path / "old", "old")
    new_vcf = bgzip_and_index(FIXTURES_DIR / "fixture-invalidation-release-n2.vcf", tmp_path / "new", "new")

    changed = compute_changed_keys(old_vcf, new_vcf)

    assert changed == ["variantAnnotation:7:117559600:C:T"]


def test_changed_keys_empty_when_nothing_changed(tmp_path):
    vcf = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    assert compute_changed_keys(vcf, vcf) == []


def test_changed_keys_empty_on_first_ingestion(tmp_path):
    new_vcf = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    assert compute_changed_keys(None, new_vcf) == []


def test_redis_key_format_matches_apis_cache_key_convention():
    assert redis_key("7", 117559600, "C", "T") == "variantAnnotation:7:117559600:C:T"
