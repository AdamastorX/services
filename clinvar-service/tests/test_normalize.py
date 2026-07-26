"""Unit tests for app/normalize.py (backlog #39) -- trim-only query-side
normalization. No reference genome/FASTA is involved here: these prove
the pure string-trimming algorithm against constructed
extra-shared-context REF/ALT pairs, including the two indels already
present in tests/fixtures/fixture-release-1.vcf so the fixture-derived
endpoint test (test_lookup_endpoint.py) has a known-good baseline."""

from __future__ import annotations

from app.normalize import normalize_variant


def test_snv_is_unchanged():
    assert normalize_variant(100, "A", "T") == (100, "A", "T")


def test_already_minimal_indel_is_unchanged():
    # ClinVar's own canonical form for the BRCA2 fixture deletion.
    assert normalize_variant(32340300, "GT", "G") == (32340300, "GT", "G")


def test_trims_shared_trailing_context_for_deletion():
    # Same deletion as above, but the caller included one extra base of
    # right-side context that ClinVar's own normalization already strips.
    assert normalize_variant(32340300, "GTA", "GA") == (32340300, "GT", "G")


def test_trims_shared_leading_context_and_advances_pos():
    # One extra base of left-side context -- trimming it must shift pos
    # forward by exactly the number of bases removed.
    assert normalize_variant(32340299, "AGT", "AG") == (32340300, "GT", "G")


def test_trims_shared_trailing_context_for_insertion():
    # ClinVar's own canonical form for the BRCA1 fixture insertion is
    # T>TG at pos 43057062. An unnormalized query carrying one extra
    # shared trailing base normalizes back to it.
    assert normalize_variant(43057062, "TA", "TGA") == (43057062, "T", "TG")


def test_never_produces_an_empty_allele():
    # VCF alleles are never empty -- trimming must stop leaving at least
    # one anchor base on both sides even when fully "collapsible".
    pos, ref, alt = normalize_variant(500, "AT", "A")
    assert ref != "" and alt != ""
    assert (pos, ref, alt) == (500, "AT", "A")


def test_lowercase_input_is_normalized_to_uppercase():
    assert normalize_variant(32340300, "gta", "ga") == (32340300, "GT", "G")
