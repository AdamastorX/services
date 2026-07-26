"""Real tabix-indexed-VCF point-query tests against a small fixture built
from real ClinVar GRCh38 BRCA1/BRCA2 records (rs80357906, rs80359550) --
the same fixture the Java `api`/`workers` modules used for ADR 0018."""

from __future__ import annotations

import datetime

from app.vcf_query import query, read_published_date, rebuild_tabix_index
from tests.conftest import FIXTURES_DIR, bgzip_and_index


def test_query_returns_brca1_pathogenic_variant(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    hit = query(vcf_gz, "17", 43057062, "T", "TG")

    assert hit is not None
    assert hit.clinical_significance == "Pathogenic"
    assert hit.review_status == "reviewed_by_expert_panel"
    assert hit.rsid == "rs80357906"


def test_query_returns_brca2_pathogenic_variant(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    hit = query(vcf_gz, "13", 32340300, "GT", "G")

    assert hit is not None
    assert hit.clinical_significance == "Pathogenic"
    assert hit.rsid == "rs80359550"


def test_query_normalizes_chr_prefix(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    hit = query(vcf_gz, "chr17", 43057062, "T", "TG")

    assert hit is not None
    assert hit.rsid == "rs80357906"


def test_query_no_match_for_wrong_allele(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    # Right position, wrong ALT allele -- must not fuzzy-match.
    assert query(vcf_gz, "17", 43057062, "T", "TGG") is None


def test_query_no_match_for_unknown_position(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    assert query(vcf_gz, "1", 12345, "A", "T") is None


def test_read_published_date_from_vcf_header(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)

    assert read_published_date(vcf_gz) == datetime.date(2026, 7, 20)


def test_comma_containing_review_status_reconstructed_correctly(tmp_path):
    """Load-bearing regression test: CLNREVSTAT is a Number=. INFO field, so
    pysam splits its raw text on commas. A real ClinVar review-status value
    can itself legitimately contain a comma
    ("criteria_provided,_single_submitter") -- verified directly against
    pysam's raw parsing (not assumed) that this splits into two tokens,
    which must be rejoined with "," to recover the exact original string.
    """
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-invalidation-release-n.vcf", tmp_path)

    hit = query(vcf_gz, "7", 117559600, "C", "T")

    assert hit is not None
    assert hit.review_status == "criteria_provided,_single_submitter"
    assert hit.clinical_significance == "Uncertain_significance"


def test_rebuild_tabix_index_produces_a_queryable_index(tmp_path):
    vcf_gz = bgzip_and_index(FIXTURES_DIR / "fixture-release-1.vcf", tmp_path)
    tbi_path = vcf_gz.with_suffix(vcf_gz.suffix + ".tbi")
    tbi_path.unlink()
    assert not tbi_path.exists()

    rebuild_tabix_index(vcf_gz)

    assert tbi_path.exists()
    hit = query(vcf_gz, "17", 43057062, "T", "TG")
    assert hit is not None
    assert hit.clinical_significance == "Pathogenic"
