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


_VCF_HEADER = """##fileformat=VCFv4.1
##fileDate=2026-08-14
##source=ClinVar
##reference=GRCh38
##ID=<Description="ClinVar Variation ID">
##INFO=<ID=ALLELEID,Number=1,Type=Integer,Description="the ClinVar Allele ID">
##INFO=<ID=CLNSIG,Number=.,Type=String,Description="Aggregate germline classification for this single variant; multiple values are separated by a vertical bar">
##INFO=<ID=CLNVC,Number=1,Type=String,Description="Variant type">
##INFO=<ID=CLNVCSO,Number=1,Type=String,Description="Sequence Ontology id for variant type">
##INFO=<ID=GENEINFO,Number=1,Type=String,Description="Gene(s) for the variant reported as gene symbol:gene id.">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
"""


def _write_vcf(tmp_path, name: str, records: str) -> Path:
    from pathlib import Path

    plain = tmp_path / f"{name}-src.vcf"
    plain.write_text(_VCF_HEADER + records)
    return plain


def test_changed_keys_flags_a_real_deletion_but_not_an_already_unclassified_one(tmp_path):
    """backlog #132: compute_changed_keys now pops matched keys out of a
    single `old` dict as it streams `new`, instead of building a second
    full `new` dict -- whatever remains in `old` afterward is exactly
    "present in old, never mentioned in new" (a real deletion). Proves
    the None-vs-missing semantics this refactor deliberately preserved:
    a real deletion of a variant that HAD a classification must be
    flagged (api's cache holds a real, now-stale answer to invalidate);
    a "deletion" of a variant old already had no classification for must
    not be (nothing real to invalidate -- old.get(key) and a missing key
    were always indistinguishable here, unchanged by this refactor).
    """
    old_records = (
        "1\t1000\t1\tA\tT\t.\t.\tALLELEID=1;CLNSIG=Pathogenic;CLNVC=single_nucleotide_variant;CLNVCSO=SO:0001483;GENEINFO=X:1\n"
        "2\t2000\t2\tG\tC\t.\t.\tALLELEID=2;CLNVC=single_nucleotide_variant;CLNVCSO=SO:0001483;GENEINFO=X:1\n"
    )
    new_records = "3\t3000\t3\tA\tG\t.\t.\tALLELEID=3;CLNSIG=Benign;CLNVC=single_nucleotide_variant;CLNVCSO=SO:0001483;GENEINFO=X:1\n"

    old_vcf = bgzip_and_index(_write_vcf(tmp_path, "old", old_records), tmp_path / "old", "old")
    new_vcf = bgzip_and_index(_write_vcf(tmp_path, "new", new_records), tmp_path / "new", "new")

    changed = compute_changed_keys(old_vcf, new_vcf)

    assert changed == [
        "variantAnnotation:1:1000:A:T",  # real deletion, old had a real CLNSIG
        "variantAnnotation:3:3000:A:G",  # new-only, has a real CLNSIG
    ]
    # 2:2000:G:C -- deleted, but old never had a real CLNSIG for it either
    # (no cached real answer anywhere to invalidate) -- correctly absent.
