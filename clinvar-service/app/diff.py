"""Release-diff computation (ADR 0019).

This is the fix for the second half of ADR 0018's original mistake:
``api`` should never need to re-read a tabix file itself just to know
what to invalidate. clinvar-service holds both the old and new release's
tabix files locally (app/paths.py's retention policy keeps exactly the
current and immediately-previous release on disk for this reason) and
computes the full set of changed variant keys itself, publishing the
list on Kafka.

Deliberate scope difference from ADR 0018's Redis-``SCAN``-based
approach: that code only ever diffed keys already present in ``api``'s
Redis cache, because ``api`` was the one doing the diffing and had no
other way to bound the cost. Here, the diff is a full old-vs-new
comparison of every variant present in either release -- clinvar-service
doesn't have (and, per this ADR, must never need) visibility into what
``api`` actually has cached, so it can't scope the diff to "only what's
cached" the way the old code did. For a fixture-sized VCF this is
trivially cheap; at full ClinVar scale (a few million records) this is
O(n) two-pass work done once per weekly ingestion in the background, not
on any request path -- acceptable for this project's scale, called out
here as a known place to optimize (e.g. a sorted merge-join instead of
building two full in-memory dicts) if it ever needs to be cheaper.
"""

from __future__ import annotations

from pathlib import Path

from app.vcf_query import iter_all_variants

VariantKey = tuple[str, int, str, str]


def _read_classifications(vcf_path: Path) -> dict[VariantKey, str | None]:
    result: dict[VariantKey, str | None] = {}
    for chrom, pos, ref, alt, clnsig in iter_all_variants(vcf_path):
        result[(chrom, pos, ref, alt)] = clnsig
    return result


def redis_key(chrom: str, pos: int, ref: str, alt: str) -> str:
    """Same key format api's Redis cache-aside uses:
    ``variantAnnotation:{chrom}:{pos}:{ref}:{alt}``."""
    return f"variantAnnotation:{chrom}:{pos}:{ref}:{alt}"


def compute_changed_keys(old_vcf_path: Path | None, new_vcf_path: Path) -> list[str]:
    """Returns the sorted list of ``variantAnnotation:...`` keys whose
    clinical significance differs between ``old_vcf_path`` and
    ``new_vcf_path``. If ``old_vcf_path`` is ``None`` (first-ever
    ingestion, nothing to diff against), returns an empty list -- there is
    no previous cached answer anywhere for ``api`` to invalidate.
    """
    if old_vcf_path is None:
        return []

    old = _read_classifications(old_vcf_path)
    new = _read_classifications(new_vcf_path)

    changed: list[str] = []
    for key in old.keys() | new.keys():
        if old.get(key) != new.get(key):
            chrom, pos, ref, alt = key
            changed.append(redis_key(chrom, pos, ref, alt))

    changed.sort()
    return changed
