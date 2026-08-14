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
on any request path.

**Real, live, confirmed contributor to backlog #131's OOM kills**: this
used to build two full in-memory dicts (old release, new release)
simultaneously -- ~4.4M entries each. Even after backlog #132's own
streaming fix to the variant-index-build step (that step's own peak
memory measured flat at ~88MiB across a real full scan, confirmed live),
a fourth real ingestion attempt still failed, its memory trace jumping
from ~88MiB to 580MiB+ in the single sample right after the scan
finished and this diff step began. ``compute_changed_keys`` now streams
``new_vcf_path`` against a single materialized ``old`` dict, popping
each key as it's matched so ``old`` shrinks as ``new`` is consumed
instead of a second full dict ever existing alongside it -- roughly
halves this step's peak (one ~4.4M-entry dict instead of two) using the
exact same O(n) dict-lookup algorithm, no new assumption about
old/new's VCF ordering (a full sorted merge-join would remove the
second dict entirely, but was assessed and deliberately not attempted
here -- see the docstring below for why).
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

    backlog #132: streams ``new_vcf_path`` against one materialized
    ``old`` dict rather than materializing both fully -- ``old.pop(key,
    None)`` doubles as both "look up old's value" and "remove it from
    consideration", so whatever remains in ``old`` once every ``new``
    record has been processed is, by construction, exactly the set of
    keys ``new`` never mentioned at all (the "deleted in this release"
    case). ``.pop(key, None)`` deliberately mirrors the original code's
    own ``dict.get(key)`` default of ``None`` for a missing key -- a key
    absent from ``old`` and a key present in ``old`` with a stored
    ``None`` classification are indistinguishable here, exactly as they
    always were (this function never claimed to tell those apart).

    **Accepted, narrow edge case, not silently glossed over**: a real
    (chrom, pos, ref, alt) key repeated more than once *within a single
    file* (the same exact variant listed twice in one VCF release --
    not the normal case for ClinVar's own one-entry-per-variant data
    model, and not observed in any real release this project has
    ingested) is fully collapsed for ``old`` (built as a complete dict
    first, same as before) but only compared against ``old`` on its
    *first* occurrence within ``new`` -- a second occurrence of the same
    key within ``new`` compares against the now-already-popped default
    of ``None`` rather than the true prior value, which can produce an
    extra (never a *missed*, except in the specific case where the
    second occurrence's own classification is ``None``) changed-key
    entry. A full sorted merge-join over both files would remove this
    edge case entirely (and the remaining ``old`` dict), but was
    assessed and deliberately not attempted in this same change: it
    requires assuming both VCFs share one consistent chromosome/contig
    sort order, which is very likely true for ClinVar's own stable
    release process but wasn't validated against real multi-chromosome
    data in this codebase's own fixtures, and a bug here would mean
    ``api`` silently keeps serving a stale cached classification
    indefinitely -- a worse failure than the real, live OOM this change
    already meaningfully reduces.
    """
    if old_vcf_path is None:
        return []

    old = _read_classifications(old_vcf_path)

    changed: list[str] = []
    for chrom, pos, ref, alt, clnsig in iter_all_variants(new_vcf_path):
        if old.pop((chrom, pos, ref, alt), None) != clnsig:
            changed.append(redis_key(chrom, pos, ref, alt))

    for (chrom, pos, ref, alt), old_clnsig in old.items():
        if old_clnsig is not None:
            changed.append(redis_key(chrom, pos, ref, alt))

    changed.sort()
    return changed
