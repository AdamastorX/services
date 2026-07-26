"""pysam-based VCF/tabix access (ADR 0019 -- replaces ADR 0018's htsjdk
usage with the tool practitioners in this domain actually reach for; same
underlying htslib C library either way).

**Load-bearing detail, verified against a real fixture, not assumed**:
ClinVar's ``CLNSIG``/``CLNREVSTAT``/``RS`` INFO fields are declared
``Number=.`` in the VCF header, so pysam (like htsjdk) parses their raw
text as a *comma-separated list* and hands back a tuple of tokens. Some
real ClinVar review-status values themselves legitimately contain a
literal comma (e.g. ``criteria_provided,_single_submitter``) -- for such a
value, pysam returns ``("criteria_provided", "_single_submitter")``, two
tokens, not one. The original string is only recoverable by re-joining
the tuple with ``","``, exactly what ADR 0018's Java
``ClinVarVcfQueryService.joinedAttribute`` did and this module's
``_joined_info`` does too. Skipping this step silently truncates or
mangles review-status values for a large share of real ClinVar records.
"""

from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass
from pathlib import Path

import pysam

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VcfHit:
    clinical_significance: str | None
    review_status: str | None
    rsid: str | None


def _joined_info(record, key: str) -> str | None:
    values = record.info.get(key)
    if values is None:
        return None
    if isinstance(values, tuple):
        if not values:
            return None
        return ",".join(str(v) for v in values)
    return str(values)


def _normalize_rsid(raw: str) -> str:
    """ClinVar's own ``RS`` INFO values are bare numbers (e.g. ``80357906``),
    not ``rs``-prefixed."""
    text = raw
    if text.lower().startswith("rs"):
        text = text[2:]
    return f"rs{text}"


def _normalize_chrom(chrom: str) -> str:
    return chrom[3:] if chrom.startswith("chr") else chrom


def query(vcf_path: Path, chrom: str, pos: int, ref: str, alt: str) -> VcfHit | None:
    """Queries position ``(chrom, pos)`` (1-based, VCF convention) against
    ``vcf_path`` and returns the record whose REF/ALT exactly match, if any.
    ClinVar normalizes one VCF record per allele, so more than one record
    can share a position -- only an exact allele match is the variant the
    caller actually asked about.
    """
    normalized_chrom = _normalize_chrom(chrom)
    with pysam.VariantFile(str(vcf_path)) as vf:
        try:
            # pysam.fetch is 0-based half-open; a 1-based VCF position
            # `pos` is [pos-1, pos) in that coordinate system.
            records = vf.fetch(normalized_chrom, pos - 1, pos)
        except ValueError:
            # Unknown contig for this VCF (e.g. a chrom with literally no
            # records at all) -- pysam raises rather than yielding empty.
            return None
        for record in records:
            if record.ref != ref:
                continue
            if record.alts and alt in record.alts:
                return VcfHit(
                    clinical_significance=_joined_info(record, "CLNSIG"),
                    review_status=_joined_info(record, "CLNREVSTAT"),
                    rsid=_rsid_from_record(record),
                )
    return None


def _rsid_from_record(record) -> str | None:
    joined = _joined_info(record, "RS")
    if joined is None:
        return None
    # RS can carry multiple comma-joined ids; the first is what this
    # project's rsID index/lookup convention uses (matches ADR 0018's
    # normalizeRsId, applied per-id there but callers here only ever deal
    # with the first id for a given allele).
    first = joined.split(",")[0]
    return _normalize_rsid(first)


def read_published_date(vcf_path: Path) -> datetime.date:
    """Parses the VCF's own ``##fileDate`` header line -- not file mtime,
    which reflects when *this app* downloaded the file, not when NCBI cut
    the release (ADR 0018's explicit AC, carried over unchanged)."""
    with pysam.VariantFile(str(vcf_path)) as vf:
        for record in vf.header.records:
            if record.key == "fileDate":
                value = record.value
                return datetime.date.fromisoformat(value)
    raise ValueError(f"No ##fileDate header found in {vcf_path}")


def iter_records(vcf_path: Path):
    """Streams raw pysam records from ``vcf_path`` -- the shared low-level
    seam both ``iter_all_variants`` (diffing) and
    ``app.ingestion._build_variant_index_rows`` (rsID indexing) build on,
    so there's exactly one place that opens/iterates a VCF file wholesale.
    """
    with pysam.VariantFile(str(vcf_path)) as vf:
        yield from vf


def iter_all_variants(vcf_path: Path):
    """Streams every record in ``vcf_path``, yielding ``(chrom, pos, ref, alt,
    clinical_significance)`` once per ALT allele (ClinVar's own
    one-record-per-allele convention still holds per multi-ALT record in
    principle, so this expands defensively rather than assuming exactly one
    ALT per record).
    """
    for record in iter_records(vcf_path):
        clnsig = _joined_info(record, "CLNSIG")
        if not record.alts:
            continue
        for alt in record.alts:
            yield record.chrom, record.pos, record.ref, alt, clnsig


def rebuild_tabix_index(vcf_gz_path: Path) -> Path:
    """Rebuilds the ``.tbi`` for an already block-gzipped VCF via
    ``pysam.tabix_index`` -- used only when NCBI's published index fails
    checksum validation (ADR 0018's explicit AC, avoids re-indexing a
    ~250MB file on every ingestion when the published index is almost
    always fine)."""
    pysam.tabix_index(str(vcf_gz_path), preset="vcf", force=True)
    tbi_path = vcf_gz_path.with_suffix(vcf_gz_path.suffix + ".tbi")
    logger.info("Rebuilt tabix index %s for %s", tbi_path, vcf_gz_path)
    return tbi_path
