"""GET /internal/clinvar/lookup (ADR 0019).

Contract, restated so it's checkable against this file directly: accepts
either ``(chrom, pos, ref, alt)`` or ``rsid`` -- exactly one of the two key
styles. Both or neither is a ``400`` with a clear message. A match returns
``200`` with the annotation body; no match is a ``404``. rsID lookups
resolve through ``clinvar_variant_index`` first (Postgres), then do the
actual point query against the *current* release's tabix-indexed VCF --
tabix indexes are position-based, so this is the only feasible path for an
rsID lookup.

**Coordinate-form queries are normalized (trim-only, backlog #39)** before
matching against the VCF -- see ``app/normalize.py`` for exactly what
level of normalization that covers.

**rsID multi-match (backlog #38)**: an rsID can legitimately map to more
than one coordinate row in the index. Every match is checked against the
current release's VCF (not just the first row). Exactly one real hit
resolves normally. Zero real hits is a plain ``404``. More than one real
hit is a genuinely ambiguous rsID -- this is reported as a loud, explicit
``409 Conflict`` naming every candidate, never silently picked, since the
response body's shape is a single fixed-contract object (matches the Java
``api``-side record this replaces) rather than a list.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

from app import repository
from app.metrics import LOOKUP_DURATION_SECONDS
from app.normalize import normalize_variant
from app.schemas import VariantAnnotationResponse
from app.vcf_query import VcfHit
from app.vcf_query import query as vcf_point_query

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/internal/clinvar/lookup", response_model=VariantAnnotationResponse)
def lookup(
    request: Request,
    chrom: str | None = Query(default=None),
    pos: int | None = Query(default=None),
    ref: str | None = Query(default=None),
    alt: str | None = Query(default=None),
    rsid: str | None = Query(default=None),
) -> VariantAnnotationResponse:
    # The raw HTTP-call latency/error rate from api's perspective (ADR
    # 0020) -- a cache-outcome split is api's own job (ADR 0016's Redis
    # layer), not this service's. Wraps the whole handler, including the
    # 400/404 HTTPException paths -- a Histogram context manager records
    # elapsed time in __exit__ regardless of the exception that triggers
    # it, so a request's outcome doesn't skip it out of the latency data.
    with LOOKUP_DURATION_SECONDS.time():
        return _lookup(request, chrom, pos, ref, alt, rsid)


def _lookup(
    request: Request,
    chrom: str | None,
    pos: int | None,
    ref: str | None,
    alt: str | None,
    rsid: str | None,
) -> VariantAnnotationResponse:
    by_coordinates = chrom is not None or pos is not None or ref is not None or alt is not None
    by_rsid = rsid is not None

    if by_coordinates and by_rsid:
        raise HTTPException(status_code=400, detail="Provide either (chrom, pos, ref, alt) or rsid, not both")
    if not by_coordinates and not by_rsid:
        raise HTTPException(status_code=400, detail="Provide either (chrom, pos, ref, alt) or rsid")

    pool = request.app.state.db_pool
    paths = request.app.state.refdata_paths

    with pool.connection() as conn:
        active_release = repository.current_active_release(conn)
        if active_release is None:
            raise HTTPException(status_code=404, detail="No ClinVar release has been ingested yet")

        if by_rsid:
            candidates = repository.find_coordinates_by_rsid(conn, rsid)
            if not candidates:
                raise HTTPException(status_code=404, detail=f"No indexed variant for rsid={rsid}")
        else:
            if chrom is None or pos is None or ref is None or alt is None:
                raise HTTPException(
                    status_code=400, detail="Coordinate lookup requires all of chrom, pos, ref, and alt"
                )
            norm_pos, norm_ref, norm_alt = normalize_variant(pos, ref, alt)
            candidates = [(chrom, norm_pos, norm_ref, norm_alt, active_release.release_id)]

    vcf_path = paths.current_vcf_path()
    real_hits: list[tuple[tuple[str, int, str, str, object], VcfHit]] = []
    for candidate in candidates:
        cand_chrom, cand_pos, cand_ref, cand_alt, cand_release_id = candidate
        hit = vcf_point_query(vcf_path, cand_chrom, cand_pos, cand_ref, cand_alt)
        if hit is not None:
            real_hits.append((candidate, hit))

    if not real_hits:
        if by_rsid:
            raise HTTPException(status_code=404, detail=f"No ClinVar record for rsid={rsid}")
        raise HTTPException(
            status_code=404,
            detail=f"No ClinVar record for {chrom}:{pos}:{ref}:{alt}",
        )

    if len(real_hits) > 1:
        # Genuinely ambiguous rsID -- more than one real ClinVar record
        # shares it (backlog #38). Loud and explicit rather than a
        # silent pick: this response shape is a single fixed-contract
        # object, so a list can't be returned here without breaking the
        # `api`-side contract; a 409 naming every candidate is the
        # documented disambiguation rule instead.
        candidate_strs = [f"{c[0]}:{c[1]}:{c[2]}:{c[3]}" for c, _ in real_hits]
        logger.warning("Ambiguous rsid=%s resolved to %d real ClinVar records: %s", rsid, len(real_hits), candidate_strs)
        raise HTTPException(
            status_code=409,
            detail=f"rsid={rsid} is ambiguous -- matches multiple ClinVar records: {', '.join(candidate_strs)}",
        )

    (resolved_chrom, resolved_pos, resolved_ref, resolved_alt, release_id), hit = real_hits[0]

    return VariantAnnotationResponse(
        chrom=resolved_chrom,
        pos=resolved_pos,
        ref=resolved_ref,
        alt=resolved_alt,
        rsid=hit.rsid,
        clinicalSignificance=hit.clinical_significance,
        clinicalReviewStatus=hit.review_status,
        # gnomAD cross-referencing is deferred (see app/schemas.py) --
        # always null until a future change wires up a real gnomAD slice.
        gnomadAlleleFrequency=None,
        clinvarReleaseId=str(release_id),
    )
