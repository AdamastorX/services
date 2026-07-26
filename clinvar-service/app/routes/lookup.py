"""GET /internal/clinvar/lookup (ADR 0019).

Contract, restated so it's checkable against this file directly: accepts
either ``(chrom, pos, ref, alt)`` or ``rsid`` -- exactly one of the two key
styles. Both or neither is a ``400`` with a clear message. A match returns
``200`` with the annotation body; no match is a ``404``. rsID lookups
resolve through ``clinvar_variant_index`` first (Postgres), then do the
actual point query against the *current* release's tabix-indexed VCF --
tabix indexes are position-based, so this is the only feasible path for an
rsID lookup.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from app import repository
from app.metrics import LOOKUP_DURATION_SECONDS
from app.schemas import VariantAnnotationResponse
from app.vcf_query import query as vcf_point_query

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
            resolved = repository.find_coordinates_by_rsid(conn, rsid)
            if resolved is None:
                raise HTTPException(status_code=404, detail=f"No indexed variant for rsid={rsid}")
            resolved_chrom, resolved_pos, resolved_ref, resolved_alt, release_id = resolved
        else:
            if chrom is None or pos is None or ref is None or alt is None:
                raise HTTPException(
                    status_code=400, detail="Coordinate lookup requires all of chrom, pos, ref, and alt"
                )
            resolved_chrom, resolved_pos, resolved_ref, resolved_alt = chrom, pos, ref, alt
            release_id = active_release.release_id

    hit = vcf_point_query(paths.current_vcf_path(), resolved_chrom, resolved_pos, resolved_ref, resolved_alt)
    if hit is None:
        raise HTTPException(
            status_code=404,
            detail=f"No ClinVar record for {resolved_chrom}:{resolved_pos}:{resolved_ref}:{resolved_alt}",
        )

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
