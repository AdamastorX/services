"""Response shape for GET /internal/clinvar/lookup (ADR 0019).

Field names/casing match the Java ``api``-side ``VariantAnnotation``
record this replaces *exactly* -- ``api`` is being rebuilt against this
same contract in parallel, so this shape is not up for reinterpretation.
Field names are deliberately camelCase Python attribute names (not
idiomatic snake_case) specifically so the model needs no alias
gymnastics to produce exactly this wire shape -- boring beats clever here.
"""

from __future__ import annotations

from pydantic import BaseModel


class VariantAnnotationResponse(BaseModel):
    chrom: str
    pos: int
    ref: str
    alt: str
    rsid: str | None
    clinicalSignificance: str | None
    clinicalReviewStatus: str | None
    # Always null for now -- gnomAD population allele-frequency
    # cross-referencing is explicitly deferred out of this component's
    # initial scope (see README.md's "Deferred scope" section). The field
    # is kept on the shape now, not added later, so a future change only
    # has to populate it, never alter the response contract.
    gnomadAlleleFrequency: float | None
    clinvarReleaseId: str
