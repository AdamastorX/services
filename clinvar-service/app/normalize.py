"""Query-side variant normalization (backlog #39).

ClinVar's own VCF stores one canonical, already-normalized representation
per allele. A caller's ``(chrom, pos, ref, alt)`` query can describe the
exact same clinically-equivalent event with extra, redundant flanking
context that ClinVar's own normalization already stripped -- an exact
string match against the tabix-indexed VCF then produces a false-negative
404 even though the underlying variant is present.

**Scope actually implemented here, stated explicitly rather than
overclaimed**: this module implements *trimming* (the parsimony half of
full VCF normalization) -- removing a shared suffix and then a shared
prefix between REF and ALT, adjusting POS as bases are removed from the
left, down to the minimal VCF-legal representation (an indel's REF/ALT
must keep at least one shared anchor base; VCF alleles are never empty).
This needs only the REF/ALT strings the caller already submitted -- no
reference genome access at all -- and resolves any query that carries
extra redundant context around an indel.

**Explicitly deferred, not implemented**: true left-alignment -- sliding
an already-minimal indel to the leftmost equivalent position inside a
homopolymer or tandem-repeat run (e.g. representing an insertion of a `G`
one position to the right of where ClinVar itself anchored it, inside a
run of `G`s). That requires walking real reference-genome sequence
flanking the variant, which this service has no access to today: the
tabix-indexed VCF only carries REF bases at variant positions, not
arbitrary flanking sequence, and this project has no reference FASTA
provisioned (a real GRCh38 FASTA is ~3GB and would need its own
download/storage story -- new infrastructure out of scope for this
backlog item, not a plain code fix). If that gap needs closing, it's a
follow-up item that adds a reference-FASTA slice (e.g. via
``pysam.FastaFile``), not a change to this module's algorithm.
"""

from __future__ import annotations


def normalize_variant(pos: int, ref: str, alt: str) -> tuple[int, str, str]:
    """Trims a shared suffix then a shared prefix between ``ref`` and
    ``alt``, adjusting ``pos`` for any bases removed from the left.
    ``pos`` is 1-based (VCF convention), matching the rest of this
    service (see ``app/vcf_query.py``).

    Stops trimming a side once either allele would be emptied -- VCF
    alleles always keep at least one anchor base, exactly the
    representation ClinVar's own VCF uses (e.g. a plain deletion is
    encoded as ``REF=GT ALT=G``, never ``REF=T ALT=``).

    No-ops (returns the input unchanged) for SNVs and already-minimal
    indels, so it's always safe to call unconditionally on an incoming
    coordinate-form query.
    """
    ref, alt = ref.upper(), alt.upper()

    # Right-trim: drop a shared trailing base while both alleles have
    # more than one base left.
    while len(ref) > 1 and len(alt) > 1 and ref[-1] == alt[-1]:
        ref, alt = ref[:-1], alt[:-1]

    # Left-trim: drop a shared leading base while both alleles have more
    # than one base left, advancing pos by one per base removed.
    while len(ref) > 1 and len(alt) > 1 and ref[0] == alt[0]:
        ref, alt = ref[1:], alt[1:]
        pos += 1

    return pos, ref, alt
