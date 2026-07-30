"""Skewed rsID selection for `GET /variants/lookup` (backlog #45 AC: "a
*skewed* key distribution... a hot set of a few rsIDs plus a long tail, so
#29's hot-key panel plots something real").

HOT_RSIDS are not invented for this generator -- they're the same known-
real ClinVar GRCh38 pathogenic variants this project's own tests already
seed and assert against:
  - rs80357906 (BRCA1) -- services/api's VariantLookupIntegrationTest,
    services/clinvar-service's README.md/test_ingestion.py/
    test_lookup_endpoint.py/test_vcf_query.py.
  - rs80359550 (BRCA2) -- same VariantLookupIntegrationTest, same
    test_vcf_query.py docstring.
A real live cluster's clinvar-service has genuinely ingested these (weekly
ClinVar release ingestion, ADR 0019), so hot-set lookups aren't just
"repeated" -- they're real cache hits/DB hits against real data.

FAKE_RSID mirrors VariantLookupIntegrationTest's own
`unknownRsidReturnsNotFound` fixture value exactly, for the same reason
HOT_RSIDS mirrors the pathogenic ones: reuse a value this project has
already established as "known not to resolve", not a new one invented
here that might coincidentally exist in a future ClinVar release.
"""

from __future__ import annotations

import random

HOT_RSIDS = ["rs80357906", "rs80359550"]

FAKE_RSID = "rs00000000"


def pick_rsid(rng: random.Random, hot_key_weight: float) -> str:
    """With probability `hot_key_weight`, return one of the small hot set
    (uniformly -- the skew is hot-vs-tail, not further skewed within the
    hot set itself, which stays deliberately tiny). Otherwise return a
    random-looking long-tail rsID: most won't resolve (dbSNP has ~1
    billion+ ids; the odds of colliding with something clinvar-service
    actually ingested are low but non-zero), which is exactly the "long
    tail" shape a hot-key panel needs to contrast against the hot set --
    distinct from FAKE_RSID/error_fraction's *guaranteed* misses.
    """
    if rng.random() < hot_key_weight:
        return rng.choice(HOT_RSIDS)
    return f"rs{rng.randint(1_000_000, 999_999_999)}"
