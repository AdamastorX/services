"""Real-Postgres regression tests for app/repository.py (backlog #38):
find_coordinates_by_rsid must return every matching row for an rsID, not
silently drop alternate matches behind a LIMIT 1."""

from __future__ import annotations

import datetime
import uuid

from app import repository


def _seed_release(conn) -> uuid.UUID:
    release_id = uuid.uuid4()
    repository.insert_pending_release(conn, release_id, "u://vcf", "deadbeef", datetime.date(2026, 7, 20))
    repository.activate_release(conn, release_id, variant_count=2)
    return release_id


def test_find_coordinates_by_rsid_returns_all_matches_for_multi_mapped_rsid(db_conn):
    release_id = _seed_release(db_conn)

    # A single rsID legitimately mapping to two distinct alleles/positions
    # -- a real, non-edge-case occurrence in ClinVar's own data.
    repository.insert_variant_index_rows(
        db_conn,
        [
            ("rs123", "1", 1000, "A", "T", release_id),
            ("rs123", "2", 2000, "G", "C", release_id),
        ],
    )

    rows = repository.find_coordinates_by_rsid(db_conn, "rs123")

    assert len(rows) == 2
    coordinates = {(chrom, pos, ref, alt) for chrom, pos, ref, alt, _ in rows}
    assert coordinates == {("1", 1000, "A", "T"), ("2", 2000, "G", "C")}


def test_find_coordinates_by_rsid_returns_single_row_list_for_unambiguous_rsid(db_conn):
    release_id = _seed_release(db_conn)
    repository.insert_variant_index_rows(db_conn, [("rs456", "17", 43057062, "T", "TG", release_id)])

    rows = repository.find_coordinates_by_rsid(db_conn, "rs456")

    assert len(rows) == 1
    assert rows[0][:4] == ("17", 43057062, "T", "TG")


def test_find_coordinates_by_rsid_returns_empty_list_for_unknown_rsid(db_conn):
    _seed_release(db_conn)

    rows = repository.find_coordinates_by_rsid(db_conn, "rs_does_not_exist")

    assert rows == []


def test_find_coordinates_by_rsid_ignores_a_not_yet_active_release(db_conn):
    """backlog #132: streaming variant-index inserts (app/ingestion.py)
    means real index rows for a not-yet-active release can now sit in
    clinvar_variant_index for the full scan duration, alongside the
    still-active previous release's own rows -- widened from the old
    single-bulk-insert shape's low-single-digit-second window. An rsID
    whose coordinates are unchanged between releases (the common case)
    would otherwise look ambiguous (two rows, same real coordinate, two
    different clinvar_release_ids) purely because of that overlap, not
    because it's genuinely ambiguous. find_coordinates_by_rsid must only
    ever see the one real active release, regardless of what else
    happens to be sitting in the table -- proven here by seeding a
    pending (is_active=false) release's row for the same rsid alongside
    the active one, not just asserting the query text looks right.
    """
    active_release_id = _seed_release(db_conn)
    repository.insert_variant_index_rows(db_conn, [("rs789", "5", 5000, "A", "G", active_release_id)])

    pending_release_id = uuid.uuid4()
    repository.insert_pending_release(db_conn, pending_release_id, "u://vcf2", "cafebabe", datetime.date(2026, 8, 1))
    repository.insert_variant_index_rows(db_conn, [("rs789", "5", 5000, "A", "G", pending_release_id)])

    rows = repository.find_coordinates_by_rsid(db_conn, "rs789")

    assert len(rows) == 1
    assert rows[0] == ("5", 5000, "A", "G", active_release_id)
