"""Filesystem layout for clinvar-service's own PVC (ADR 0019).

    {refdata_path}/
      releases/
        {release_id}/
          clinvar.vcf.gz
          clinvar.vcf.gz.tbi
      current -> releases/{release_id}   (symlink, relative target)

Same download-then-swap safety property ADR 0018 established, reimplemented
in Python: a new release is written entirely into its own private
directory, and ``flip_current`` only ever repoints the ``current`` symlink
-- callers are responsible for only calling it after the corresponding
Postgres row has committed (app/ingestion.py enforces the ordering; this
module doesn't know about Postgres at all).

Retention: only the current release and the immediately-previous one are
kept on disk (``prune_other_than``) -- the previous release's tabix file
has to stay around across an ingestion specifically because
app/diff.py needs both the old and new release's files at once to compute
which cached keys changed.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

VCF_FILENAME = "clinvar.vcf.gz"
TBI_FILENAME = "clinvar.vcf.gz.tbi"
_CURRENT_LINK_NAME = "current"
_RELEASES_DIR_NAME = "releases"


class ClinVarRefdataPaths:
    def __init__(self, refdata_path: str | Path) -> None:
        self.root = Path(refdata_path)

    def release_dir(self, release_id: uuid.UUID) -> Path:
        return self.root / _RELEASES_DIR_NAME / str(release_id)

    def vcf_path(self, release_id: uuid.UUID) -> Path:
        return self.release_dir(release_id) / VCF_FILENAME

    def tbi_path(self, release_id: uuid.UUID) -> Path:
        return self.release_dir(release_id) / TBI_FILENAME

    def current_link(self) -> Path:
        return self.root / _CURRENT_LINK_NAME

    def current_vcf_path(self) -> Path:
        """Resolves the VCF path a reader should query right now, following ``current``."""
        return self.current_link() / VCF_FILENAME

    def flip_current(self, release_id: uuid.UUID) -> None:
        """Atomically repoints ``current`` at ``release_dir(release_id)``.

        A symlink can't be replaced atomically in place, so this creates a
        new symlink under a temporary name and renames it onto ``current``
        -- POSIX guarantees a rename is a single atomic filesystem
        operation, so a reader either sees the fully-written old release
        or the fully-written new one, never a torn state.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        relative_target = Path(_RELEASES_DIR_NAME) / str(release_id)
        temp_link = self.root / f"{_CURRENT_LINK_NAME}.tmp-{uuid.uuid4()}"
        temp_link.symlink_to(relative_target)
        temp_link.replace(self.current_link())

    def current_release_id_or_none(self) -> uuid.UUID | None:
        link = self.current_link()
        if not link.is_symlink():
            return None
        try:
            target = link.readlink()
            return uuid.UUID(target.name)
        except (OSError, ValueError):
            return None

    def prune_other_than(self, keep: set[uuid.UUID]) -> None:
        releases_dir = self.root / _RELEASES_DIR_NAME
        if not releases_dir.is_dir():
            return
        keep_names = {str(release_id) for release_id in keep}
        for child in releases_dir.iterdir():
            if child.is_dir() and child.name not in keep_names:
                shutil.rmtree(child, ignore_errors=True)
