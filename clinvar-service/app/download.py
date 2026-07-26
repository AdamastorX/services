"""HTTP download + checksum helpers (ADR 0019, reimplemented from ADR 0018's
Java ``ClinVarDownloadClient``/``ClinVarTabixIndexer``).

Deliberate scope reduction from the Java prior art, stated explicitly
rather than silently dropped: the original ``ClinVarDownloadClient``
supported resuming a partially-downloaded file via HTTP ``Range``
requests. This port does a plain streaming download instead -- weekly,
infrequent, unauthenticated GETs of a single file, where a failed attempt
simply retries from scratch on the next scheduled run. Resume support can
be added back if partial-download retries ever become a real operational
problem; nothing in this module's interface would need to change for a
caller to add it later.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 1024 * 1024


class Downloader:
    """Thin, swappable seam over ``urllib`` -- tests substitute a fake that
    copies local fixture files instead of hitting the network (see
    tests/conftest.py's ``FakeDownloader``).
    """

    def download(self, url: str, destination: Path) -> str:
        """Downloads ``url`` to ``destination``, returning the SHA-256 hex digest
        of the downloaded content."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        request = Request(url, headers={"User-Agent": "clinvar-service/1.0"})
        with urlopen(request, timeout=600) as response, destination.open("wb") as out:
            while chunk := response.read(_CHUNK_SIZE):
                out.write(chunk)
                digest.update(chunk)
        return digest.hexdigest()

    def fetch_optional_text(self, url: str) -> str | None:
        """Best-effort fetch of a small text sidecar (NCBI's ``.md5`` checksum
        companion). Returns ``None`` on any failure -- a 404, a network
        error, anything -- the caller treats "no checksum available" as
        equivalent to "validation failed", the safe default (rebuild rather
        than trust an unverifiable index).
        """
        try:
            request = Request(url, headers={"User-Agent": "clinvar-service/1.0"})
            with urlopen(request, timeout=30) as response:
                if response.status != 200:
                    logger.info(
                        "Checksum sidecar %s returned HTTP %s -- treating as unavailable",
                        url,
                        response.status,
                    )
                    return None
                return response.read().decode("utf-8").strip()
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            logger.info("Checksum sidecar %s unreachable -- treating as unavailable (%s)", url, exc)
            return None


def md5_hex(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_hex(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def extract_hex(sidecar_content: str) -> str:
    """Sidecar files are typically ``"<hex>  <filename>"`` (coreutils
    md5sum format) or just the bare hex digest -- handle both."""
    return sidecar_content.strip().split()[0]


def validate_tbi(tbi_path: Path, published_checksum: str | None) -> bool:
    """Compares ``tbi_path``'s locally-computed MD5 against NCBI's published
    sidecar (a different check from ``clinvar_release.file_sha256``, which is
    this project's own SHA-256 of the *VCF*, not the index -- MD5 purely
    because that's the algorithm NCBI's sidecar uses; an integrity check
    against transfer corruption, not a security control).
    """
    if published_checksum is None:
        logger.warning("No published checksum available -- treating %s as unvalidated", tbi_path)
        return False
    expected = extract_hex(published_checksum)
    try:
        actual = md5_hex(tbi_path)
    except OSError:
        logger.warning("Could not read %s to compute its checksum -- treating as invalid", tbi_path)
        return False
    matches = expected.lower() == actual.lower()
    if not matches:
        logger.warning("Checksum mismatch for %s: expected %s but computed %s", tbi_path, expected, actual)
    return matches
