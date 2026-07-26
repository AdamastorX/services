"""Test-only doubles (not shipped in app/)."""

from __future__ import annotations

import shutil
from pathlib import Path

from app.download import sha256_hex


class FakeDownloader:
    """Swaps in for app.download.Downloader in tests: 'downloads' by
    copying a preconfigured local file instead of making a real HTTP
    call, so ingestion tests are hermetic and don't depend on NCBI's FTP
    server being reachable. ``source_map`` maps the URL string ingestion
    code will ask for to a local Path to copy from; ``checksums`` maps a
    checksum-sidecar URL to the text it should "publish" (or omit a key
    to simulate an unavailable sidecar, triggering the rebuild path).
    """

    def __init__(self, source_map: dict[str, Path], checksums: dict[str, str] | None = None) -> None:
        self._source_map = source_map
        self._checksums = checksums or {}

    def download(self, url: str, destination: Path) -> str:
        source = self._source_map[url]
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        return sha256_hex(destination)

    def fetch_optional_text(self, url: str) -> str | None:
        return self._checksums.get(url)


class FakeEventProducer:
    """Captures published events instead of talking to a real Kafka broker."""

    def __init__(self) -> None:
        self.published: list = []

    def publish(self, event) -> None:
        self.published.append(event)

    def close(self) -> None:
        pass
