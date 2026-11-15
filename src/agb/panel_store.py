"""The panel store: content-addressed, zstd-compressed bodies on the data disk.

The panel's full prompt and response bodies and the filing texts it read are too large and too
rarely read to belong in Postgres (docs/architecture/08, "Panel logs"). Each body is stored
once, under the SHA-256 of its uncompressed bytes, and the database refers to it by that hash:

    <root>/<sha[:2]>/<sha>.zst

A body is written to a temporary file in its directory and renamed into place, and the
directory is synced after the rename, so a reader never sees a partial file and a crash cannot
lose a body the database already names. Storing bytes that are already present is a no-op when
the file on disk still decompresses to them; a body that has gone bad (truncated, bit-rotted,
overwritten) is rewritten from the bytes in hand, so the next run repairs it.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path

import zstandard

DEFAULT_DIR = Path("panel")
# Where the store lives, which the pipeline service reads.
DIR_ENV = "AGB_PANEL_DIR"
LEVEL = 10


class StoreError(Exception):
    """A body that is missing, or whose content no longer matches its hash."""


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sync_dir(directory: Path) -> None:
    """Make a rename in `directory` durable: the file's own fsync does not cover its entry."""
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class PanelStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def path(self, digest: str) -> Path:
        return self.root / digest[:2] / f"{digest}.zst"

    def put(self, data: bytes) -> str:
        """Store `data` and return its SHA-256. Storing identical content again writes nothing,
        unless the body on disk no longer matches its hash, in which case it is rewritten."""
        digest = sha256(data)
        target = self.path(digest)
        if target.exists():
            try:
                self.get(digest)
            except StoreError:
                pass
            else:
                return digest
        target.parent.mkdir(parents=True, exist_ok=True)
        compressed = zstandard.ZstdCompressor(level=LEVEL).compress(data)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{digest}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(compressed)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, target)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
        _sync_dir(target.parent)
        return digest

    def put_text(self, text: str) -> str:
        return self.put(text.encode())

    def get(self, digest: str) -> bytes:
        """The body stored under `digest`, checked against it."""
        try:
            compressed = self.path(digest).read_bytes()
        except FileNotFoundError:
            raise StoreError(f"no body {digest} in {self.root}") from None
        try:
            data = zstandard.ZstdDecompressor().decompress(compressed)
        except zstandard.ZstdError as exc:
            raise StoreError(f"body {digest} in {self.root} does not decompress: {exc}") from exc
        if sha256(data) != digest:
            raise StoreError(f"body {digest} in {self.root} does not match its hash")
        return data

    def get_text(self, digest: str) -> str:
        return self.get(digest).decode()

    def has(self, digest: str) -> bool:
        return self.path(digest).exists()
