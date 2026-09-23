import hashlib

import pytest

from agb.panel_store import PanelStore, StoreError


def files(root):
    return sorted(p for p in root.rglob("*") if p.is_file())


def test_bodies_are_stored_by_the_sha256_of_their_content(tmp_path):
    store = PanelStore(tmp_path)
    body = b"Item 1A. Risk Factors\n" * 1000

    digest = store.put(body)

    assert digest == hashlib.sha256(body).hexdigest()
    [path] = files(tmp_path)
    assert path == tmp_path / digest[:2] / f"{digest}.zst"
    assert path.stat().st_size < len(body) / 10
    assert store.get(digest) == body
    assert store.has(digest)
    assert store.get_text(store.put_text("naïve – text")) == "naïve – text"


def test_restoring_identical_content_is_a_no_op(tmp_path):
    store = PanelStore(tmp_path)
    digest = store.put(b"the same body")
    [path] = files(tmp_path)
    before = path.stat()

    assert store.put(b"the same body") == digest
    assert PanelStore(tmp_path).put(b"the same body") == digest

    assert files(tmp_path) == [path]
    after = path.stat()
    assert (after.st_mtime_ns, after.st_ino) == (before.st_mtime_ns, before.st_ino)


def test_a_missing_or_corrupt_body_is_an_error(tmp_path):
    store = PanelStore(tmp_path)
    with pytest.raises(StoreError, match="no body"):
        store.get("0" * 64)

    digest = store.put(b"original")
    other = store.put(b"tampered")
    (tmp_path / digest[:2] / f"{digest}.zst").write_bytes(
        (tmp_path / other[:2] / f"{other}.zst").read_bytes()
    )
    with pytest.raises(StoreError):
        store.get(digest)
