import hashlib
import os

import pytest
import zstandard

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


def corrupt(store, digest, damage):
    path = store.path(digest)
    path.write_bytes(damage(path.read_bytes()))


@pytest.mark.parametrize(
    "damage",
    [
        pytest.param(lambda data: data[: len(data) // 2], id="truncated"),
        pytest.param(lambda data: b"\0" * len(data), id="zeroed"),
        pytest.param(lambda data: zstandard.compress(b"some other body"), id="overwritten"),
    ],
)
def test_storing_a_body_again_repairs_one_that_has_gone_bad(tmp_path, damage):
    store = PanelStore(tmp_path)
    digest = store.put(b"the original body")
    corrupt(store, digest, damage)
    with pytest.raises(StoreError):
        store.get(digest)

    assert store.put(b"the original body") == digest

    assert store.get(digest) == b"the original body"
    assert files(tmp_path) == [store.path(digest)]


def test_a_new_body_is_synced_with_its_directory(tmp_path, monkeypatch):
    store = PanelStore(tmp_path)
    synced = []
    fsync = os.fsync

    def recording(fd):
        synced.append(os.fstat(fd).st_ino)
        fsync(fd)

    monkeypatch.setattr(os, "fsync", recording)

    digest = store.put(b"a body the database will name")

    path = store.path(digest)
    assert synced == [path.stat().st_ino, path.parent.stat().st_ino]
