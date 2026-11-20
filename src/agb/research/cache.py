"""Checkpoints for a certification's expensive intermediates, under the research directory.

A certification is one long run. The cross-validation refits the model once per split, and the
validation backtest replays every path through the engine session by session; together they
take a day and a half, almost all of it in the backtest. An attempt that fails a gate, is
interrupted, or is started again after a change that cannot have moved the numbers pays for
all of it a second time. This module keeps those intermediates, keyed by everything that
determines them, so the next attempt reuses what it can.

Three kinds of entry are kept:

    cpcv        one per cross-validation: every path's out-of-sample scores and the run's
                results, keyed by the panel, the model configuration and the options
    backtest    one per multi-path backtest or replay: the whole run's results, keyed by the
                panel, the backtest options and every path's scores
    path        one per path inside such a run: that path's report, keyed by the same things

The path entries are what makes an interrupted backtest resumable: each is written as its path
finishes, so a run started again picks up at the first path it has no entry for. The run-level
entry is what makes a complete rerun quick rather than merely quicker, because it is the only
one that lets a run skip opening the lake and loading the bars at all.

A hit has to be the same computation, not probably the same, because a certification is an
audit record. Every key is a digest of a record of what it keys, that record is stored in the
entry, and it is compared with the asking run's before the payload is used, so a digest
collision misses rather than lies. `trials.code_version` is part of every record, so an entry
written by another build of the code is never reused, and `FORMAT` is too, so a change to what
an entry holds retires every entry written before it. The panel is keyed by its values as well
as its rows, features and labels included, because the panel is built by its own command and
may have been built by another build than the one reading it: a panel rebuilt after a feature
formula changed misses even where every other part of the record agrees.

A path is keyed by the scores it trades rather than by the model that produced them, which is
why changing the model configuration misses every path without a path key having to know what
a configuration is. It is keyed by *every* path's scores, not only its own, because the engine
feed is loaded for the union of the names any path ranks, and a name's bars therefore begin at
the first month any path holds it: a path's report depends on the set it was run with.

A `cpcv` or `backtest` entry also names the ledger row its run wrote, and reusing the entry
means reusing that trial id rather than writing a new row. Nothing trained, so there is
nothing to record; and recording it anyway would leave the first attempt's backtest standing
as one of the candidates the overfitting gate reads, which would move the verdict on a rerun.
An entry whose trial id is not in the ledger the asking run writes to, or is not `succeeded`
there, is not reused at all, because a manifest has to name trials its own shipped ledger
holds.

Nothing here is consulted unless a cache is handed in. `certify` hands one in, and so does the
`cpcv` command when the build has a version: a cross-validation the operator runs by hand
writes the same `cpcv` entry, scores included, that the certification of that configuration
over that panel then reads instead of refitting. `reproduce` hands in none, so a reproduction
still recomputes the run it is checking, which is the whole point of it."""

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from agb.research.panel import Panel
from agb.research.trials import Ledger, LedgerError, code_version, encode

DIRECTORY = "cache"  # under the research directory
ENTRY = "entry.json"  # written last: its presence is what marks an entry complete
SCORES = "scores.parquet"
# What an entry holds and how its key is built. Bump it for any change to either, and every
# entry written before the change misses rather than being read under the new rules.
FORMAT = 3  # 3: the first month carries the first basket's fees (issue #168)
KINDS = ("cpcv", "backtest", "path")
_DIGITS = 32  # of the key's digest, which names the entry's directory
_SCORES_SCHEMA = pa.schema([
    ("path", pa.string()),
    ("as_of", pa.string()),
    ("ticker", pa.string()),
    ("score", pa.float64()),
])  # fmt: skip


class CacheError(Exception):
    """A cache that cannot be used as asked."""


@dataclass(frozen=True)
class Key:
    """One entry's place in the cache, and the record that entitles a run to read it."""

    root: Path
    kind: str
    digest: str
    keyed_by: dict[str, Any]

    @property
    def directory(self) -> Path:
        return self.root / self.kind / self.digest

    def read(self) -> dict[str, Any] | None:
        """The entry's payload, or None if there is no entry here this run may use.

        A half-written, unreadable or stale entry is a miss, never an error: the cache is only
        ever an optimisation, and a run that cannot read it computes instead."""
        try:
            entry = json.loads((self.directory / ENTRY).read_text())
        except (OSError, ValueError):
            return None
        if not isinstance(entry, dict) or entry.get("format") != FORMAT:
            return None
        if encode(entry.get("keyed_by")) != encode(self.keyed_by):
            return None
        payload = entry.get("payload")
        return payload if isinstance(payload, dict) else None

    def write(self, payload: dict[str, Any], *, table: pa.Table | None = None) -> None:
        """Record `payload` under this key, with `table` beside it if there is one.

        The payload is written to a temporary name and moved into place, as the ledger writes
        itself, so a run killed mid-write leaves either the whole entry or none of it. The
        table goes first: `entry.json` is what a reader looks for, so it is what appears last.
        """
        self.directory.mkdir(parents=True, exist_ok=True)
        if table is not None:
            _atomic(self.directory / SCORES, lambda path: pq.write_table(table, path))
        entry = {"format": FORMAT, "kind": self.kind, "keyed_by": self.keyed_by}
        entry["payload"] = payload
        _atomic(
            self.directory / ENTRY,
            lambda path: path.write_text(json.dumps(entry, indent=2, sort_keys=True)),
        )

    def table(self) -> pa.Table | None:
        path = self.directory / SCORES
        try:
            return pq.read_table(path, schema=_SCORES_SCHEMA)
        except (OSError, pa.ArrowInvalid):
            return None


@dataclass(frozen=True)
class Cache:
    """The cache rooted at `root`. A directory that does not exist is an empty cache."""

    root: Path

    @classmethod
    def under(cls, research_dir: Path) -> "Cache":
        return cls(research_dir / DIRECTORY)

    def key(self, kind: str, keyed_by: Mapping[str, Any]) -> Key:
        if kind not in KINDS:
            raise CacheError(f"unknown cache kind {kind!r}; expected one of {KINDS}")
        record = {
            **keyed_by,
            "kind": kind,
            "format": FORMAT,
            "code_version": code_version(),
        }
        digest = hashlib.sha256(encode(record).encode()).hexdigest()[:_DIGITS]
        return Key(root=self.root, kind=kind, digest=digest, keyed_by=record)


def cpcv_key(
    cache: Cache, panel: Panel, *, model: Mapping[str, Any], options: Mapping[str, Any]
) -> Key:
    """The key of the cross-validation of `model` over `panel` under `options`."""
    return cache.key("cpcv", {"panel": fingerprint(panel), "model": model, "options": options})


def backtest_key(
    cache: Cache,
    panel: Panel,
    *,
    stage: str,
    options: Mapping[str, Any],
    scores: Mapping[str, pd.Series],
    forced: Sequence[Any] = (),
) -> Key:
    """The key of a whole multi-path backtest or replay of `scores` over `panel`.

    `stage` is `run` or `replay`. They do the same work over the paths, which is why they
    share the path entries, but what a run-level entry holds differs: a run's names the ledger
    row it wrote and a replay writes no row, so the two do not share one."""
    if stage not in ("run", "replay"):
        raise CacheError(f"unknown backtest stage {stage!r}")
    return cache.key("backtest", {**_traded(panel, options, scores, forced), "stage": stage})


def path_key(
    cache: Cache,
    panel: Panel,
    *,
    label: str,
    options: Mapping[str, Any],
    scores: Mapping[str, pd.Series],
    forced: Sequence[Any] = (),
) -> Key:
    """The key of one path's report inside that run. `scores` is every path's, not this
    path's alone: see the module docstring on why the set a path ran with is part of it."""
    return cache.key("path", {**_traded(panel, options, scores, forced), "path": label})


def path_keys(
    cache: Cache,
    panel: Panel,
    *,
    options: Mapping[str, Any],
    scores: Mapping[str, pd.Series],
    forced: Sequence[Any] = (),
) -> dict[str, Key]:
    """Every path's key, by label, computed once for a run rather than per path.

    The labels are strings, as `backtest.run` names its paths: a cross-validation hands its
    scores over by integer path number and the backtest reports `str(label)`, so stringifying
    here is what makes a key found by the label the report carries."""
    traded = _traded(panel, options, scores, forced)
    return {
        str(label): cache.key("path", {**traded, "path": str(label)}) for label in sorted(scores)
    }


def _traded(
    panel: Panel,
    options: Mapping[str, Any],
    scores: Mapping[str, pd.Series],
    forced: Sequence[Any],
) -> dict[str, Any]:
    """What determines what the engine does: the panel, the options, every path's scores and
    the forced corporate events the operator recorded."""
    return {
        "panel": fingerprint(panel),
        "options": dict(options),
        "paths": {str(label): scores_digest(series) for label, series in scores.items()},
        "forced": [_event(event) for event in forced],
    }


def fingerprint(panel: Panel) -> dict[str, Any]:
    """What identifies the panel a run reads: the lake bytes it was built from, the cost model
    it was costed with, a digest over its rows in order, and a digest over its values.

    The row digest is what lets a cached score series be read back by position, and it is what
    makes a panel rebuilt over a wider window a different panel even when its partition digest
    happens to match. The value digest is what makes a panel rebuilt after a feature formula
    or the labelling changed a different panel: the lake bytes and the rows are the same, but
    what the model learns from is not, and a code version alone does not say so when the
    panel was built by another build than the one certifying it."""
    frame = panel.frame
    keys = frame["as_of"].astype(str).str.cat(frame["ticker"].astype(str), sep="|")
    hasher = hashlib.sha256()
    hasher.update(f"rows={len(frame)}\n".encode())
    hasher.update("\n".join(keys).encode())
    return {
        "partition_hash": panel.partition_hash,
        "cost_model_version": panel.cost_model_version,
        "rows": hasher.hexdigest(),
        "values": values_digest(frame),
    }


def values_digest(frame: pd.DataFrame) -> str:
    """A digest over every column's name and values in row order, the features and the labels
    among them. Numbers are hashed by their bytes, missing values included, as `scores_digest`
    hashes a score series; anything else, the sector and the dates, by its text."""
    hasher = hashlib.sha256()
    hasher.update(f"rows={len(frame)}\n".encode())
    for name in frame.columns:
        column = frame[name]
        hasher.update(f"column={name}\n".encode())
        if pd.api.types.is_numeric_dtype(column.dtype):
            hasher.update(column.to_numpy(dtype="<f8", na_value=np.nan).tobytes())
        else:
            hasher.update("\n".join(str(value) for value in column.tolist()).encode())
    return hasher.hexdigest()


def scores_digest(values: Any) -> str:
    """A digest over a score series' own bytes, missing values included."""
    array = np.asarray(values, dtype="<f8")
    hasher = hashlib.sha256()
    hasher.update(f"len={array.size}\n".encode())
    hasher.update(array.tobytes())
    return hasher.hexdigest()


def _event(event: Any) -> list[str]:
    """One forced event as strings, so a key can hold events carrying decimals and enums."""
    return [f"{field.name}={getattr(event, field.name)}" for field in fields(event)]


def scores_table(panel: Panel, scores: Mapping[Any, pd.Series]) -> pa.Table:
    """Every path's scores, long, in the panel's row order, with the month and name of each
    row written out so the file says for itself what it holds."""
    frame = panel.frame
    as_of = frame["as_of"].astype(str).to_numpy()
    ticker = frame["ticker"].astype(str).to_numpy()
    labels, months, names, values = [], [], [], []
    for label in sorted(scores):
        series = scores[label]
        if len(series) != len(frame):
            raise CacheError(f"path {label} has {len(series)} scores for {len(frame)} panel rows")
        labels.append(np.full(len(frame), str(label), dtype=object))
        months.append(as_of)
        names.append(ticker)
        values.append(np.asarray(series, dtype="float64"))
    if not labels:
        return _SCORES_SCHEMA.empty_table()
    return pa.table(
        {
            "path": pa.array(np.concatenate(labels), type=pa.string()),
            "as_of": pa.array(np.concatenate(months), type=pa.string()),
            "ticker": pa.array(np.concatenate(names), type=pa.string()),
            "score": pa.array(np.concatenate(values), type=pa.float64()),
        },
        schema=_SCORES_SCHEMA,
    )


def scores_from(table: pa.Table, panel: Panel) -> dict[int, pd.Series] | None:
    """The score series the table holds, by path, indexed as the panel's rows are.

    The rows are taken by position, which the key's row digest is what entitles the reader to
    do; the month and name columns are checked against the panel all the same, because a cache
    that has been meddled with should miss rather than feed a certification someone else's
    numbers. A table that does not line up is a miss, like any other unusable entry."""
    frame = panel.frame
    if table.num_rows % len(frame) or not table.num_rows:
        return None
    paths: dict[int, pd.Series] = {}
    as_of = frame["as_of"].astype(str).to_numpy()
    ticker = frame["ticker"].astype(str).to_numpy()
    for start in range(0, table.num_rows, len(frame)):
        chunk = table.slice(start, len(frame))
        labels = set(chunk.column("path").to_pylist())
        if len(labels) != 1:
            return None
        label = labels.pop()
        if not np.array_equal(chunk.column("as_of").to_numpy(zero_copy_only=False), as_of):
            return None
        if not np.array_equal(chunk.column("ticker").to_numpy(zero_copy_only=False), ticker):
            return None
        try:
            key = int(label)
        except ValueError:
            return None
        values = chunk.column("score").to_numpy(zero_copy_only=False)
        paths[key] = pd.Series(np.asarray(values, dtype="float64"), index=frame.index)
    return paths


def standing(ledger: Ledger, trial_id: Any) -> bool:
    """Whether `trial_id` is a succeeded row of this ledger.

    A cached run hands back the trial id of the run that filled the entry instead of writing a
    row of its own, so the id has to be one the ledger the certification ships actually holds.
    A cache carried to another research directory, or one whose ledger was rolled back, misses.
    """
    if not isinstance(trial_id, str) or not trial_id:
        return False
    try:
        return ledger.get(trial_id)["status"] == "succeeded"
    except LedgerError:
        return False


def _atomic(path: Path, write) -> None:
    tmp = path.with_name(path.name + ".tmp")
    write(tmp)
    os.replace(tmp, path)
