"""The trial ledger: every configuration ever tried, in `trials.parquet`.

Nothing trains without a row. `Ledger.trial` appends the row before the run starts, marked
`running`, and rewrites it when the run ends as `succeeded`, `failed` (the error is kept) or
`abandoned` (the operator interrupted it), so a run that never reached its result still shows
what was tried. `train.fit` refuses to train without an open trial to count its refit against.

A row holds what reproducing the run needs: the kind of run, its whole configuration and
feature list as JSON, the training window, the lake partition digest the panel was built
from, the cost-model version, and the code version. `digest` hashes the ledger, so a report
can name exactly which trials it summarises."""

import hashlib
import json
import os
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

FILE = "trials.parquet"
STATUSES = ("running", "succeeded", "failed", "abandoned")
SCHEMA = pa.schema([
    ("trial_id", pa.string()),
    ("kind", pa.string()),
    ("status", pa.string()),
    ("started_at", pa.timestamp("us", "UTC")),
    ("finished_at", pa.timestamp("us", "UTC")),
    ("config", pa.string()),  # JSON: the whole run configuration
    ("features", pa.string()),  # JSON: the feature names the model saw, in order
    ("train_start", pa.date32()),
    ("train_end", pa.date32()),
    ("partition_hash", pa.string()),
    ("cost_model_version", pa.string()),
    ("code_version", pa.string()),
    ("refits", pa.int64()),
    ("results", pa.string()),  # JSON: the run's summary, once it has one
    ("error", pa.string()),
])  # fmt: skip


class LedgerError(Exception):
    """A trial the ledger does not have."""


@dataclass(frozen=True)
class Run:
    """What a finished run hands back: its ledger row's id and the results the row holds."""

    trial_id: str
    results: dict[str, object]


@dataclass
class Trial:
    """One open trial. The run counts each refit against it and hands it its results."""

    trial_id: str
    refits: int = 0
    results: dict[str, object] | None = None
    notes: dict[str, object] = field(default_factory=dict)

    def refit(self) -> None:
        self.refits += 1


def encode(value: object) -> str:
    """Canonical JSON: sorted keys, no whitespace, so equal configurations hash equal."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def _json_default(value: object) -> object:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple | set | frozenset):
        return list(value)
    raise TypeError(f"{type(value).__name__} is not JSON serialisable")


def code_version() -> str:
    return os.environ.get("AGB_VERSION", "dev")


class Ledger:
    """The ledger at `path`. A missing file is an empty ledger; the first trial creates it."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def table(self) -> pa.Table:
        if not self.path.exists():
            return SCHEMA.empty_table()
        return pq.read_table(self.path, schema=SCHEMA)

    def rows(self) -> list[dict[str, object]]:
        return self.table().to_pylist()

    def count(self) -> int:
        return self.table().num_rows

    def get(self, trial_id: str) -> dict[str, object]:
        for row in self.rows():
            if row["trial_id"] == trial_id:
                return row
        raise LedgerError(f"no trial {trial_id!r} in {self.path}")

    def digest(self) -> str:
        """A SHA-256 over every row, in the order they were appended."""
        hasher = hashlib.sha256()
        for row in self.rows():
            hasher.update(encode(row).encode())
            hasher.update(b"\n")
        return hasher.hexdigest()

    @contextmanager
    def trial(
        self,
        kind: str,
        config: dict[str, object],
        *,
        features: tuple[str, ...],
        train_start: date,
        train_end: date,
        partition_hash: str,
        cost_model_version: str,
    ) -> Iterator[Trial]:
        """Record a run. The row is written as `running` before the body starts and rewritten
        with its outcome when the body ends, however it ends."""
        started = datetime.now(UTC)
        row: dict[str, object] = {
            "trial_id": None,
            "kind": kind,
            "status": "running",
            "started_at": started,
            "finished_at": None,
            "config": encode(config),
            "features": encode(list(features)),
            "train_start": train_start,
            "train_end": train_end,
            "partition_hash": partition_hash,
            "cost_model_version": cost_model_version,
            "code_version": code_version(),
            "refits": 0,
            "results": None,
            "error": None,
        }
        row["trial_id"] = _trial_id(row)
        self._write([*self.rows(), row])
        trial = Trial(row["trial_id"])
        try:
            yield trial
        except KeyboardInterrupt:
            row["status"], row["error"] = "abandoned", "interrupted"
            raise
        except BaseException as error:
            row["status"] = "failed"
            row["error"] = "".join(traceback.format_exception_only(error)).strip()
            raise
        else:
            row["status"] = "succeeded"
            row["results"] = None if trial.results is None else encode(trial.results)
        finally:
            row["finished_at"] = datetime.now(UTC)
            row["refits"] = trial.refits
            self._replace(row)

    def _replace(self, row: dict[str, object]) -> None:
        rows = [row if r["trial_id"] == row["trial_id"] else r for r in self.rows()]
        self._write(rows)

    def _write(self, rows: list[dict[str, object]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows, schema=SCHEMA)
        tmp = self.path.with_name(self.path.name + ".tmp")
        pq.write_table(table, tmp)
        os.replace(tmp, self.path)


def _trial_id(row: dict[str, object]) -> str:
    """Sixteen hex digits over what the trial is and when it started: the same configuration
    tried twice gets two rows with two ids."""
    keyed = {k: v for k, v in row.items() if k not in {"trial_id", "finished_at", "results"}}
    return hashlib.sha256(encode(keyed).encode()).hexdigest()[:16]
