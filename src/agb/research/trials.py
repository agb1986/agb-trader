"""The trial ledger: every configuration ever tried, in `trials.parquet`.

Nothing trains without a row. `Ledger.trial` appends the row before the run starts, marked
`running`, and rewrites it when the run ends as `succeeded`, `failed` (the error is kept) or
`abandoned` (the operator interrupted it), so a run that never reached its result still shows
what was tried. `train.fit` refuses to train without an open trial to count its refit against.

A row holds what reproducing the run needs: the kind of run, its whole configuration and
feature list as JSON, the training window, the lake partition digest the panel was built
from, the cost-model version, and the code version. `digest` hashes the ledger, so a report
can name exactly which trials it summarises.

Every row carries `previous`, a digest of the row appended before it, so the rows form a chain:
taking a row out of the middle, putting one in or reordering them leaves a row whose link no
longer matches, and `Ledger.verify` names it. Certification verifies the chain before it runs
and the artifact import verifies the shipped copy, so a failed trial cannot be dropped quietly
to make the declared count smaller. The link covers what a row tried and where it sits, not how
it ended, because a row's outcome is rewritten when its run finishes. Rows written before the
chain existed have no link; they must all come before the first row that has one. Every write
reads, changes and replaces the file under an exclusive lock on `trials.parquet.lock`, through
a temporary file whose name no other writer shares, so two runs appending at once both land.

Rows are not trials, for the purpose a certification declares a trial count for. One
certification writes three rows, its own and the cross-validation and backtest it nests, and
the same configuration rerun after a crash or rechecked by `reproduce` writes another row
every time; none of that is another configuration searched. `declared` therefore counts the
distinct configurations the ledger holds, by the rule `RULE`, and `count` stays the row
count. What each rule means, and what none of them can see, is
docs/architecture/01-data-broker-backtesting.md and `agb.research.certify`."""

import contextvars
import fcntl
import hashlib
import json
import os
import stat
import tempfile
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
# How a certification reads its declared trial count off the ledger. `rows` is every row,
# which is what certifications up to lgbm-2026-10 declared; `configurations` is the distinct
# configurations that came back with a result, nested by clock; `started` is the distinct
# configurations started, whatever became of them, nested by the parent a row names, which is
# what they declare now. Each certification's manifest records the rule it used, so an
# artifact is checked against the rule it was certified under rather than the one in force
# when it is read.
RULES = ("rows", "configurations", "started")
RULE = "started"
# What the first chained row links to, when nothing was appended before it.
GENESIS = "0" * 64
# What a row's link covers: what it tried, which run it ran inside, and the row before it. Not
# the outcome, which `Ledger.trial` rewrites when the run ends.
LINKED = (
    "trial_id", "kind", "started_at", "config", "features", "train_start", "train_end",
    "partition_hash", "cost_model_version", "code_version", "parent", "previous",
)  # fmt: skip
# The fields that say what a run tried. Two rows that agree on all of them are one
# configuration evaluated twice, not two configurations: a rerun after a crash, a
# reproduction, a certification attempt that fell over and was started again. The window,
# the partition digest and the cost-model version are part of it, so the same model
# configuration over a rebuilt panel is a configuration tried again, not the same reading.
IDENTITY = (
    "kind", "config", "features", "train_start", "train_end",
    "partition_hash", "cost_model_version", "code_version",
)  # fmt: skip
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
    ("parent", pa.string()),  # the trial this one ran inside, if any
    ("previous", pa.string()),  # `link` of the row before; null on rows from before the chain
])  # fmt: skip
# The trials open in this context, innermost last, as (ledger path, trial id): a trial opened
# while another on the same ledger is open names that one as its parent. A context variable
# rather than an argument, so the cross-validation and backtest a certification runs need no
# new parameter, and a run in another thread or process is nobody's child.
_OPEN: contextvars.ContextVar[tuple[tuple[str, str], ...]] = contextvars.ContextVar(
    "open_trials", default=()
)


class LedgerError(Exception):
    """A trial the ledger does not have, or a ledger whose rows do not chain."""


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


def configuration(row: dict[str, object]) -> tuple[object, ...]:
    """What the row tried, with when it was tried and how it went left out."""
    return tuple(row[name] for name in IDENTITY)


def _closes_at(row: dict[str, object], current: str | None) -> datetime:
    """When the row stopped being a run that could hold other runs inside it.

    A row still marked `running` is either the run asking the question, which is open until it
    finishes, or a run that was killed and never wrote its end. The first is open to the end of
    time; the second closes at its own start, because treating a zombie row as open would make
    every trial since it look like one of its children.
    """
    finished = row["finished_at"]
    if isinstance(finished, datetime):
        return finished
    if row["trial_id"] == current:
        return datetime.max.replace(tzinfo=UTC)
    started = row["started_at"]
    assert isinstance(started, datetime)
    return started


def link(row: dict[str, object]) -> str:
    """The digest the row after `row` carries as its `previous`."""
    linked = {name: row.get(name) for name in LINKED}
    return hashlib.sha256(encode(linked).encode()).hexdigest()


def broken(rows: list[dict[str, object]]) -> str | None:
    """What is wrong with the chain through `rows`, or None when every link holds.

    Each chained row must carry the link of the row before it, or `GENESIS` when it is the
    first; the rows from before the chain must all come first; and a parent must be a row that
    was appended earlier. A row taken off the end leaves a chain that still holds, which is
    what `ledger_rows` in a certification's manifest is for.
    """
    seen: set[object] = set()
    chained = False
    for index, row in enumerate(rows):
        trial_id, previous, parent = row["trial_id"], row.get("previous"), row.get("parent")
        if trial_id in seen:
            return f"trial {trial_id} appears twice"
        if previous is None:
            if chained:
                return f"row {index}, trial {trial_id}, has no link but follows rows that do"
        else:
            if previous != (link(rows[index - 1]) if index else GENESIS):
                return (
                    f"row {index}, trial {trial_id}, does not link to the row before it: a row"
                    " was removed, inserted or reordered"
                )
            chained = True
        if parent is not None and parent not in seen:
            return f"row {index}, trial {trial_id}, names parent {parent}, which is not before it"
        seen.add(trial_id)
    return None


def nested(rows: list[dict[str, object]], *, current: str | None = None) -> set[str]:
    """The ids of the rows a run opened while another run was still open.

    A certification runs a cross-validation and a backtest and each writes its own row, so its
    interval contains theirs. That is one configuration being evaluated, not three, so the
    inner rows are not configurations the search tried. Containment is read off the intervals,
    which is all a row from before the `parent` column carries; rows that start at the same
    instant as another are siblings, not children, so a batch written in one go stays counted.
    """
    inside = set()
    for row in rows:
        start, end = row["started_at"], _closes_at(row, current)
        for other in rows:
            if other["trial_id"] == row["trial_id"]:
                continue
            if other["started_at"] < start and _closes_at(other, current) >= end:
                inside.add(str(row["trial_id"]))
                break
    return inside


def roots(rows: list[dict[str, object]], *, current: str | None = None) -> dict[str, dict]:
    """Each row's id, mapped to the run at its root: the run it was the mechanics of.

    A chained row follows the `parent` links up to a row that names none, so a run that merely
    overlapped another, from a second terminal, is its own root. A row from before the chain is
    read by its interval, as `nested` reads it: the first run around it that is inside none.
    """
    by_id = {row["trial_id"]: row for row in rows}
    inside = nested(rows, current=current)
    outer = [row for row in rows if row["trial_id"] not in inside]
    found = {}
    for row in rows:
        top = row
        if row.get("previous") is None:
            if row["trial_id"] in inside:
                start, end = row["started_at"], _closes_at(row, current)
                top = next(
                    (o for o in outer if o["started_at"] < start and _closes_at(o, current) >= end),
                    row,
                )
        else:
            for _ in rows:  # bounded, so a parent cycle in an unverified ledger cannot spin
                if top.get("parent") not in by_id:
                    break
                top = by_id[top["parent"]]
        found[str(row["trial_id"])] = top
    return found


def _children(rows: list[dict[str, object]], *, current: str | None) -> set[str]:
    """The ids of the rows the `started` rule folds into another run.

    A row that names a parent is folded into it when the parent's interval holds it. A row
    without a link is from before the chain and parents, and is folded by `nested` as it always
    was. A chained row that names no parent ran on its own, so it counts even if it ran while
    another trial was open, from another terminal. The interval check on a parent keeps every
    row this folds one that `nested` folds as well, so `started` never counts fewer than
    `configurations`: the children of a run killed before it wrote its end still count, as they
    did.
    """
    by_id = {row["trial_id"]: row for row in rows}
    clock = nested(rows, current=current)
    inside = set()
    for row in rows:
        trial_id = str(row["trial_id"])
        parent = by_id.get(row.get("parent"))
        if row.get("previous") is None:
            if trial_id in clock:
                inside.add(trial_id)
        elif parent is not None and (
            parent["started_at"] < row["started_at"]
            and _closes_at(parent, current) >= _closes_at(row, current)
        ):
            inside.add(trial_id)
    return inside


def declared(rows: list[dict[str, object]], *, rule: str = RULE, current: str | None = None) -> int:
    """How many trials a certification over `rows` declares, under `rule`.

    `rows` counts every row, which is what certifications up to lgbm-2026-10 declared.
    `configurations` counts the distinct configurations that were searched and came back with a
    result: rows nested inside another run's interval are the mechanics of one evaluation,
    repeats of a configuration are one configuration, and a configuration no attempt of which
    ever succeeded is left out. The run asking counts itself, since by the time anything
    verifies the number its own row has succeeded. `started` counts the distinct configurations
    that were started, however they ended: a run that failed or was abandoned was still a try,
    and whoever chose what to certify saw that it fell over. Rows are folded into the run they
    name as their parent, see `_children`, rather than into whatever run happened to be open.
    """
    if rule == "rows":
        return len(rows)
    if rule == "started":
        inside = _children(rows, current=current)
        return len({configuration(row) for row in rows if row["trial_id"] not in inside})
    if rule != "configurations":
        raise LedgerError(f"unknown trial count rule {rule!r}; expected one of {RULES}")
    inside = nested(rows, current=current)
    counted = {
        configuration(row)
        for row in rows
        if row["trial_id"] not in inside
        and (row["status"] == "succeeded" or row["trial_id"] == current)
    }
    return len(counted)


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
        """Every row in the ledger, whatever it is. `declared` is what a certification says."""
        return self.table().num_rows

    def declared(self, *, rule: str = RULE, current: str | None = None) -> int:
        """The trial count a certification declares, by `rule`. See the module function."""
        return declared(self.rows(), rule=rule, current=current)

    def get(self, trial_id: str) -> dict[str, object]:
        for row in self.rows():
            if row["trial_id"] == trial_id:
                return row
        raise LedgerError(f"no trial {trial_id!r} in {self.path}")

    def verify(self) -> None:
        """Raise `LedgerError` unless every row links to the one before it. See `broken`."""
        problem = broken(self.rows())
        if problem is not None:
            raise LedgerError(f"the chain through {self.path} is broken: {problem}")

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
        with its outcome when the body ends, however it ends. A trial opened inside another on
        the same ledger, in the same thread, names it as its parent."""
        key = str(self.path.resolve())
        parent = next((t for p, t in reversed(_OPEN.get()) if p == key), None)
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
            "parent": parent,
            "previous": None,
        }
        self._append(row)
        token = _OPEN.set((*_OPEN.get(), (key, str(row["trial_id"]))))
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
            try:
                self._replace(row)
            finally:
                _OPEN.reset(token)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        """Hold the ledger's write lock: one writer reads, changes and replaces it at a time."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.with_name(self.path.name + ".lock").open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            yield

    def _append(self, row: dict[str, object]) -> None:
        """Link `row` to the last row, give it its id and append it, all under the lock, so the
        link is to the row that really is last."""
        with self._locked():
            rows = self.rows()
            row["previous"] = link(rows[-1]) if rows else GENESIS
            row["trial_id"] = _trial_id(row)
            self._write([*rows, row])

    def _replace(self, row: dict[str, object]) -> None:
        with self._locked():
            rows = [row if r["trial_id"] == row["trial_id"] else r for r in self.rows()]
            self._write(rows)

    def _write(self, rows: list[dict[str, object]]) -> None:
        """Replace the file with `rows`, through a temporary file only this write uses, so a
        reader sees the old ledger or the new one and never half of either."""
        table = pa.Table.from_pylist(rows, schema=SCHEMA)
        mode = stat.S_IMODE(self.path.stat().st_mode) if self.path.exists() else 0o644
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=self.path.name + ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as handle:
                os.fchmod(handle.fileno(), mode)
                pq.write_table(table, handle)
            os.replace(tmp, self.path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise


def _trial_id(row: dict[str, object]) -> str:
    """Sixteen hex digits over what the trial is and when it started: the same configuration
    tried twice gets two rows with two ids."""
    keyed = {k: v for k, v in row.items() if k not in {"trial_id", "finished_at", "results"}}
    return hashlib.sha256(encode(keyed).encode()).hexdigest()[:16]
