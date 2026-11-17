"""Reproducing a trial from its ledger row.

A row names everything its run depended on: the kind of run, its whole configuration, the
lake partition digest of the panel it learned from, and the code version that ran it. Given a
panel with the same digest, under the same code version, the run is done again and its
results compared with the row's. A panel built from a lake that has moved, or a checkout that
is not the one the trial ran, is refused rather than compared: either would be a different
configuration, and a difference would say nothing about whether the trial reproduces.

Every row a reproduction writes is marked with the trial it reproduces (see
`Ledger.reproducing`). The rows still count as rows, and since the configuration is the
trial's own, the rules that count configurations count no new trial for it.

Cross-validations, walk-forwards, scans and backtests rerun as themselves. A certification
reruns as a whole into a scratch research directory, with nothing taken from the cache, and is
compared on its paths, on what its cross-validation and backtest returned, and on its verdict:
the gates are run again over the rerun's paths and the ledger the certification counted, which
is the ledger's first `ledger_rows` rows, so rows appended since, the reproduction's own among
them, cannot move the count it is compared on. A row nested inside another run, such as the
model-score backtest of a certification, is reproduced by rerunning the run at its root and
comparing the matching row of the rerun."""

import json
import math
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from agb import lake
from agb.research import (
    artifact,
    backtest,
    certify,
    cpcv,
    gates,
    recompute,
    scan,
    train,
    walkforward,
)
from agb.research.panel import Panel
from agb.research.trials import Ledger, LedgerError, Run, code_version, configuration, roots


class ReproduceError(Exception):
    pass


@dataclass(frozen=True)
class Reproduction:
    original: dict[str, Any]
    rerun: Run
    matches: bool
    # Where the rerun parted from the original, one line each; empty when it matches.
    differences: tuple[str, ...] = ()


# What a certification's results are compared on directly. The rest is the verdict, which
# depends on the ledger the run read, and that has grown by at least the original since: it is
# compared through `recompute.verdict` over the ledger the original counted instead, which is
# where a count that moved would show. The ids of the rows the rerun wrote are new by
# construction; what those rows hold is compared too, see `_compare`.
_CERTIFICATION_OUTCOME = (
    "paths", "cpcv", "backtest", "model_id", "certified_capital", "declared_trials_rule",
)  # fmt: skip
# How the lines name the two sides.
_LABELS = ("original", "rerun")


def rerun(
    row: dict[str, Any],
    panel: Panel,
    ledger: Ledger,
    *,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
) -> Run:
    """Run the trial `row` describes again, over `panel`, recording a new trial."""
    if panel.partition_hash != row["partition_hash"]:
        raise ReproduceError(
            f"the panel was built from partitions {panel.partition_hash[:12]}, not the trial's"
            f" {row['partition_hash'][:12]}; sync the lake the trial saw"
        )
    if panel.cost_model_version != row["cost_model_version"]:
        raise ReproduceError(
            f"the trial ran under cost model {row['cost_model_version']!r},"
            f" not {panel.cost_model_version!r}"
        )
    if row["code_version"] != code_version():
        raise ReproduceError(
            f"the trial ran under code version {row['code_version']!r}, and this is"
            f" {code_version()!r}; a rerun under other code is a new configuration, not a"
            " reproduction, so check out the trial's version and set AGB_VERSION to it"
        )
    config = json.loads(row["config"])
    kind = row["kind"]
    if kind == cpcv.KIND:
        model = train.Config.from_dict(config["model"])
        return cpcv.run(panel, model, cpcv.Options(**config[cpcv.KIND]), ledger)
    if kind == walkforward.KIND:
        model = train.Config.from_dict(config["model"])
        windows = walkforward.Options(**config[walkforward.KIND])
        return walkforward.run(panel, model, windows, ledger)
    if kind == scan.KIND:
        search = scan.Options(**config[scan.KIND])
        return scan.run(panel, search, ledger, lake_dir=lake_dir, db=db)
    if kind == backtest.KIND:
        costs = backtest.Options(**config[backtest.KIND])
        return backtest.run(panel, costs, ledger, lake_dir=lake_dir, db=db)
    if kind == artifact.KIND:
        return _certify(config, panel, ledger, lake_dir=lake_dir, db=db)
    raise ReproduceError(f"no way to rerun a {kind!r} trial")


def reproduce(
    ledger: Ledger,
    trial_id: str,
    panel: Panel,
    *,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
) -> Reproduction:
    """Rerun `trial_id` and compare. A nested row reruns the run at its root."""
    try:
        before = ledger.rows()
        original = ledger.get(trial_id)
    except LedgerError as error:
        raise ReproduceError(str(error)) from None
    if original["status"] != "succeeded":
        raise ReproduceError(f"trial {trial_id} {original['status']}; it has no results to match")
    root = roots(before)[trial_id]
    if root["status"] != "succeeded":
        raise ReproduceError(
            f"trial {trial_id} ran inside {root['trial_id']}, which {root['status']};"
            " only a run that finished can be run again to reach it"
        )
    with ledger.reproducing(trial_id):
        repeat = rerun(root, panel, ledger, lake_dir=lake_dir, db=db)
    after = ledger.rows()
    if root["trial_id"] != trial_id:
        repeat = _counterpart(original, root, before, repeat.trial_id, after)
    found = _compare(original, repeat, before, after)
    return Reproduction(
        original=original, rerun=repeat, matches=not found, differences=tuple(found)
    )


def same(left: Any, right: Any) -> bool:
    """Whether two results agree, with floats compared to nine significant figures."""
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(same(left[k], right[k]) for k in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(same(a, b) for a, b in zip(left, right, strict=True))
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, int | float) and isinstance(right, int | float):
        return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-12)
    return left == right


def _certify(
    config: dict[str, Any],
    panel: Panel,
    ledger: Ledger,
    *,
    lake_dir: Path,
    db: duckdb.DuckDBPyConnection | None,
) -> Run:
    """A certification again, from its row's configuration, into a scratch directory: the
    artifact a reproduction writes is a by-product and never one to import, and the cache is
    off so every stage is computed rather than handed back from the original's run."""
    settings = dict(config[artifact.KIND])
    thresholds = gates.Thresholds(**settings["thresholds"])
    options = certify.Options(
        model_id=settings["model_id"],
        size=settings["size"],
        hold_through=settings["hold_through"],
        thresholds=thresholds,
    )
    model = train.Config.from_dict(config["model"])
    validation = cpcv.Options(**config["cpcv"])
    with tempfile.TemporaryDirectory(prefix="reproduce-") as scratch:
        try:
            done = certify.run(
                panel,
                model,
                validation,
                options,
                ledger,
                research_dir=Path(scratch),
                lake_dir=lake_dir,
                db=db,
                checkpoint=False,
            )
        except (certify.CertificationError, backtest.BacktestError) as error:
            raise ReproduceError(f"the certification did not run again: {error}") from None
    row = ledger.get(done.trial_id)
    return Run(done.trial_id, json.loads(str(row["results"])))


def _family(rows: list[dict[str, Any]], root_id: str) -> list[dict[str, Any]]:
    """The rows whose root is `root_id`, in the order they were appended."""
    found = roots(rows)
    return [row for row in rows if found[row["trial_id"]]["trial_id"] == root_id]


def _counterpart(
    original: dict[str, Any],
    root: dict[str, Any],
    before: list[dict[str, Any]],
    rerun_root: str,
    after: list[dict[str, Any]],
) -> Run:
    """The row of the rerun that stands where `original` stood in the run it was nested in:
    the same configuration, at the same position among that run's rows of it."""
    wanted = configuration(original)
    mine = [r["trial_id"] for r in _family(before, root["trial_id"]) if configuration(r) == wanted]
    theirs = [r for r in _family(after, rerun_root) if configuration(r) == wanted]
    index = mine.index(original["trial_id"])
    if index >= len(theirs):
        raise ReproduceError(
            f"the rerun of {root['trial_id']} wrote no row like {original['trial_id']}"
        )
    row = theirs[index]
    return Run(row["trial_id"], json.loads(row["results"] or "null"))


def _compare(
    original: dict[str, Any],
    repeat: Run,
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> list[str]:
    recorded = json.loads(original["results"])
    if original["kind"] != artifact.KIND:
        return recompute.differences(recorded, repeat.results, labels=_LABELS)
    found = recompute.differences(
        {key: recorded.get(key) for key in _CERTIFICATION_OUTCOME},
        {key: repeat.results.get(key) for key in _CERTIFICATION_OUTCOME},
        labels=_LABELS,
    )
    # What the certification's cross-validation and backtest returned, beyond the summaries
    # its own results carry: the backtest's paths hold the participation the gates read.
    by_id = {row["trial_id"]: row for row in after}
    for key in ("cpcv_trial_id", "backtest_trial_id"):
        mine = json.loads(by_id[recorded[key]]["results"] or "null")
        theirs = json.loads(by_id[repeat.results[key]]["results"] or "null")
        found += recompute.differences(
            mine, theirs, at=key.removesuffix("_trial_id"), labels=_LABELS
        )
    paths = repeat.results["paths"]
    assert isinstance(paths, list)
    found += _verdict(original, recorded, paths, before)
    return found


def _verdict(
    original: dict[str, Any],
    recorded: dict[str, Any],
    paths: list[dict[str, Any]],
    before: list[dict[str, Any]],
) -> list[str]:
    """The gates over the rerun's paths and the ledger the original counted, against the
    verdict the original recorded, in the fields its manifest carries."""
    shipped = before[: recorded["ledger_rows"]]
    manifest = {
        "trial_id": original["trial_id"],
        "backtest_trial_id": recorded["backtest_trial_id"],
        "partition_hash": original["partition_hash"],
        "cost_model_version": original["cost_model_version"],
        "train_start": original["train_start"].isoformat(),
        "train_end": original["train_end"].isoformat(),
        "gates": {"thresholds": recorded["thresholds"]},
    }
    try:
        again = recompute.verdict(manifest, paths, shipped)
    except recompute.RecomputeError as error:
        raise ReproduceError(f"the gates cannot be run again: {error}") from None
    written = json.loads(
        json.dumps(
            recompute.recorded(
                recorded, paths=len(recorded["paths"]), dropped=recorded["candidates_dropped"]
            )
        )
    )
    return recompute.differences(written, again, at="verdict", labels=_LABELS)
