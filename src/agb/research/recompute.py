"""A certification's verdict, recomputed from what its artifact ships (issue #211).

`agb.research.certify` gates a model over the returns it measured and the ledger it ran in, and
writes the verdict into the manifest. A digest over the manifest says the manifest has not
changed since it was written; it cannot say the verdict in it is the one the returns give,
because whoever writes the manifest writes the digest too. So the server does not take the
verdict as written. `verdict` runs the same gates over the same inputs, every one of which the
artifact ships:

    paths.json       each path's monthly net and gross returns at 1x cost, net at 2x, and the
                     benchmark's return over the same month, which must be the paths the
                     certification's own ledger row recorded
    trials.parquet   the ledger: the declared trial count the deflated Sharpe ratio charges
                     for, counted under `trials.RULE` and no other; every earlier backtest
                     over the same panel, which is a column of the overfitting matrix; and
                     the validation backtest's row, which holds each path's participation

and the manifest supplies only what selects among them: the panel's window, partition and cost
model, the trial ids of the certification and its backtest, and the thresholds, which may be
stricter than `gates.Thresholds()` and never looser. `differences` then names every field
where the manifest and the recomputation disagree, and the import refuses an artifact with any.

The comparison allows `REL_TOL` relative and `ABS_TOL` absolute difference on a number and
none on anything else. The recomputation runs the same code over the same bytes, so the only
honest difference is a last-bit one from a floating-point sum taken in another order on another
machine; a gate's own resolution is many orders of magnitude coarser, so no tolerance this small
can let a verdict through that the returns do not give. A flipped verdict, a flipped gate or a
count off by one is a difference, as is a PBO whose rank tie broke the other way.

What this cannot catch is a change to the returns themselves made consistently everywhere they
are recorded: paths.json, the certification's ledger row and the candidate rows. The ledger's
chain covers what each row tried and not what it measured, so a ledger rewritten that way still
chains. That is a forgery of the research, not of the verdict, and the defence against it is
that the operator certifies on their own workstation from their own lake.

This module imports numpy, `agb.research.gates` and `agb.research.trials` and nothing else
outside the standard library, so the server's app image can run it; tests/test_artifact.py
holds that line."""

import json
import math
from collections.abc import Sequence
from dataclasses import dataclass, fields
from datetime import date
from typing import Any

import numpy as np

from agb.research import gates
from agb.research.trials import RULE, configuration, declared, roots

PATHS = "paths.json"
# The trial kinds whose results carry monthly net returns the overfitting gate can read.
CANDIDATE_KINDS = ("backtest",)
# How far a recomputed number may sit from the manifest's; see the module docstring.
REL_TOL = 1e-9
ABS_TOL = 1e-12


class RecomputeError(Exception):
    """An artifact whose verdict cannot be recomputed from what it ships, and why."""


@dataclass(frozen=True)
class Candidates:
    """The overfitting gate's columns, one per configuration and keyed by the trial id of the
    row that supplied it, what was left out and why, and which of those leave a declared
    configuration without the column it should have, which fails the gate."""

    series: dict[str, np.ndarray]
    dropped: list[dict[str, str]]
    missing: list[str]


def candidates(
    rows: list[dict[str, Any]],
    *,
    partition_hash: str,
    cost_model_version: str,
    start: date,
    end: date,
    labels: Sequence[str],
    exclude: set[str] = frozenset(),
    current: str | None = None,
) -> Candidates:
    """The monthly net return series the overfitting gate ranks a model against, one per
    configuration among the succeeded backtests in `rows` over the panel `partition_hash`,
    `cost_model_version`, `start` and `end` describe, keyed by the trial id of the run that
    supplied it. A backtest over several paths contributes the paths' mean month.

    A configuration is what `trials.configuration` says of the run at the root of a backtest's
    `parent` links (`trials.roots`; by interval for rows from before the chain): every
    certification trades its scores through a backtest whose own configuration is the same
    whatever the model, so the backtest's row alone cannot tell two models apart, and the
    certification around it can. `current` is the certification asking, and anything of its
    configuration is this run, which is the matrix's first column already; `exclude` names
    rows to leave out besides. The first usable series of a configuration is its column and
    later runs of it are reruns, which add none.

    A backtest's recorded window is the panel it was handed, while `labels` is the shorter
    window it could trade, so the series a candidate contributes is held to the traded length:
    two runs whose months differ are not two readings of the same thing. Every series left out
    is in `dropped` with its reason, and a configuration none of whose runs left a usable
    series is in `missing`, which fails the gate: it was tried, and a PBO without it is not
    the PBO of the search."""
    top = roots(rows, current=current)
    mine = next((configuration(r) for r in rows if r["trial_id"] == current), None)
    series: dict[str, np.ndarray] = {}
    columns: dict[tuple[object, ...], str] = {}
    unusable: dict[tuple[object, ...], list[str]] = {}
    dropped: list[dict[str, str]] = []

    def drop(row: dict[str, Any], reason: str) -> None:
        dropped.append({"trial_id": str(row["trial_id"]), "reason": reason})

    for row in rows:
        if row["kind"] not in CANDIDATE_KINDS or row["trial_id"] in exclude:
            continue
        if row["status"] != "succeeded":
            drop(row, f"status {row['status']}")
            continue
        if (
            row["partition_hash"] != partition_hash
            or row["cost_model_version"] != cost_model_version
            or row["train_start"] != start
            or row["train_end"] != end
        ):
            drop(row, "another panel, window or cost model")
            continue
        identity = configuration(top[str(row["trial_id"])])
        if identity == mine:
            drop(row, "this certification's own configuration")
            continue
        if identity in columns:
            drop(row, f"rerun of the configuration of {columns[identity]}")
            continue
        found, reason = series_of(row, labels)
        if found is None:
            drop(row, reason)
            unusable.setdefault(identity, []).append(f"{row['trial_id']}: {reason}")
            continue
        columns[identity] = str(row["trial_id"])
        series[str(row["trial_id"])] = found
    missing = [
        "; ".join(reasons) for identity, reasons in unusable.items() if identity not in columns
    ]
    return Candidates(series, dropped, missing)


def series_of(row: dict[str, Any], labels: Sequence[str]) -> tuple[np.ndarray | None, str]:
    """A candidate row's mean monthly net return series, or none and why it cannot be read."""
    if not row["results"]:
        return None, "no results"
    results = json.loads(row["results"])
    if "paths" in results:
        found = matrix(results["paths"], "net").mean(axis=0)
    elif "monthly" in results:
        found = np.array([month["net"] for month in results["monthly"]], dtype="float64")
    else:
        return None, "no monthly returns"
    if len(found) != len(labels):
        return None, f"{len(found)} months, {len(labels)} traded"
    if not np.isfinite(found).all():
        return None, "a month that is not finite"
    return found, ""


def matrix(paths: list[dict[str, Any]], key: str) -> np.ndarray:
    """The paths-by-months matrix of each path's monthly `key`."""
    return np.array([[month[key] for month in path["monthly"]] for path in paths], dtype="float64")


def benchmark(paths: list[dict[str, Any]]) -> tuple[np.ndarray | None, str | None]:
    """The benchmark's return over each path's months, or why there is none: any month of any
    path the lake has no benchmark return for makes the whole benchmark missing, because a gate
    over the months that happen to be covered is not the gate over the run."""
    values = [[month.get("benchmark") for month in path["monthly"]] for path in paths]
    absent = sum(value is None for row in values for value in row)
    if absent:
        total = sum(len(row) for row in values)
        return None, (
            f"the lake holds no {gates.BENCHMARK} return for {absent} of {total} traded path-months"
        )
    return np.array(values, dtype="float64"), None


def recorded(
    verdict: dict[str, Any], *, paths: int, dropped: list[dict[str, str]]
) -> dict[str, Any]:
    """The part of a certification's manifest that is the gates' verdict, from what
    `gates.evaluate` returned, the number of paths it read and the candidates left out of the
    overfitting matrix. The certification writes exactly this into the manifest and the server
    recomputes exactly this, so the two cannot drift apart in what they cover."""
    return {
        "passed": verdict["passed"],
        "declared_trials": verdict["declared_trials"],
        "dsr": verdict["dsr"],
        "pbo": verdict["pbo"],
        "sharpe_net": verdict["sharpe_net"],
        "dsr_2x": verdict["dsr_2x"],
        "sharpe_basis": verdict["sharpe_basis"],
        "excess": verdict["excess"],
        "sharpe_total": verdict["sharpe_total"],
        "break_even_multiple": verdict["break_even_multiple"],
        "sharpe_gross": verdict["sharpe_gross"],
        "gross_sharpe_flag": verdict["gross_sharpe_flag"],
        "participation_max": verdict["participation_max"],
        "participation_mean": verdict["participation_mean"],
        "path_quantiles": verdict["path_quantiles"],
        "gates": {
            **verdict["gates"],
            "thresholds": verdict["thresholds"],
            "start_dates": verdict["start_dates"],
            "start_date_stable": verdict["start_date_stable"],
            "paths": paths,
            "months": verdict["months"],
            "candidates": verdict["overfitting"]["candidates"],
            "candidates_dropped": dropped,
        },
    }


def thresholds(recorded: dict[str, Any]) -> gates.Thresholds:
    """The thresholds a manifest says it was gated at, refused if any is looser than this
    code's: a deflated Sharpe bar lower, an overfitting bar higher, a Sharpe bar lower or a
    suspicion flag raised later, or another CSCV block count. Stricter ones are a certification
    that asked more of the model, and its verdict is recomputed at them."""
    names = {f.name for f in fields(gates.Thresholds)}
    if not isinstance(recorded, dict) or set(recorded) != names:
        raise RecomputeError(f"the manifest's thresholds are not {sorted(names)}")
    try:
        found = gates.Thresholds(**recorded)
    except (TypeError, ValueError) as error:
        raise RecomputeError(f"the manifest's thresholds are not valid: {error}") from None
    floor = gates.Thresholds()
    looser = [
        name
        for name, worse in (
            ("dsr", found.dsr < floor.dsr),
            ("pbo", found.pbo > floor.pbo),
            ("sharpe", found.sharpe < floor.sharpe),
            ("suspicious_sharpe", found.suspicious_sharpe > floor.suspicious_sharpe),
            ("blocks", found.blocks != floor.blocks),
        )
        if worse
    ]
    if looser:
        raise RecomputeError(
            "the certification was gated at thresholds looser than this code's: "
            + ", ".join(f"{name} {getattr(found, name)} against {getattr(floor, name)}"
                        for name in looser)
        )  # fmt: skip
    return found


def verdict(
    manifest: dict[str, Any], paths: list[dict[str, Any]], rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """What `recorded` gives when the gates are run again over `paths`, the artifact's
    paths.json, and `rows`, its shipped ledger, for the certification `manifest` describes.
    Normalised through JSON, as the manifest itself was, so the two compare like for like."""
    by_id = {row["trial_id"]: row for row in rows}
    backtest = by_id.get(manifest["backtest_trial_id"])
    if backtest is None or backtest["kind"] != "backtest" or backtest["status"] != "succeeded":
        raise RecomputeError(
            f"the shipped ledger holds no succeeded backtest {manifest['backtest_trial_id']}"
        )
    traded = json.loads(backtest["results"] or "null") or {}
    if not paths or len(traded.get("paths", ())) != len(paths):
        raise RecomputeError("the backtest's paths are not the paths the artifact ships")
    labels = [month["as_of"] for month in paths[0]["monthly"]]
    net = matrix(paths, "net")
    others = candidates(
        rows,
        partition_hash=manifest["partition_hash"],
        cost_model_version=manifest["cost_model_version"],
        start=date.fromisoformat(manifest["train_start"]),
        end=date.fromisoformat(manifest["train_end"]),
        labels=labels,
        exclude={manifest["trial_id"], manifest["backtest_trial_id"]},
        current=manifest["trial_id"],
    )
    found, missing = benchmark(paths)
    try:
        result = gates.evaluate(
            net=net,
            gross=matrix(paths, "gross"),
            net_2x=matrix(paths, "net_2x"),
            candidates=np.column_stack([net.mean(axis=0), *others.series.values()]),
            trials=declared(rows, rule=RULE),
            months=labels,
            participation=[(p["participation_max"], p["participation_mean"])
                           for p in traded["paths"]],
            thresholds=thresholds(manifest["gates"]["thresholds"]),
            missing=others.missing,
            benchmark=found,
            benchmark_missing=missing,
        )  # fmt: skip
    except gates.GateError as error:
        raise RecomputeError(f"the gates cannot be run over the shipped paths: {error}") from None
    return json.loads(json.dumps(recorded(result, paths=len(paths), dropped=others.dropped)))


def differences(recorded: Any, recomputed: Any, at: str = "") -> list[str]:
    """Every place `recorded` and `recomputed` disagree, as `<field>: manifest <x>, recomputed
    <y>`. Numbers may differ by `REL_TOL` or `ABS_TOL`, see the module docstring; a boolean is
    never a number here, and anything else must be equal. A dict must have the same keys and a
    list the same length, so a field the manifest adds or drops is a difference too."""
    where = at or "manifest"
    if isinstance(recorded, dict) and isinstance(recomputed, dict):
        prefix = f"{at}." if at else ""
        found = [
            f"{prefix}{key}: manifest {'absent' if key not in recorded else 'has it'},"
            f" recomputed {'absent' if key not in recomputed else 'has it'}"
            for key in sorted(set(recorded) ^ set(recomputed), key=str)
        ]
        for key in sorted(set(recorded) & set(recomputed), key=str):
            found += differences(recorded[key], recomputed[key], f"{prefix}{key}")
        return found
    if isinstance(recorded, list) and isinstance(recomputed, list):
        if len(recorded) != len(recomputed):
            return [f"{where}: manifest {len(recorded)} entries, recomputed {len(recomputed)}"]
        found = []
        for index, (left, right) in enumerate(zip(recorded, recomputed, strict=True)):
            found += differences(left, right, f"{where}[{index}]")
        return found
    if _number(recorded) and _number(recomputed):
        if _close(float(recorded), float(recomputed)):
            return []
    elif type(recorded) is type(recomputed) and recorded == recomputed:
        return []
    return [f"{where}: manifest {recorded!r}, recomputed {recomputed!r}"]


def _number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _close(left: float, right: float) -> bool:
    if math.isnan(left) or math.isnan(right):
        return math.isnan(left) and math.isnan(right)
    return math.isclose(left, right, rel_tol=REL_TOL, abs_tol=ABS_TOL)
