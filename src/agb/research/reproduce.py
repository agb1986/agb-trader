"""Reproducing a trial from its ledger row.

A row names everything its run depended on: the kind of run, its whole configuration, and
the lake partition digest of the panel it learned from. Given a panel with the same digest,
the run is done again, as a new trial, and its results compared with the row's. A panel
built from a lake that has moved is refused rather than compared."""

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from agb import lake
from agb.research import cpcv, scan, train, walkforward
from agb.research.panel import Panel
from agb.research.trials import Ledger, Run


class ReproduceError(Exception):
    pass


@dataclass(frozen=True)
class Reproduction:
    original: dict[str, Any]
    rerun: Run
    matches: bool


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
    config = json.loads(row["config"])
    kind = row["kind"]
    if kind == cpcv.KIND:
        model = train.Config.from_dict(config["model"])
        return cpcv.run(panel, model, cpcv.Options(**config[cpcv.KIND]), ledger)
    if kind == walkforward.KIND:
        model = train.Config.from_dict(config["model"])
        options = walkforward.Options(**config[walkforward.KIND])
        return walkforward.run(panel, model, options, ledger)
    if kind == scan.KIND:
        options = scan.Options(**config[scan.KIND])
        return scan.run(panel, options, ledger, lake_dir=lake_dir, db=db)
    raise ReproduceError(f"no way to rerun a {kind!r} trial")


def reproduce(
    ledger: Ledger,
    trial_id: str,
    panel: Panel,
    *,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
) -> Reproduction:
    original = ledger.get(trial_id)
    if original["status"] != "succeeded":
        raise ReproduceError(f"trial {trial_id} {original['status']}; it has no results to match")
    repeat = rerun(original, panel, ledger, lake_dir=lake_dir, db=db)
    matches = same(json.loads(original["results"]), repeat.results)
    return Reproduction(original=original, rerun=repeat, matches=matches)


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
