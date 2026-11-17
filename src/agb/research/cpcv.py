"""Combinatorial purged cross-validation with an embargo.

The panel's months are cut into `groups` contiguous groups. Every choice of `test_groups` of
them is a split: the model is trained on the rest and scored on the chosen groups, so at the
defaults, sixteen groups and two held out, there are 120 splits, and the same number of
refits. Each group is held out in C(N-1, k-1) splits, and taking its p-th one for every
group stitches together the p-th path: a full out-of-sample history of the panel, scored
month by month, of which there are fifteen at the defaults. The spread of a metric across
the paths is its sampling distribution, not one lucky draw.

Purging keeps the training set clean of the test months. A training month whose label window
overlaps a test month's label window is dropped: its return is partly the test's. The
embargo then drops the training months whose features were computed inside a test month's
window, or in the `embargo_months` after it, whose features are the test's returns in
another guise. It is one-sided: nothing before a test month is embargoed, because a
training month that ends before the test starts shares none of its returns, and one whose
label window runs into it is already purged. What remains is proven disjoint by the tests."""

import itertools
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from agb.research import train
from agb.research.cache import Cache, Key, cpcv_key, scores_from, scores_table, standing
from agb.research.panel import Month, Panel
from agb.research.trials import Ledger, Run

KIND = "cpcv"


@dataclass(frozen=True)
class Options:
    groups: int = 16
    test_groups: int = 2
    embargo_months: int = 1

    def __post_init__(self) -> None:
        if not 0 < self.test_groups < self.groups:
            raise ValueError("test_groups must be between 1 and groups - 1")
        if self.embargo_months < 0:
            raise ValueError("embargo_months cannot be negative")


@dataclass(frozen=True)
class Split:
    index: int
    test_groups: tuple[int, ...]
    train: tuple[Month, ...]
    test: tuple[Month, ...]


def add_months(day: date, months: int) -> date:
    """`day` moved `months` calendar months on, clipped to the target month's length."""
    total = day.year * 12 + day.month - 1 + months
    year, month = divmod(total, 12)
    month += 1
    last = (date(year + month // 12, month % 12 + 1, 1) - date(year, month, 1)).days
    return date(year, month, min(day.day, last))


def overlaps(train_month: Month, test_month: Month) -> bool:
    """Whether the two months' label windows share any session's return."""
    return train_month.entry < test_month.exit and train_month.exit > test_month.entry


def embargoed(train_month: Month, test_month: Month, embargo_months: int) -> bool:
    """Whether the training month's features were computed inside the test month's window or
    within the embargo after it."""
    fence = add_months(test_month.exit, embargo_months)
    return test_month.as_of < train_month.as_of < fence


def excluded(train_month: Month, tests: tuple[Month, ...], embargo_months: int) -> bool:
    return any(
        overlaps(train_month, test_month) or embargoed(train_month, test_month, embargo_months)
        for test_month in tests
    )


def group_months(months: list[Month], groups: int) -> list[tuple[Month, ...]]:
    """`months` cut into `groups` contiguous runs whose lengths differ by at most one."""
    if groups > len(months):
        raise ValueError(f"{len(months)} months cannot make {groups} groups")
    bounds = np.linspace(0, len(months), groups + 1).round().astype(int)
    return [tuple(months[bounds[i] : bounds[i + 1]]) for i in range(groups)]


def splits(months: list[Month], options: Options) -> list[Split]:
    """Every split, in the order `itertools.combinations` yields the held-out groups."""
    grouped = group_months(months, options.groups)
    result = []
    held_out = itertools.combinations(range(options.groups), options.test_groups)
    for index, held in enumerate(held_out):
        test = tuple(month for g in held for month in grouped[g])
        train_months = tuple(
            month
            for g in range(options.groups)
            if g not in held
            for month in grouped[g]
            if not excluded(month, test, options.embargo_months)
        )
        result.append(Split(index, held, train_months, test))
    return result


def paths(split_list: list[Split], options: Options) -> list[tuple[tuple[int, int], ...]]:
    """The out-of-sample paths: for each, the (group, split index) pair that scores every
    group. Path p takes each group's p-th split among those holding it out."""
    holders = {
        g: [s.index for s in split_list if g in s.test_groups] for g in range(options.groups)
    }
    count = min(len(indices) for indices in holders.values())
    return [tuple((g, holders[g][p]) for g in range(options.groups)) for p in range(count)]


def run(
    panel: Panel,
    config: train.Config,
    options: Options,
    ledger: Ledger,
    *,
    scores_out: dict[int, pd.Series] | None = None,
    cache: Cache | None = None,
) -> Run:
    """Every split's refit and score, and the per-path results, recorded as one trial.
    `scores_out`, when given, is filled with each path's out-of-sample scores as a series
    aligned to `panel.frame`, for the certification backtest to trade.

    `cache`, when given, is consulted for a cross-validation of this configuration over this
    panel and filled with this one, scores included whether or not the caller asked for them,
    so a `cpcv` run the operator starts by hand leaves an entry the certification of the same
    configuration reuses rather than refitting. A hit does no refit, so it writes no ledger row
    and hands back the row the run it reuses wrote; see `agb.research.cache`."""
    months = panel.months
    split_list = splits(months, options)
    grouped = group_months(months, options.groups)
    entry = (
        None
        if cache is None
        else cpcv_key(cache, panel, model=config.to_dict(), options=asdict(options))
    )
    if entry is not None:
        held = _held(entry, panel, ledger, scores_out)
        if held is not None:
            return held
    # The entry holds every path's scores, so a run that fills the cache collects them even
    # when its caller has no use for them.
    collected = {} if scores_out is None and entry is not None else scores_out
    with ledger.trial(
        KIND,
        {"model": config.to_dict(), KIND: asdict(options)},
        features=config.features,
        train_start=months[0].as_of,
        train_end=months[-1].as_of,
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
    ) as trial:
        scored: dict[tuple[int, int], tuple[Any, np.ndarray]] = {}
        per_split = []
        for split in split_list:
            booster = train.fit(panel.select(split.train), config, trial=trial)
            test = panel.select(split.test)
            scores = train.predict(booster, test, config)
            for g in split.test_groups:
                mask = test["as_of"].isin({m.as_of for m in grouped[g]}).to_numpy()
                scored[(g, split.index)] = (test[mask], scores[mask])
            per_split.append({
                "index": split.index,
                "test_groups": list(split.test_groups),
                "train_months": len(split.train),
                "test_months": len(split.test),
                "metrics": train.evaluate(test, scores),
            })  # fmt: skip
        per_path = []
        for p, members in enumerate(paths(split_list, options)):
            frames, scores = zip(*(scored[key] for key in members), strict=True)
            frame = pd.concat(frames, ignore_index=True)
            per_path.append({
                "path": p,
                "splits": [index for _, index in members],
                "metrics": train.evaluate(frame, np.concatenate(scores)),
            })  # fmt: skip
            if collected is not None:
                collected[p] = _aligned(panel.frame, frame, np.concatenate(scores))
        trial.results = {
            "groups": [len(group) for group in grouped],
            "splits": per_split,
            "paths": per_path,
            "summary": summarise([path["metrics"] for path in per_path]),
        }
        results = trial.results
        trial_id = trial.trial_id
    # Written after the trial's row says `succeeded`, so an entry never names a row that is
    # still open or that failed.
    if entry is not None and collected is not None:
        entry.write(
            {"trial_id": trial_id, "results": results},
            table=scores_table(panel, collected),
        )
    return Run(trial_id, results)


def _held(
    entry: Key, panel: Panel, ledger: Ledger, scores_out: dict[int, pd.Series] | None
) -> Run | None:
    """The cross-validation this key holds, if the cache holds one this run may use.

    An entry is only a hit when its scores read back whole over this panel, even for a caller
    that passes no `scores_out`: every run that fills the cache writes them, so an entry
    without them is damaged, and serving its results alone would leave it in place for the
    certification that does need them."""
    payload = entry.read()
    if payload is None or not standing(ledger, payload.get("trial_id")):
        return None
    table = entry.table()
    if table is None:
        return None
    paths = scores_from(table, panel)
    if paths is None:
        return None
    if scores_out is not None:
        scores_out.update(paths)
    return Run(str(payload["trial_id"]), payload["results"])


def _aligned(target: pd.DataFrame, frame: pd.DataFrame, scores: np.ndarray) -> pd.Series:
    """`scores`, given row by row for `frame`, as a series over `target`'s rows, matched by
    month and ticker; a row of `target` the path never scored is NaN."""
    key = ["as_of", "ticker"]
    scored = pd.Series(scores, index=pd.MultiIndex.from_frame(frame[key]))
    values = scored.reindex(pd.MultiIndex.from_frame(target[key])).to_numpy(dtype="float64")
    return pd.Series(values, index=target.index)


def summarise(metrics: list[dict[str, Any]]) -> dict[str, Any]:
    """Each metric's mean, lowest and highest across the paths."""
    summary: dict[str, Any] = {"paths": len(metrics)}
    for name in ("ic_mean", "ic_t", "ndcg_mean", "spread_mean"):
        values = [m[name] for m in metrics if m.get(name) is not None]
        summary[name] = None if not values else float(np.mean(values))
        summary[f"{name}_min"] = None if not values else float(min(values))
        summary[f"{name}_max"] = None if not values else float(max(values))
    return summary
