"""The walk-forward test: the model that trades year Y+1 was trained on data through year Y.

One fold per traded year. Its training set is every month whose label had settled by the
last day of year Y, so November and December of Y, whose returns land in Y+1, are out. Its
test set is every month whose basket is bought in Y+1. The window expands: each year's model
sees everything the year before's did and one more year, as the live model will, refit
annually."""

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from agb.research import train
from agb.research.panel import Month, Panel
from agb.research.trials import Ledger, Run

KIND = "walkforward"


@dataclass(frozen=True)
class Options:
    first_test_year: int | None = None
    last_test_year: int | None = None


@dataclass(frozen=True)
class Fold:
    year: int  # the year the test months are traded in
    train: tuple[Month, ...]
    test: tuple[Month, ...]

    @property
    def cutoff(self) -> date:
        """The last day of the year the training data runs through."""
        return date(self.year - 1, 12, 31)


def folds(months: list[Month], options: Options | None = None) -> list[Fold]:
    options = options or Options()
    result = []
    for year in sorted({month.entry.year for month in months}):
        if options.first_test_year is not None and year < options.first_test_year:
            continue
        if options.last_test_year is not None and year > options.last_test_year:
            continue
        cutoff = date(year - 1, 12, 31)
        train_months = tuple(month for month in months if month.exit <= cutoff)
        test_months = tuple(month for month in months if month.entry.year == year)
        if train_months and test_months:
            result.append(Fold(year, train_months, test_months))
    return result


def run(panel: Panel, config: train.Config, options: Options, ledger: Ledger) -> Run:
    months = panel.months
    fold_list = folds(months, options)
    if not fold_list:
        raise ValueError("the panel does not span two years with settled labels")
    with ledger.trial(
        KIND,
        {"model": config.to_dict(), KIND: asdict(options)},
        features=config.features,
        train_start=fold_list[0].train[0].as_of,
        train_end=fold_list[-1].train[-1].as_of,
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
    ) as trial:
        years: list[dict[str, Any]] = []
        frames, scores = [], []
        for fold in fold_list:
            booster = train.fit(panel.select(fold.train), config, trial=trial)
            test = panel.select(fold.test)
            predicted = train.predict(booster, test, config)
            frames.append(test)
            scores.append(predicted)
            years.append({
                "year": fold.year,
                "train_through": fold.cutoff.isoformat(),
                "train_months": len(fold.train),
                "test_months": len(fold.test),
                "metrics": train.evaluate(test, predicted),
            })  # fmt: skip
        overall = train.evaluate(pd.concat(frames, ignore_index=True), np.concatenate(scores))
        trial.results = {"years": years, "overall": overall}
        return Run(trial.trial_id, trial.results)
