"""LightGBM LambdaRank over the panel.

One query group per month: the model learns to order the names within a month by grade and
is scored on how well its order agrees with the realised one. `deterministic` and a fixed
seed make a refit over the same rows and the same parameters reproduce the same model, which
is what lets a trial be reproduced from its row.

Nothing trains without an open trial: `fit` takes one and counts the refit against it."""

import math
from dataclasses import asdict, dataclass, replace
from typing import Any

import numpy as np
import pandas as pd

from agb import features
from agb.research import labels
from agb.research.trials import Trial


@dataclass(frozen=True)
class Config:
    """A model configuration. Everything LightGBM sees, so two rows with equal configurations
    describe the same model."""

    features: tuple[str, ...] = features.FEATURES
    num_boost_round: int = 300
    learning_rate: float = 0.03
    num_leaves: int = 31
    min_data_in_leaf: int = 100
    feature_fraction: float = 0.8
    bagging_fraction: float = 0.8
    bagging_freq: int = 1
    lambda_l2: float = 1.0
    seed: int = 0
    num_threads: int = 0  # 0 lets LightGBM use every core; results do not depend on it

    def params(self) -> dict[str, Any]:
        return {
            "objective": "lambdarank",
            "metric": "ndcg",
            "label_gain": list(range(labels.GRADES)),
            "learning_rate": self.learning_rate,
            "num_leaves": self.num_leaves,
            "min_data_in_leaf": self.min_data_in_leaf,
            "feature_fraction": self.feature_fraction,
            "bagging_fraction": self.bagging_fraction,
            "bagging_freq": self.bagging_freq,
            "lambda_l2": self.lambda_l2,
            "seed": self.seed,
            "num_threads": self.num_threads,
            "deterministic": True,
            "force_row_wise": True,
            "verbose": -1,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**asdict(self), "features": list(self.features)}

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "Config":
        return cls(**{**values, "features": tuple(values.get("features", features.FEATURES))})

    def with_features(self, names: tuple[str, ...]) -> "Config":
        unknown = set(names) - set(features.FEATURES)
        if unknown:
            raise ValueError(f"not features: {sorted(unknown)}")
        return replace(self, features=tuple(names))

    @property
    def categorical(self) -> list[str]:
        return [name for name in self.features if name in features.CATEGORICAL]


def matrix(frame: pd.DataFrame, config: Config) -> pd.DataFrame:
    """The feature columns LightGBM reads, in the configuration's order."""
    return frame[list(config.features)]


def groups(frame: pd.DataFrame) -> list[int]:
    """The query group sizes: how many names each month has, in the frame's order. The frame
    must be sorted by month, as `Panel.select` returns it."""
    months = frame["as_of"].to_numpy()
    if len(months) == 0:
        return []
    if (months[1:] < months[:-1]).any():
        raise ValueError("training rows must be sorted by month")
    _, sizes = np.unique(months, return_counts=True)
    return [int(size) for size in sizes]


def fit(frame: pd.DataFrame, config: Config, *, trial: Trial):
    """Train one model over `frame`. The trial is not optional: every refit is counted."""
    import lightgbm as lgb

    if not isinstance(trial, Trial):
        raise TypeError("fit needs the open trial the refit belongs to")
    if frame.empty:
        raise ValueError("nothing to train on")
    trial.refit()
    data = lgb.Dataset(
        matrix(frame, config),
        label=frame["grade"].to_numpy(),
        group=groups(frame),
        categorical_feature=config.categorical or "auto",
        free_raw_data=False,
    )
    return lgb.train(config.params(), data, num_boost_round=config.num_boost_round)


def predict(booster, frame: pd.DataFrame, config: Config) -> np.ndarray:
    if frame.empty:
        return np.empty(0)
    return np.asarray(booster.predict(matrix(frame, config), num_threads=config.num_threads))


# Scoring ----------------------------------------------------------------------------------


def evaluate(frame: pd.DataFrame, scores: np.ndarray) -> dict[str, float | int | None]:
    """How a score ordered the names within each month, averaged across months: the mean
    Spearman information coefficient with forward return and its t-statistic, NDCG over the
    grades, and the spread between the top and bottom fifth's mean forward returns."""
    scored = frame[["as_of", "forward_return", "grade"]].copy()
    scored["score"] = np.asarray(scores, dtype="float64")
    ics, ndcgs, spreads = [], [], []
    for _, month in scored.groupby("as_of", sort=True):
        if len(month) < 2:
            continue
        ic = spearman(month["score"].to_numpy(), month["forward_return"].to_numpy())
        if ic is not None:
            ics.append(ic)
        ndcgs.append(ndcg(month["score"].to_numpy(), month["grade"].to_numpy()))
        spreads.append(quintile_spread(month["score"].to_numpy(), month["forward_return"]))
    return {
        "months": len(ndcgs),
        "ic_mean": _mean(ics),
        "ic_t": _t_stat(ics),
        "ndcg_mean": _mean(ndcgs),
        "spread_mean": _mean(spreads),
    }


def spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    rx = np.asarray(features.percentile_rank(list(x)), dtype="float64")
    ry = np.asarray(features.percentile_rank(list(y)), dtype="float64")
    if np.std(rx) == 0 or np.std(ry) == 0:
        return None
    return float(np.corrcoef(rx, ry)[0, 1])


def ndcg(scores: np.ndarray, grades: np.ndarray) -> float:
    """Normalised discounted cumulative gain of the score's order over the whole month, with
    the grade as the gain, as LightGBM's `ndcg` metric with `label_gain` 0..4 computes it."""
    discounts = 1 / np.log2(np.arange(2, len(scores) + 2))
    by_score = grades[np.argsort(-scores, kind="stable")]
    ideal = np.sort(grades)[::-1]
    best = float((ideal * discounts).sum())
    return 1.0 if best == 0 else float((by_score * discounts).sum()) / best


def quintile_spread(scores: np.ndarray, returns: pd.Series) -> float:
    """The mean return of the top fifth by score less the bottom fifth's."""
    order = np.argsort(-scores, kind="stable")
    n = max(1, len(scores) // labels.GRADES)
    values = returns.to_numpy()
    return float(values[order[:n]].mean() - values[order[-n:]].mean())


def _mean(values: list[float]) -> float | None:
    return None if not values else float(np.mean(values))


def _t_stat(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    sd = float(np.std(values, ddof=1))
    return None if sd == 0 else float(np.mean(values)) / (sd / math.sqrt(len(values)))
