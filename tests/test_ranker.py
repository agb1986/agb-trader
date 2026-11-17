"""agb.ranker.order: the deterministic ranking every ranker hands the rebalance, and the NaN
score it refuses."""

import math

import pytest

from agb.ranker import Ranked, order


def test_names_rank_by_score_ties_by_symbol_and_unscored_last():
    scores = {"A": 0.5, "B": 0.9, "C": 0.5, "D": None}
    assert order(["D", "C", "B", "A", "E"], scores) == [
        Ranked("B", 1, 0.9),
        Ranked("A", 2, 0.5),
        Ranked("C", 3, 0.5),
        Ranked("D", 4, None),
        Ranked("E", 5, None),
    ]


def test_a_nan_score_raises_instead_of_ranking():
    with pytest.raises(ValueError, match=r"scored \['B', 'D'\] NaN"):
        order(["A", "B", "C", "D"], {"A": 0.1, "B": math.nan, "C": 0.3, "D": float("nan")})
