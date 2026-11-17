"""The ranking order: score descending, ties by symbol, unscored names last, and the NaN score
it refuses."""

import dataclasses
import math

import pytest

from agb.ranker import Ranked, order


def test_names_are_ranked_from_one_by_score_descending():
    ranked = order(["AAA", "BBB", "CCC"], {"AAA": 0.1, "BBB": 0.9, "CCC": 0.5})
    assert [(r.symbol, r.rank, r.score) for r in ranked] == [
        ("BBB", 1, 0.9),
        ("CCC", 2, 0.5),
        ("AAA", 3, 0.1),
    ]


def test_ties_break_by_symbol():
    ranked = order(["ZZZ", "AAA", "MMM"], {"ZZZ": 1.0, "AAA": 1.0, "MMM": 1.0})
    assert [r.symbol for r in ranked] == ["AAA", "MMM", "ZZZ"]


def test_negative_scores_rank_below_positive_and_above_unscored():
    ranked = order(["A", "B", "C", "D"], {"A": -2.0, "B": 0.0, "C": None})
    assert [r.symbol for r in ranked] == ["B", "A", "C", "D"]
    assert [r.score for r in ranked] == [0.0, -2.0, None, None]


def test_unscored_names_come_last_in_symbol_order():
    ranked = order(["Z", "A", "B"], {"B": 0.3, "Z": None})
    assert [(r.symbol, r.rank) for r in ranked] == [("B", 1), ("A", 2), ("Z", 3)]


def test_a_symbol_listed_twice_is_ranked_once():
    ranked = order(["AAA", "AAA", "BBB"], {"AAA": 1.0, "BBB": 2.0})
    assert [r.symbol for r in ranked] == ["BBB", "AAA"]
    assert [r.rank for r in ranked] == [1, 2]


def test_scores_for_names_that_were_not_asked_about_are_ignored():
    ranked = order(["AAA"], {"AAA": 1.0, "ZZZ": 5.0})
    assert [r.symbol for r in ranked] == ["AAA"]


def test_an_empty_universe_has_an_empty_ranking():
    assert order([], {"AAA": 1.0}) == []


def test_the_order_does_not_depend_on_the_input_order():
    scores = {"A": 0.2, "B": 0.2, "C": 0.7, "D": None}
    assert order(["D", "C", "B", "A"], scores) == order(iter(["A", "B", "C", "D"]), scores)


def test_a_ranked_name_cannot_be_changed():
    ranked = Ranked("AAA", 1, 0.5)
    with pytest.raises(dataclasses.FrozenInstanceError):
        ranked.rank = 2  # type: ignore[misc]


def test_a_nan_score_raises_instead_of_ranking():
    with pytest.raises(ValueError, match=r"scored \['B', 'D'\] NaN"):
        order(["A", "B", "C", "D"], {"A": 0.1, "B": math.nan, "C": 0.3, "D": float("nan")})
