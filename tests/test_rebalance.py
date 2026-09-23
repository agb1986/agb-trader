"""The rebalance rules, table-driven: the 20/30 buffer, the tie-breaks, an empty book, fewer
than twenty names, equal weights and the order plan. And that the module needs no service."""

import subprocess
import sys
from decimal import Decimal

import pytest

from agb import rebalance
from agb.rebalance import Order, Selection, Side

RANKING = [f"N{i:02d}" for i in range(1, 41)]


def names(*ranks):
    return tuple(f"N{i:02d}" for i in ranks)


CASES = {
    "empty book buys the top twenty": (
        RANKING, (), (), Selection((), names(*range(1, 21)), (), ()),
    ),
    "an incumbent ranked 25 is held through the buffer": (
        RANKING, names(*range(1, 20), 25), (),
        Selection(names(*range(1, 20), 25), (), (), ()),
    ),
    "an incumbent ranked 31 is sold and the top name outside the book bought": (
        RANKING, names(*range(1, 20), 31), (),
        Selection(names(*range(1, 20)), names(20), names(31), ()),
    ),
    "an incumbent that fell out of the ranking is sold": (
        RANKING, (*names(*range(1, 20)), "GONE"), (),
        Selection(names(*range(1, 20)), names(20), ("GONE",), ()),
    ),
    "too many incumbents in the buffer: the lowest-ranked is dropped": (
        RANKING, names(*range(1, 19), 25, 28, 30), (),
        Selection(names(*range(1, 19), 25, 28), (), names(30), ()),
    ),
    "slots fill from the top of the ranking": (
        RANKING, names(5, 30), (),
        Selection(names(5, 30), names(1, 2, 3, 4, *range(6, 20)), (), ()),
    ),
    "a halted incumbent is carried, not replaced: nineteen names trade": (
        RANKING, names(*range(1, 20), 35), names(35),
        Selection(names(*range(1, 20)), (), (), names(35)),
    ),
    "a halted name outside the book is passed over": (
        RANKING, (), names(3),
        Selection((), names(1, 2, *range(4, 22)), (), ()),
    ),
    "a ranking short of twenty tradeable names leaves slots empty": (
        RANKING[:15], (), (), Selection((), names(*range(1, 16)), (), ()),
    ),
    "sells are listed in rank order, unranked last": (
        RANKING, (*names(*range(1, 19), 33, 31), "ZZZ", "AAA"), (),
        Selection(names(*range(1, 19)), names(19, 20), (*names(31, 33), "AAA", "ZZZ"), ()),
    ),
}  # fmt: skip


@pytest.mark.parametrize(("ranking", "held", "untradeable", "expected"), CASES.values(), ids=CASES)
def test_select(ranking, held, untradeable, expected):
    assert rebalance.select(ranking, held, untradeable=untradeable) == expected


def test_a_selection_names_every_position_after_the_rebalance():
    selection = rebalance.select(RANKING, names(*range(1, 20), 35), untradeable=names(35))
    assert selection.names == names(*range(1, 20), 35)
    assert len(selection.names) == 20
    assert selection.held == frozenset(names(*range(1, 20), 35))


def test_the_size_cannot_exceed_the_hold_through_rank():
    with pytest.raises(ValueError, match="exceeds"):
        rebalance.select(RANKING, (), size=31)


def test_a_ranking_that_repeats_a_name_is_refused():
    with pytest.raises(ValueError, match="twice"):
        rebalance.select(["A", "B", "A"], ())


@pytest.mark.parametrize("count", [20, 19, 1])
def test_weights_are_a_twentieth_each_whatever_the_count(count):
    weight = rebalance.weights(RANKING[:count])
    assert set(weight.values()) == {Decimal("0.05")}
    assert sum(weight.values()) == Decimal("0.05") * count


def test_more_than_twenty_names_cannot_be_weighted():
    with pytest.raises(ValueError, match="21 names"):
        rebalance.weights(RANKING[:21])


def test_orders_buy_whole_shares_of_each_weight_and_sell_whole_positions():
    selection = Selection(keep=("K",), buy=("B1", "B2"), sell=("S",), carry=("C",))
    prices = {"K": Decimal("10"), "B1": Decimal("33.3"), "B2": Decimal("7"), "S": Decimal("50")}
    positions = {"K": 500, "S": Decimal("120"), "C": 10}

    plan = rebalance.orders(
        selection, rebalance.weights(selection.names), prices, positions, Decimal("100000")
    )

    assert plan == [
        Order("S", Side.SELL, 120, Decimal("50")),
        Order("B1", Side.BUY, 150, Decimal("33.3")),  # floor(5000 / 33.3)
        Order("B2", Side.BUY, 714, Decimal("7")),  # floor(5000 / 7)
    ]
    assert plan[1].notional == Decimal("4995.0")


def test_a_weight_that_buys_no_whole_share_is_an_error():
    selection = Selection((), ("X",), (), ())
    with pytest.raises(ValueError, match="X: 50.00 buys no whole share"):
        rebalance.orders(
            selection, rebalance.weights(["X"]), {"X": Decimal("60")}, {}, Decimal(1000)
        )


def test_a_name_without_a_price_cannot_be_ordered():
    selection = Selection((), ("X",), (), ())
    with pytest.raises(ValueError, match="X: no price"):
        rebalance.orders(selection, rebalance.weights(["X"]), {}, {}, Decimal(1000))


IMPORTS = """
import sys
before = set(sys.modules)
import agb.rebalance
print("\\n".join(set(sys.modules) - before))
"""


def test_the_rules_import_nothing_beyond_the_standard_library():
    loaded = subprocess.run(
        [sys.executable, "-c", IMPORTS], check=True, capture_output=True, text=True
    ).stdout.split()
    outside_stdlib = {
        name
        for name in loaded
        if name.split(".")[0] not in sys.stdlib_module_names and not name.startswith("_")
    }
    assert outside_stdlib == {"agb", "agb.rebalance"}
