"""The UK after-tax diagnostic's arithmetic: Section 104 pooling per name, the 6 April year
boundary, a year's losses netting against its gains, a surplus loss carried forward against a
later year's gains above the allowance, and the rate on what survives.

The fixture below is eight trades over three tax years, chosen so every figure comes out to the
cent, and the test asserts the hand calculation written out beside it. Everything here is
`Decimal`: a tax bill is money."""

from datetime import date
from decimal import Decimal

import pytest

from agb import cost
from agb.research import tax


def buy(day: date, ticker: str, qty: int, price: str, fee: str) -> tax.Trade:
    return tax.Trade(day, ticker, cost.Side.BUY, qty, Decimal(price), Decimal(fee))


def sell(day: date, ticker: str, qty: int, price: str, fee: str) -> tax.Trade:
    return tax.Trade(day, ticker, cost.Side.SELL, qty, Decimal(price), Decimal(fee))


# Two names bought and sold inside 2024/25, one of them reopened and sold at a loss in 2025/26,
# and a third bought and sold at a gain in 2026/27 with the loss still carried forward.
HAND = [
    buy(date(2024, 5, 1), "AAA", 1000, "10.00", "20.00"),
    buy(date(2024, 5, 1), "BBB", 500, "40.00", "25.00"),
    buy(date(2024, 6, 3), "AAA", 1000, "14.00", "30.00"),
    sell(date(2024, 9, 2), "AAA", 1000, "20.00", "40.00"),
    sell(date(2024, 10, 1), "BBB", 500, "36.00", "20.00"),
    sell(date(2025, 6, 2), "AAA", 1000, "9.00", "15.00"),
    buy(date(2026, 5, 4), "CCC", 200, "50.00", "10.00"),
    sell(date(2026, 9, 1), "CCC", 200, "95.00", "30.00"),
]


def test_the_hand_calculation_of_three_tax_years_on_eight_trades():
    """The whole computation on the fixture, by hand.

    AAA's pool after the two buys holds 2,000 shares at 1,000 x 10.00 + 20.00 + 1,000 x 14.00
    + 30.00 = 24,050.00, so the first disposal of half of it takes 12,025.00 against proceeds
    of 1,000 x 20.00 - 40.00 = 19,960.00: a gain of 7,935.00. BBB's 500 shares cost
    20,025.00 and raise 500 x 36.00 - 20.00 = 17,980.00: a loss of 2,045.00. Both fall in
    2024/25, where they net to 5,890.00; the allowance takes 3,000.00 off that and 24% of the
    remaining 2,890.00 is 693.60.

    AAA's remaining 1,000 shares carry the 12,025.00 left in the pool and raise
    1,000 x 9.00 - 15.00 = 8,985.00 in 2025/26: a loss of 3,040.00, no tax, and the loss is
    carried forward whole.

    CCC costs 200 x 50.00 + 10.00 = 10,010.00 and raises 200 x 95.00 - 30.00 = 18,970.00 in
    2026/27: a gain of 8,960.00. Only the 5,960.00 above the allowance can take the carried
    loss, which is less than that, so all 3,040.00 of it is relieved; the allowance takes
    3,000.00 off the remaining 5,920.00 and 24% of 2,920.00 is 700.80.

    The bill on the run is 693.60 + 700.80 = 1,394.40."""
    assessment = tax.assess(HAND)

    assert [
        (d.ticker, d.qty, d.proceeds, d.allowable_cost, d.gain) for d in assessment.disposals
    ] == [
        ("AAA", 1000, Decimal("19960.00"), Decimal("12025.00"), Decimal("7935.00")),
        ("BBB", 500, Decimal("17980.00"), Decimal("20025.00"), Decimal("-2045.00")),
        ("AAA", 1000, Decimal("8985.00"), Decimal("12025.00"), Decimal("-3040.00")),
        ("CCC", 200, Decimal("18970.00"), Decimal("10010.00"), Decimal("8960.00")),
    ]
    # The gains and losses each year realises, and what they net to.
    assert [(y.year, y.gains, y.losses, y.net) for y in assessment.years] == [
        ("2024/25", Decimal("7935.00"), Decimal("2045.00"), Decimal("5890.00")),
        ("2025/26", Decimal(0), Decimal("3040.00"), Decimal("-3040.00")),
        ("2026/27", Decimal("8960.00"), Decimal(0), Decimal("8960.00")),
    ]
    # What the brought-forward loss and the allowance take off that net, and the rate's share.
    assert [(y.relieved, y.exempt, y.taxable, y.tax, y.carried) for y in assessment.years] == [
        (Decimal(0), Decimal("3000"), Decimal("2890.00"), Decimal("693.60"), Decimal(0)),
        (Decimal(0), Decimal(0), Decimal(0), Decimal("0.00"), Decimal("3040.00")),
        (Decimal("3040.00"), Decimal("3000"), Decimal("2920.00"), Decimal("700.80"), Decimal(0)),
    ]
    assert assessment.tax == Decimal("1394.40")
    assert [y.disposals for y in assessment.years] == [2, 1, 1]


def test_the_tax_year_turns_on_the_sixth_of_april():
    """The UK tax year runs 6 April to 5 April, so two disposals a session apart can fall in
    different years and neither year's allowance helps the other."""
    assert tax.year_of(date(2025, 4, 5)) == "2024/25"
    assert tax.year_of(date(2025, 4, 6)) == "2025/26"
    assert tax.year_of(date(2024, 12, 31)) == "2024/25"
    assert tax.year_of(date(2099, 4, 6)) == "2099/00", "the label is the year's last two digits"

    trades = [
        buy(date(2024, 5, 1), "AAA", 100, "10.00", "0.00"),
        buy(date(2024, 5, 1), "BBB", 100, "10.00", "0.00"),
        sell(date(2025, 4, 5), "AAA", 100, "35.00", "0.00"),
        sell(date(2025, 4, 6), "BBB", 100, "35.00", "0.00"),
    ]
    years = tax.assess(trades).years

    assert [y.year for y in years] == ["2024/25", "2025/26"]
    assert [y.net for y in years] == [Decimal("2500.00"), Decimal("2500.00")]
    # Each 2,500.00 gain is sheltered by its own year's allowance and nothing is due. Both in
    # one year would have netted 5,000.00 against one allowance, leaving 2,000.00 taxable.
    assert [y.taxable for y in years] == [Decimal(0), Decimal(0)]
    assert tax.assess(trades).tax == Decimal(0)
    together = [*trades[:2], trades[2], sell(date(2025, 4, 5), "BBB", 100, "35.00", "0.00")]
    assert tax.assess(together).tax == Decimal("480.00")


def test_the_pool_is_spent_exactly_however_the_shares_divide():
    """A part disposal takes its share of the pool rounded to the cent, and the disposal that
    empties the pool takes whatever is left, so no rounded residue survives as a phantom gain
    or loss."""
    trades = [
        buy(date(2024, 5, 1), "AAA", 3, "10.00", "0.01"),
        sell(date(2024, 6, 3), "AAA", 1, "11.00", "0.00"),
        sell(date(2024, 7, 1), "AAA", 2, "11.00", "0.00"),
    ]

    disposals = tax.assess(trades).disposals

    assert [d.allowable_cost for d in disposals] == [Decimal("10.00"), Decimal("20.01")]
    assert sum(d.allowable_cost for d in disposals) == Decimal("30.01"), "the whole cost, once"


def test_a_disposal_larger_than_the_holding_is_a_caller_error():
    """The diagnostic taxes a long-only run's fills; a sell it cannot price out of a pool means
    the trades it was handed are wrong, and a silent zero cost would read as a large gain."""
    with pytest.raises(tax.TaxError, match="sells 2 shares on 2024-06-03 from a pool of 1"):
        tax.assess([
            buy(date(2024, 5, 1), "AAA", 1, "10.00", "0.00"),
            sell(date(2024, 6, 3), "AAA", 2, "11.00", "0.00"),
        ])  # fmt: skip


def test_the_view_states_the_bill_against_the_run_as_floats():
    """What the report carries: the bill, the pre-tax and after-tax return on the starting
    cash, and a row per tax year, all as floats because a report row is serialised as JSON."""
    view = tax.view(HAND, Decimal("100000"), Decimal("110000"))

    assert view["tax"] == 1394.40
    assert view["disposals"] == 4
    assert view["realised_gains"] == 16895.0 and view["realised_losses"] == 5085.0
    assert view["taxable_gains"] == 5810.0
    assert view["loss_carried_forward"] == 0.0
    assert view["total_return"] == 0.1
    assert view["final_value"] == 110000.0
    assert view["after_tax_final_value"] == 108605.60
    assert view["after_tax_return"] == pytest.approx(0.0860560)
    assert [year["year"] for year in view["years"]] == ["2024/25", "2025/26", "2026/27"]
    assert [year["tax"] for year in view["years"]] == [693.60, 0.0, 700.80]
    assert all(
        isinstance(value, float) for year in view["years"] for value in list(year.values())[2:]
    )
    idle = tax.view([], Decimal("100000"), Decimal("100000"))
    assert (idle["tax"], idle["years"], idle["loss_carried_forward"]) == (0.0, [], 0.0)
    assert idle["after_tax_return"] == 0.0, "a run that disposed of nothing owes nothing"


def test_the_simplifications_are_stated_and_the_constants_are_the_specifications():
    """The report labels the view approximate and carries this list with the numbers, so the
    figure cannot be read as a computation. The rate and the allowance are the specification's:
    24% after GBP 3,000."""
    assert tax.RATE == Decimal("0.24")
    assert tax.EXEMPT_AMOUNT == Decimal("3000")
    assert len(tax.SIMPLIFICATIONS) >= 5
    text = " ".join(tax.SIMPLIFICATIONS)
    for stated in ("USD", "Section 104", "24%", "3,000", "open at the end", "Dividends"):
        assert stated.lower() in text.lower(), stated
