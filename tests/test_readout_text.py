"""The readout's plain-text helpers: the money and percent formats, the message layout, the
month bounds, and the way a measurement that did not parse is shown rather than guessed at.

These need no database. The queries behind `/status`, `/gate`, `/why` and the rest are
covered through the bot's commands."""

from datetime import datetime
from decimal import Decimal

import pytest

from agb import readout
from agb.nyse import NEW_YORK
from agb.readout import CheckRow, Item


@pytest.mark.parametrize(
    "value, text",
    [
        (Decimal("0"), "$0.00"),
        (Decimal("1234567.891"), "$1,234,567.89"),
        (Decimal("-0.5"), "-$0.50"),
        (Decimal("-1000"), "-$1,000.00"),
    ],
)
def test_dollars_are_grouped_and_signed_before_the_symbol(value, text):
    assert readout.dollars(value) == text


@pytest.mark.parametrize(
    "qty, text",
    [
        (Decimal("10"), "10"),
        (Decimal("10.500"), "10.5"),
        (Decimal("1E+2"), "100"),
        (Decimal("0.0000100"), "0.00001"),
    ],
)
def test_share_counts_drop_trailing_zeros_and_never_use_an_exponent(qty, text):
    assert readout.shares(qty) == text


def test_percentages_are_read_off_a_fraction():
    assert readout.percent(Decimal("0.1234")) == "12.34%"
    assert readout.percent(Decimal("0.1234"), 1) == "12.3%"
    assert readout.signed_percent(Decimal("0.05")) == "+5.00%"
    assert readout.signed_percent(Decimal("-0.0123")) == "-1.23%"
    assert readout.signed_percent(Decimal("0")) == "+0.00%"


def test_a_message_is_a_header_two_line_items_and_an_optional_footer():
    items = [Item("AAA", "10 shares"), Item("BBB", "5 shares")]
    assert readout.message("Positions", items) == (
        "Positions\n\n  AAA\n    10 shares\n\n  BBB\n    5 shares"
    )
    assert readout.message("Positions", items, "End").endswith("\n\nEnd")
    assert readout.message("Nothing", []) == "Nothing"


@pytest.mark.parametrize(
    "period, start, end",
    [
        ("2026-10", (2026, 10, 1), (2026, 11, 1)),
        ("2026-12", (2026, 12, 1), (2027, 1, 1)),
        ("2026-01", (2026, 1, 1), (2026, 2, 1)),
    ],
)
def test_a_month_runs_from_its_first_new_york_instant_to_the_next_months(period, start, end):
    first, after = readout.month_bounds(period)
    assert first == datetime(*start, tzinfo=NEW_YORK)
    assert after == datetime(*end, tzinfo=NEW_YORK)


@pytest.mark.parametrize("good", ["2026-01", "2026-09", "2026-12"])
def test_a_period_is_a_year_and_a_month_from_01_to_12(good):
    assert readout.PERIOD.match(good)


@pytest.mark.parametrize("bad", ["2026-00", "2026-13", "2026-1", "26-10", "2026-10-01", "x"])
def test_anything_else_is_not_a_period(bad):
    assert readout.PERIOD.match(bad) is None


def test_a_rank_that_is_missing_says_so():
    assert readout.rank_text(7) == "rank 7"
    assert readout.rank_text(None) == "not ranked"


@pytest.mark.parametrize(
    "measured, text",
    [
        (None, "turnover not measured"),
        ("no book", "turnover none, no book yet"),
        ("12.34%", "turnover 12.3%"),
        ("8", "turnover 8.0%"),
        ("abc", "turnover abc"),
    ],
)
def test_turnover_is_shown_as_measured_or_as_what_it_was(measured, text):
    assert readout.turnover_text(measured) == text


@pytest.mark.parametrize(
    "measured, text",
    [
        ("5000", "cash after 5.0%"),
        (None, "cash after not measured"),
        ("", "cash after not measured"),
        ("n/a", "cash after n/a"),
    ],
)
def test_cash_is_shown_against_the_capital_or_as_what_it_was(measured, text):
    assert readout.cash_text(measured, Decimal(100000)) == text


def test_a_check_that_failed_says_what_was_measured_and_what_it_needed():
    check = CheckRow("gate", "basket", "weight", "AAA", "12%", "10%", False)
    assert readout.failed(check) == "  FAIL  weight AAA\n          measured 12%, needs 10%"


@pytest.mark.parametrize(
    "value, earlier, later",
    [
        ("$1,000", "$999", "$1,000"),
        ("2.5%", "1.5%", "10%"),
        ("n/a", "5", "n/a"),
    ],
)
def test_measurements_sort_by_their_number_and_the_unreadable_last(value, earlier, later):
    assert readout._sort_key(earlier) < readout._sort_key(later)
    assert readout._sort_key(value) is not None


@pytest.mark.parametrize(
    "n, text",
    [
        (1, "1st"),
        (2, "2nd"),
        (3, "3rd"),
        (4, "4th"),
        (11, "11th"),
        (12, "12th"),
        (13, "13th"),
        (21, "21st"),
        (22, "22nd"),
        (101, "101st"),
        (111, "111th"),
    ],
)
def test_ordinals_treat_the_teens_as_th(n, text):
    assert readout._ordinal(n) == text


def test_a_log_detail_is_collapsed_to_one_line_and_cut_with_an_ellipsis():
    assert readout._cut("a  b\n c") == "a b c"
    cut = readout._cut("x" * 500)
    assert len(cut) == readout.DETAIL_MAX
    assert cut.endswith("…")
    assert readout._cut("short") == "short"
