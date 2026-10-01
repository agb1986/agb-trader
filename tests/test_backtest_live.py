"""The validation backtest against the real lake. Skipped unless AGB_LIVE_LAKE=1, with
AGB_LAKE_DIR pointing at a lake the operator has already synced or copied down:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=/mnt/data/lake uv run pytest -m live -s tests/test_backtest_live.py

Nothing here talks to the vendor, the broker or the deployment; the lake is only read. It runs
over both windows: the five-year window built from the lake, and the saved full-history panel
under AGB_RESEARCH_DIR, which is skipped when there is none.

This is not a parity test, and the engine and the scan are not expected to agree on the real
lake. The architecture makes vectorbt the fast scan and the engine the validation run, so the
scan deliberately models three frictions fewer than the engine:

    delisting exits  the engine sells a held name at its last listed print, as the forced-exit
                     rule requires; the scan carries the last price forward and sells the name
                     at the next rebalance that drops it out of the ranking
    trims            the engine trims a buy the account cannot fund whole and drops one that
                     rounds to no shares at all; vectorbt's shared cash group does neither
    whole shares     the engine sizes whole shares at the signal session's close, as the live
                     basket sizes them; vectorbt sizes fractional shares at the fill

On the fake market none of the three ever fires, so `tests/test_backtest.py` can assert the two
fill sets are identical. On the real lake they fire tens of times over a few dozen months, so
what this test checks instead is that the divergence is only those frictions: every order one
side has and the other does not is accounted for, the two still trade the same names on the
same sessions in the overwhelming majority, and the scale-free return statistics stay within a
tolerance derived from the frictions rather than borrowed from the fake market."""

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from agb import lake
from agb.research import backtest, scan
from agb.research import panel as panel_module
from agb.research.panel import Panel
from agb.research.trials import FILE, Ledger

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("AGB_LIVE_LAKE") != "1", reason="set AGB_LIVE_LAKE=1 to run"),
]

# The tolerance for the summary statistics, derived from the frictions this module's docstring
# lists rather than imported from the fake-market test, where the two sides agree to floating
# point and the numbers describe nothing but rounding.
#
# Total return is the wrong quantity to bound. The frictions move each month's return a little
# and the panel compounds them, so a bound that holds over five years says nothing over
# twenty-eight. The scale-free statistics are the ones that can carry a fixed bound. Issue #73
# measured both windows, and annualising its total returns gives a gap of 1.7 points over the
# 67-month window (12.4% against 10.7%) and 1.9 points over the 343-month saved panel (5.8%
# against 7.7%): the mechanism behaving as described, since the gap does not grow with the
# panel and its sign is not fixed -- whole-share rounding and an early delisting exit help some
# paths and hurt others. 300 bps a year is a little under twice the larger of the two.
#
# Volatility the issue did not measure; the two sides hold nearly the same names on nearly the
# same sessions, so their monthly returns track closely and 3 points is generous. The drawdown
# is looser again, because the two can take their worst month in different places: the measured
# gap over the 67-month window was 5.3 points.
TOLERANCE = {
    "annual_return": 0.03,
    "annual_volatility": 0.03,
    "sharpe": 0.20,
    "max_drawdown": 0.10,
}
# How much of the order sets may remain unexplained once the delisting exits and the scan's
# matching late sells are taken out, as a fraction of the scan's orders. What is left is
# whole-share sizing: a buy that rounds to no shares is an order the engine never places. Over
# the 67-month window the engine placed 654 orders against the scan's 641 with 21 delisting
# exits, which leaves single digits.
RESIDUAL = 0.03
# How much of the two order sets must still be the same order. Below this the scan has stopped
# describing the strategy the engine runs, whatever the summary statistics say.
AGREEMENT = 0.90
# A buy the account cannot fund at all, as a fraction of the orders placed. The engine counts
# one whenever whole-share sizing leaves nothing to buy, which is the fractional-share friction
# made visible; it is the same mechanism as a trim and must stay as rare.
UNFILLED = 0.02


@dataclass(frozen=True)
class Both:
    """One panel run through both sides: each one's report, and its orders as (session,
    ticker, side) so the two sets can be reconciled."""

    panel: Panel
    engine: dict[str, Any]
    engine_orders: set[tuple[date, str, str]]
    scan: dict[str, Any]
    scan_orders: set[tuple[date, str, str]]


@pytest.fixture(scope="module")
def lake_dir() -> Path:
    root = Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR))
    if not (root / "sep").is_dir():
        pytest.skip(f"no lake at {root}; set AGB_LAKE_DIR")
    return root


@pytest.fixture(scope="module", params=["five-year", "full-history"])
def real_panel(request, lake_dir) -> Panel:
    """The two windows the operator runs: the five-year window built from the lake, which takes
    a while, and T17's saved panel if the research directory holds one."""
    if request.param == "full-history":
        saved = Path(os.environ.get("AGB_RESEARCH_DIR", "/research")) / "panel.parquet"
        if not saved.exists():
            pytest.skip(f"no saved panel at {saved}; set AGB_RESEARCH_DIR")
        return Panel.load(saved)
    with lake.connect(lake_dir) as conn:
        latest = conn.execute("SELECT max(date) FROM sep").fetchone()[0]
    return panel_module.build(lake_dir, date(latest.year - 5, 1, 1), None)


@pytest.fixture(scope="module")
def both(real_panel, lake_dir, tmp_path_factory) -> Both:
    """Both sides of the same panel, run once. The engine's fills come from the full run, the
    first simulation `backtest.run` makes; the later ones are its start-date sensitivity runs."""
    captured: list[backtest.Simulation] = []
    real = backtest.simulate

    def spy(feed, months, rankings, options):
        outcome = real(feed, months, rankings, options)
        captured.append(outcome)
        return outcome

    ledger = Ledger(tmp_path_factory.mktemp("live") / FILE)
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(backtest, "simulate", spy)
        engine = backtest.run(real_panel, backtest.Options(), ledger, lake_dir=lake_dir).results

    options = scan.Options()
    with lake.connect(lake_dir) as conn:
        orders = scan.build_orders(real_panel.frame, real_panel, options, conn)
    fast = scan.simulate(orders, options)

    return Both(
        panel=real_panel,
        engine=engine,
        engine_orders={(fill.day, fill.ticker, fill.side.value) for fill in captured[0].fills},
        scan=fast.results,
        scan_orders={
            (trade.Timestamp.date(), trade.Column, trade.Side.lower())
            for trade in fast.trades.itertuples(index=False)
        },
    )


def test_the_two_sides_ran_the_same_panel(both):
    """A sanity check before anything is compared, and the line the operator reads."""
    engine, fast = both.engine, both.scan
    print(
        f"\n{engine['months']} months: engine annual {engine['annual_return']:+.4f} sharpe"
        f" {engine['sharpe']:.3f} drawdown {engine['max_drawdown']:+.4f},"
        f" {engine['orders']} orders, {engine['delisting_exits']} delisting exits,"
        f" {engine['trimmed']} trims, {engine['unfilled']} unfilled; vectorbt annual"
        f" {fast['annual_return']:+.4f} sharpe {fast['sharpe']:.3f} drawdown"
        f" {fast['max_drawdown']:+.4f}, {fast['orders']} orders"
    )
    assert engine["months"] == fast["months"] == len(both.panel.months)


def test_the_engines_divergence_from_vectorbt_is_only_the_frictions_the_scan_omits(both):
    """Every order one side placed and the other did not is a delisting exit the scan does not
    model, the scan's own later sell of a name the engine has already exited, or whole-share
    sizing. Nothing else is allowed to differ, and most of the two sets must still be the same
    orders on the same sessions."""
    engine_only = both.engine_orders - both.scan_orders
    scan_only = both.scan_orders - both.engine_orders
    shared = both.engine_orders & both.scan_orders

    # A forced exit lands at the name's last listed print, which is not a rebalance session.
    sessions = {month.entry for month in both.panel.months}
    exits = {order for order in engine_only if order[0] not in sessions}
    assert all(side == "sell" for _, _, side in exits), "the engine only sells off a rebalance"
    assert len(exits) <= both.engine["delisting_exits"], "every one is a counted delisting exit"

    # The scan goes on holding each of those names at its last price until a rebalance drops it.
    exited = {ticker for _, ticker, _ in exits}
    late = {order for order in scan_only if order[1] in exited and order[2] == "sell"}
    assert len(late) <= len(exits), "one late sell at most per name the engine exited early"

    residual = (engine_only - exits) | (scan_only - late)
    assert len(residual) <= RESIDUAL * len(both.scan_orders), (
        f"{len(residual)} orders the frictions do not explain: {sorted(residual)[:10]}"
    )
    assert len(shared) >= AGREEMENT * len(both.scan_orders), "the two still run the same strategy"


def test_the_summary_statistics_stay_within_the_frictions_tolerance(both):
    """The scale-free statistics, against the tolerance this module derives. Total return is
    deliberately not among them: it compounds the frictions, so no fixed bound on it holds over
    both a five-year and a twenty-eight-year panel."""
    for key, tolerance in TOLERANCE.items():
        assert abs(both.engine[key] - both.scan[key]) < tolerance, key


def test_the_engine_fills_the_orders_it_places_at_a_sliver_of_the_days_volume(both):
    """The engine's own health on real names, which the fake market cannot show: the account
    funds what the strategy asks for, and no order is a large enough share of a name's daily
    dollar volume for the fill at the close to be a fiction."""
    assert both.engine["unfilled"] <= UNFILLED * both.engine["orders"]
    assert both.engine["participation_max"] < 0.05, "an order is a sliver of the day's volume"
