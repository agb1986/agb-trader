"""The NautilusTrader validation backtest: the pipeline's own rebalance code on the engine's
clock, fills at the trade session's close charged the live cost model, delisted names exited
rather than dropped, one report per combinatorial path for the gates, the report's contents,
and agreement with the vectorbt scan on the reference configuration.

`research_panel` in conftest is the session's fake panel: two dozen names, thirty month-ends
from January 2024. The reference configuration is run once per module over all of it and its
fills kept; the other tests run a few months each."""

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest
from research_fake import PANEL_START, Market

import agb.rebalance
from agb import corporate, cost, lake, rebalance
from agb.research import __main__ as cli
from agb.research import backtest, certify, gates, reproduce, scan, tax
from agb.research import panel as panel_module
from agb.research.panel import Panel
from agb.research.trials import Ledger

REFERENCE = backtest.Options(signal="mom_12_1", size=5, hold_through=8)
# The stated tolerance for the engine against vectorbt. Two things separate them by design:
# the engine sizes whole shares at the signal session's close where vectorbt sizes fractional
# shares at the fill, and the engine's capital compounds through its account where vectorbt's
# compounds through a cash group. Over the fake market's thirty months the differences come out
# at a fraction of these.
TOLERANCE = {"total_return": 0.02, "sharpe": 0.25, "max_drawdown": 0.01}
SMALL = dict(size=3, hold_through=5)


@dataclass(frozen=True)
class Reference:
    run: object
    simulation: backtest.Simulation
    ledger: Ledger


@pytest.fixture
def ledger(tmp_path) -> Ledger:
    return Ledger(tmp_path / "trials.parquet")


def sliced(panel: Panel, count: int) -> Panel:
    """The panel's first `count` months."""
    return Panel(panel.select(panel.months[:count]), panel.partition_hash)


def capturing(monkeypatch, captured: list) -> None:
    """Keep every simulation the run makes: the full run first, then any start-date runs."""
    real = backtest.simulate

    def spy(feed, months, rankings, options, forced=()):
        outcome = real(feed, months, rankings, options, forced)
        captured.append((list(months), outcome))
        return outcome

    monkeypatch.setattr(backtest, "simulate", spy)


@pytest.fixture(scope="module")
def reference(research_panel, research_lake, tmp_path_factory) -> Reference:
    ledger = Ledger(tmp_path_factory.mktemp("reference") / "trials.parquet")
    captured: list = []
    with pytest.MonkeyPatch.context() as monkeypatch:
        capturing(monkeypatch, captured)
        run = backtest.run(research_panel, REFERENCE, ledger, lake_dir=research_lake)
    months, simulation = captured[0]
    assert months == research_panel.months
    return Reference(run, simulation, ledger)


# The strategy is the pipeline's rebalance code ------------------------------------------------


def test_the_strategy_runs_the_rebalance_code_the_pipeline_runs(
    research_panel, research_lake, ledger, monkeypatch
):
    """The module the strategy calls is `agb.rebalance` itself, the one `agb.basket` imports,
    and every month's selection, weights and order plan goes through it: the scan's ranking,
    the names the engine holds, and the live buffer sizes."""
    from agb import basket

    assert backtest.rebalance is agb.rebalance is basket.rebalance
    assert backtest.cost is basket.cost

    calls: dict[str, list] = {name: [] for name in ("select", "weights", "orders", "liquidity")}
    for module, name in (
        (rebalance, "select"),
        (rebalance, "weights"),
        (rebalance, "orders"),
        (cost, "liquidity"),
    ):
        real = getattr(module, name)

        def spy(*args, _real=real, _name=name, **kwargs):
            result = _real(*args, **kwargs)
            calls[_name].append((args, kwargs, result))
            return result

        monkeypatch.setattr(module, name, spy)

    panel = sliced(research_panel, 4)
    options = backtest.Options(signal="mom_6_1", **SMALL)
    run = backtest.run(panel, options, ledger, lake_dir=research_lake)

    assert run.results["orders"] > 3
    held: frozenset[str] = frozenset()
    assert len(calls["select"]) == 4
    for month, (args, kwargs, selection) in zip(panel.months, calls["select"], strict=True):
        ranking, previous = args
        assert ranking == scan.ranking(panel.select([month]), options), "the scan's ranking"
        assert previous == held, "the names the engine holds going in"
        assert kwargs["size"] == 3 and kwargs["hold_through"] == 5
        assert kwargs["untradeable"] == set(), "every fake name prints every session"
        held = frozenset(selection.names)
    weighted = [(args, result) for args, _, result in calls["weights"]]
    planned = [args for args, _, _ in calls["orders"]]
    assert len(weighted) == len(planned) == sum(1 for _, _, s in calls["select"] if s.buy or s.sell)
    for (weight_args, weight), plan_args in zip(weighted, planned, strict=True):
        selection, weight_used, prints, _, capital = plan_args
        assert weight_args == (selection.names, 3)
        assert weight_used is weight
        assert set(prints) >= set(selection.buy) | set(selection.sell)
        assert capital > 0
    assert {len(args[2]) for args, _, _ in calls["liquidity"]} == {backtest.LIQUIDITY_BARS}


# Fills, costs and fees ------------------------------------------------------------------------


def test_fills_are_at_the_trade_sessions_close_and_charged_the_cost_model(
    reference, research_panel, research_lake
):
    """Every fill is on a month's trade session, the session after the signal, at that
    session's adjusted close, and its fee is `cost.trade_cost` over `cost.liquidity` through
    the signal session, sells carrying the regulatory fees on top of the two-sided cost."""
    fills = reference.simulation.fills
    by_entry = {month.entry: month for month in research_panel.months}
    assert len(fills) == reference.run.results["orders"] > 30
    assert {fill.day for fill in fills} <= set(by_entry)
    with lake.connect(research_lake) as conn:
        for fill in fills:
            month = by_entry[fill.day]
            assert month.snapshot_date < fill.day
            [(closeadj,)] = conn.execute(
                "SELECT closeadj FROM sep WHERE ticker = ? AND date = ?", [fill.ticker, fill.day]
            ).fetchall()
            assert fill.price == pytest.approx(closeadj, abs=10**-backtest.PRECISION)
            rows = conn.execute(
                "SELECT high, low, close, volume FROM ("
                "  SELECT * FROM sep WHERE ticker = ? AND date <= ? ORDER BY date DESC LIMIT ?"
                ") ORDER BY date",
                [fill.ticker, month.snapshot_date, backtest.LIQUIDITY_BARS],
            ).fetchall()
            liquidity = cost.liquidity(*(list(column) for column in zip(*rows, strict=True)))
            notional = fill.qty * fill.price
            priced = cost.trade_cost(fill.side, notional, fill.qty, liquidity)
            assert fill.fee == pytest.approx(priced.total_usd, abs=0.005), "to the cent"
            assert fill.fee > 0
            assert fill.participation == pytest.approx(notional / liquidity.adv_usd)
            if fill.side is cost.Side.SELL:
                bought = cost.trade_cost(cost.Side.BUY, notional, fill.qty, liquidity)
                assert priced.total_usd > bought.total_usd, "sells pay the regulatory fees"
    results = reference.run.results
    assert results["fees_paid"] == pytest.approx(sum(fill.fee for fill in fills))
    assert results["fees_fraction"] == pytest.approx(results["fees_paid"] / REFERENCE.init_cash)
    assert results["gross_return"] > results["total_return"], "net is after the fees"
    assert results["gross_return"] - results["total_return"] == pytest.approx(
        results["fees_fraction"]
    )


@pytest.mark.parametrize("close", [0.025, 0.1, 1.000001, 10.123456, 112.345678, 4820.5])
def test_a_fill_one_tick_from_the_close_is_the_venues_spread(close):
    """The fill guard tolerates one tick at every price, down to the split-adjusted closes of
    the 1990s, and still catches two. One tick does not always measure as one tick in binary
    floating point: `0.025 - 0.024999` comes out at 1.000000000001e-06, just over the
    tolerance, where the same spread on a dollar-priced name comes out just under it, so a
    comparison that does not round first refuses penny stocks and nothing else."""
    tick = 10**-backtest.PRECISION
    assert not backtest._off_close(close, close)
    for price in (close - tick, close + tick):
        assert not backtest._off_close(price, close), "one tick is the venue's spread"
    for price in (close - 2 * tick, close + 2 * tick):
        assert backtest._off_close(price, close), "two ticks is not a fill at the close"
    assert backtest._off_close(close, None), "a session the name does not print is refused"


def test_the_engines_account_reconciles_with_the_fills(reference):
    """The cash the fills add up to is what the engine's account holds, and the positions the
    strategy tracked are what the fills leave."""
    fills = reference.simulation.fills
    positions: dict[str, int] = {}
    cash = REFERENCE.init_cash
    for fill in fills:
        signed = fill.qty if fill.side is cost.Side.BUY else -fill.qty
        positions[fill.ticker] = positions.get(fill.ticker, 0) + signed
        cash -= signed * fill.price + fill.fee
    assert {t: q for t, q in positions.items() if q} == reference.simulation.positions
    assert len(reference.simulation.positions) == REFERENCE.size, "fully invested at the end"
    assert 0 <= cash < REFERENCE.init_cash * 0.02, "the cash left is the whole-share rounding"
    assert reference.simulation.counts["unfilled"] == 0


def test_the_cost_multiple_scales_every_charge(research_panel, research_lake, ledger):
    panel = sliced(research_panel, 6)
    once = backtest.run(panel, backtest.Options(**SMALL), ledger, lake_dir=research_lake)
    twice = backtest.run(
        panel, backtest.Options(cost_multiple=2.0, **SMALL), ledger, lake_dir=research_lake
    )
    assert twice.results["orders"] == once.results["orders"]
    assert twice.results["fees_paid"] == pytest.approx(2 * once.results["fees_paid"], rel=0.01)
    assert twice.results["total_return"] < once.results["total_return"]
    assert twice.results["gross_return"] == pytest.approx(once.results["gross_return"], abs=0.002)


# Delisting ------------------------------------------------------------------------------------


def test_a_delisted_name_is_traded_and_exited_at_its_last_print(tmp_path, ledger, monkeypatch):
    """A name whose bars stop mid-panel is bought like any other, then sold on its last listed
    session at that session's close, and the report counts the exit."""
    root = (
        Market(tmp_path / "lake").names(12).name("DEL", drift=0.004, until=date(2024, 9, 15)).land()
    )
    panel = panel_module.build(root, PANEL_START, date(2025, 3, 31))
    captured: list = []
    capturing(monkeypatch, captured)
    options = backtest.Options(**SMALL)

    run = backtest.run(panel, options, ledger, lake_dir=root)

    _, simulation = captured[0]
    fills = [fill for fill in simulation.fills if fill.ticker == "DEL"]
    assert fills, "the fastest riser is bought"
    assert fills[0].side is cost.Side.BUY
    assert fills[0].day == panel.months[0].entry
    with lake.connect(root) as conn:
        [(last, close)] = conn.execute(
            "SELECT date, closeadj FROM sep WHERE ticker = 'DEL' ORDER BY date DESC LIMIT 1"
        ).fetchall()
    assert last == date(2024, 9, 13), "the fake's last session on or before the cutoff"
    assert fills[-1].side is cost.Side.SELL
    assert (fills[-1].day, fills[-1].price) == (last, pytest.approx(close))
    assert fills[-1].qty == sum(f.qty if f.side is cost.Side.BUY else -f.qty for f in fills[:-1])
    assert fills[-1].fee > 0, "the exit is costed like any fill"
    assert "DEL" not in simulation.positions
    assert run.results["delisting_exits"] == 1
    assert not any(fill.ticker == "DEL" for fill in simulation.fills if fill.day > last)
    assert all(day <= last for day in (fill.day for fill in fills))
    # The exit is on a session that is not a trade session, and the report still balances.
    assert last not in {month.entry for month in panel.months}
    assert run.results["final_value"] == pytest.approx(
        options.init_cash * (1 + run.results["total_return"])
    )


# Forced corporate events ----------------------------------------------------------------------

# Three months is enough for any of the four: the position is taken in the first month, the
# event falls between the second and the third, and the third's rebalance deals with what it
# left. The ranking is set by hand in these tests, so what each month selects is the test's
# own, and the only thing left to read is what the record did to it.
FORCED_END = date(2024, 3, 31)


def forced_panel(tmp_path, *extra: str):
    """A small market and its panel: twelve ordinary names plus any the test names."""
    market = Market(tmp_path / "lake").names(12)
    for ticker in extra:
        market.name(ticker, drift=0.0005)
    root = market.land()
    return panel_module.build(root, PANEL_START, FORCED_END), root


def by_hand(panel: Panel, picks: list[list[str]]) -> np.ndarray:
    """A score column that ranks each month's `picks` best first and leaves every other name
    out of the ranking, so each month's selection is exactly the one the test asked for."""
    chosen = {month.as_of: list(names) for month, names in zip(panel.months, picks, strict=True)}
    return np.array(
        [
            len(chosen[as_of]) - chosen[as_of].index(ticker) if ticker in chosen[as_of] else np.nan
            for as_of, ticker in zip(panel.frame["as_of"], panel.frame["ticker"], strict=True)
        ],
        dtype="float64",
    )


def between(root, ticker: str, after: date, before: date) -> tuple[date, float]:
    """The name's first session in the gap between two months, and its adjusted close there:
    when a corporate action takes effect, and what the holding is worth when it does."""
    with lake.connect(root) as conn:
        [(day, close)] = conn.execute(
            "SELECT date, closeadj FROM sep WHERE ticker = ? AND date > ? AND date < ?"
            " ORDER BY date LIMIT 1",
            [ticker, after, before],
        ).fetchall()
    return day, close


def filed(kind, symbol: str, day: date, period: str, **extra) -> corporate.Event:
    """One event as the operator would have filed it. `shares` is the holding the record
    names; a merger's ratio is applied to the position the engine actually holds, so the
    tests read the shares a conversion paid off the fills rather than out of the record."""
    return corporate.Event(
        period, symbol, kind, day, Decimal(100), "the broker's statement", **extra
    )


def traded_on(simulation: backtest.Simulation, day: date) -> list[tuple[str, str]]:
    """What the simulation traded on one session, by name and side."""
    return sorted((fill.ticker, fill.side.name) for fill in simulation.fills if fill.day == day)


def test_a_recorded_cash_acquisition_ends_the_position_and_the_cash_waits(
    tmp_path, ledger, monkeypatch
):
    """A cash acquisition ends the holding on its effective date at the deal price, charged no
    commission because a corporate action is not a trade of ours, and nothing else happens
    that session: the cash waits for the next rebalance, which spends it on the name the
    ranking wants and never buys the acquired name back."""
    panel, root = forced_panel(tmp_path)
    months = panel.months
    picks = [["N01", "N02", "N03", "N04", "N05"]] * len(months)
    day, close = between(root, "N01", months[1].entry, months[2].snapshot_date)
    # The deal price is that session's close to the cent: a deal that is certain prints at the
    # price it pays.
    event = filed(
        corporate.Kind.CASH_ACQUISITION,
        "N01",
        day,
        months[2].as_of.strftime("%Y-%m"),
        price_usd=Decimal(str(round(close, 2))),
    )
    options = backtest.Options(**SMALL)
    captured: list = []
    capturing(monkeypatch, captured)

    run = backtest.run(
        panel,
        options,
        ledger,
        lake_dir=root,
        scores=by_hand(panel, picks),
        forced=[event],
    )

    _, simulation = captured[0]
    fills = [fill for fill in simulation.fills if fill.ticker == "N01"]
    assert [fill.side for fill in fills] == [cost.Side.BUY, cost.Side.SELL]
    bought, ended = fills
    assert bought.day == months[0].entry
    assert ended.day == day and day not in {month.entry for month in months}
    assert ended.qty == bought.qty
    assert ended.price == pytest.approx(float(event.price_usd), abs=0.005), "the deal price"
    assert (ended.fee, ended.participation) == (0.0, 0.0), "no trade of ours, no charge"
    assert "N01" not in simulation.positions
    assert traded_on(simulation, day) == [("N01", "SELL")], "the cash is not redeployed here"
    assert traded_on(simulation, months[2].entry) == [("N04", "BUY")], "it is redeployed here"
    assert run.results["cash_acquisition_events"] == 1
    assert run.results["cash_acquisition_exits"] == 1
    assert run.results["final_value"] == pytest.approx(
        options.init_cash * (1 + run.results["total_return"])
    )


def test_a_recorded_merger_pays_in_shares_the_next_rebalance_sells(tmp_path, ledger, monkeypatch):
    """A stock-for-stock merger ends the holding and the acquirer's shares arrive in its
    place, neither leg charged a commission, and the next rebalance sells them however the
    acquirer ranks: that position was never chosen. Here it ranks first and is sold anyway."""
    panel, root = forced_panel(tmp_path, "ACQ")
    months = panel.months
    picks = [["N01", "N02", "N03", "N04", "N05"]] * 2 + [["ACQ", "N02", "N03", "N04", "N05"]]
    day, _ = between(root, "N01", months[1].entry, months[2].snapshot_date)
    event = filed(
        corporate.Kind.STOCK_MERGER,
        "N01",
        day,
        months[2].as_of.strftime("%Y-%m"),
        into_symbol="ACQ",
        into_shares=Decimal(40),
    )
    captured: list = []
    capturing(monkeypatch, captured)

    run = backtest.run(
        panel,
        backtest.Options(**SMALL),
        ledger,
        lake_dir=root,
        scores=by_hand(panel, picks),
        forced=[event],
    )

    _, simulation = captured[0]
    legs = [fill for fill in simulation.fills if fill.day == day]
    assert [(leg.ticker, leg.side) for leg in legs] == [
        ("N01", cost.Side.SELL),
        ("ACQ", cost.Side.BUY),
    ]
    ended, paid = legs
    assert all(leg.fee == 0.0 for leg in legs), "a conversion is not a trade of ours"
    assert paid.qty == int(ended.qty * event.ratio), "the recorded exchange ratio, whole shares"
    assert paid.price == pytest.approx(simulation.fills[0].price, rel=1), "the acquirer's close"
    sells = [f for f in simulation.fills if f.ticker == "ACQ" and f.side is cost.Side.SELL]
    assert len(sells) == 1 and sells[0].day == months[2].entry
    assert sells[0].qty == paid.qty
    assert sells[0].fee > 0, "an ordinary sell at the rebalance, charged like one"
    assert picks[2][0] == "ACQ" and "ACQ" not in simulation.positions
    assert run.results["stock_merger_events"] == 1
    assert run.results["stock_merger_exits"] == 1


def test_a_recorded_delisting_exits_the_position_whatever_the_name_ranks(
    tmp_path, ledger, monkeypatch
):
    """A name the operator recorded as delisted to OTC is sold at the next rebalance however
    it ranks, and it is the record that sells it: the vendor is still printing the name, so
    nothing in the bars would have exited it."""
    panel, root = forced_panel(tmp_path, "DARK")
    months = panel.months
    picks = [["DARK", "N02", "N03", "N04", "N05"]] * len(months)
    day, close = between(root, "DARK", months[1].entry, months[2].snapshot_date)
    event = filed(
        corporate.Kind.DELISTING,
        "DARK",
        day,
        months[2].as_of.strftime("%Y-%m"),
        price_usd=Decimal(str(round(close, 2))),  # the last listed print
    )
    captured: list = []
    capturing(monkeypatch, captured)

    run = backtest.run(
        panel,
        backtest.Options(**SMALL),
        ledger,
        lake_dir=root,
        scores=by_hand(panel, picks),
        forced=[event],
    )

    _, simulation = captured[0]
    fills = [fill for fill in simulation.fills if fill.ticker == "DARK"]
    assert [fill.side for fill in fills] == [cost.Side.BUY, cost.Side.SELL]
    bought, exited = fills
    assert exited.day == months[2].entry and exited.qty == bought.qty
    assert exited.fee > 0, "an ordinary sell at the rebalance, charged like one"
    assert picks[2][0] == "DARK" and "DARK" not in simulation.positions
    with lake.connect(root) as conn:
        [(last,)] = conn.execute("SELECT max(date) FROM sep WHERE ticker = 'DARK'").fetchall()
    assert last > exited.day, "the bars run on; the record is what ends the position"
    assert run.results["delisting_events"] == 1
    assert run.results["delisting_exits"] == 0, "no bar went missing"


def test_a_recorded_halt_carries_the_position_and_skips_its_replacement(
    tmp_path, ledger, monkeypatch
):
    """A halt spanning a rebalance carries the position: it keeps its slot, gets no order, and
    its replacement is not bought, so one fewer name trades that month. The same month without
    the record sells the halted name and buys the replacement."""
    panel, root = forced_panel(tmp_path)
    months = panel.months
    picks = [["N01", "N02", "N03", "N04", "N05"]] * 2 + [["N01", "N06", "N07", "N08", "N05"]]
    scores = by_hand(panel, picks)
    day, _ = between(root, "N03", months[1].entry, months[2].snapshot_date)
    event = filed(corporate.Kind.HALT, "N03", day, months[2].as_of.strftime("%Y-%m"))
    options = backtest.Options(**SMALL)
    captured: list = []
    capturing(monkeypatch, captured)

    run = backtest.run(panel, options, ledger, lake_dir=root, scores=scores, forced=[event])
    backtest.run(panel, options, ledger, lake_dir=root, scores=scores)

    (_, carried), (_, unrecorded) = captured
    entry = months[2].entry
    # N02 and N03 both fall out of the ranking in the third month. Without the record both are
    # sold and both replacements are bought; with it N03 keeps its slot, one of the two
    # replacements is skipped, and one fewer name trades.
    assert traded_on(unrecorded, entry) == [
        ("N02", "SELL"),
        ("N03", "SELL"),
        ("N06", "BUY"),
        ("N07", "BUY"),
    ]
    assert traded_on(carried, entry) == [("N02", "SELL"), ("N06", "BUY")]
    assert "N07" not in carried.positions, "the replacement is skipped, not deferred"
    held = [fill for fill in carried.fills if fill.ticker == "N03"]
    assert [fill.side for fill in held] == [cost.Side.BUY]
    assert carried.positions["N03"] == held[0].qty, "the holding is untouched"
    assert len(carried.positions) == options.size, "the slot is kept, not left empty"
    assert "N03" not in unrecorded.positions
    assert run.results["halt_events"] == 1


# The report -----------------------------------------------------------------------------------


def test_the_report_has_gross_and_net_turnover_participation_and_start_dates(
    reference, research_panel
):
    results = reference.run.results
    months = research_panel.months
    assert results["months"] == len(months) == len(results["monthly"]) == 30
    assert [m["as_of"] for m in results["monthly"]] == [m.as_of.isoformat() for m in months]
    assert all(month["gross"] >= month["net"] for month in results["monthly"])
    assert any(month["gross"] > month["net"] for month in results["monthly"])
    compounded = float(np.prod([1 + month["net"] for month in results["monthly"]])) - 1
    # The first rebalance's fees come off before the first month's return is measured.
    assert 0 < compounded - results["total_return"] < results["fees_fraction"]
    assert results["final_value"] == pytest.approx(
        REFERENCE.init_cash * (1 + results["total_return"])
    )
    assert results["annual_volatility"] > 0
    assert results["sharpe"] == pytest.approx(
        results["annual_return"] / results["annual_volatility"]
    )
    assert -1 < results["max_drawdown"] < 0
    assert 0 < results["turnover"] < 1, "two-sided notional traded over the value, a month's mean"
    assert 0 < results["participation_mean"] <= results["participation_max"] < 1
    assert results["trimmed"] >= 0 and results["unfilled"] == 0 and results["delisting_exits"] == 0

    starts = results["start_dates"]
    assert [start["start"] for start in starts] == [
        months[i].as_of.isoformat() for i in (30 // 4, 30 // 2, 3 * 30 // 4)
    ]
    assert [start["months"] for start in starts] == [23, 15, 8]
    for start in starts:
        assert set(start) == {
            "start", "months", "total_return", "annual_return", "annual_volatility", "sharpe",
            "max_drawdown",
        }  # fmt: skip
        assert start["total_return"] != results["total_return"]
    assert len({start["total_return"] for start in starts}) == 3


def test_explicit_start_dates_run_from_the_first_month_at_or_after_each(
    research_panel, research_lake, ledger
):
    panel = sliced(research_panel, 6)
    options = backtest.Options(starts=("2024-03-01", "2024-03-15", date(2024, 5, 15)), **SMALL)
    assert options.starts == ("2024-03-01", "2024-03-15", "2024-05-15")

    run = backtest.run(panel, options, ledger, lake_dir=research_lake)

    starts = run.results["start_dates"]
    march, may = panel.months[2].as_of, panel.months[4].as_of
    assert (march, may) == (date(2024, 3, 28), date(2024, 5, 31)), "Good Friday closed March"
    assert [start["start"] for start in starts] == [march.isoformat(), may.isoformat()]
    assert [start["months"] for start in starts] == [4, 2], "no repeats"
    assert run.results["months"] == 6
    with pytest.raises(backtest.BacktestError, match="no months to backtest from 2024-06-28"):
        backtest.run(
            panel, backtest.Options(starts=("2024-06-28",), **SMALL), ledger, lake_dir=research_lake
        )
    assert (
        backtest.run(
            sliced(research_panel, 3), backtest.Options(**SMALL), ledger, lake_dir=research_lake
        ).results["start_dates"]
        == []
    ), "too short for the default sensitivities"


# The UK after-tax diagnostic ------------------------------------------------------------------


def test_the_report_carries_the_approximate_after_tax_view(reference):
    """The report states an approximate UK capital gains bill on the run's own disposals, with
    the rate, the allowance and the simplifications it rests on beside the numbers, and the
    buy-and-hold comparison of the benchmark the reference run's lake holds."""
    results = reference.run.results
    view = results["after_tax"]

    assert view["approximate"] is True and "approximate" in view["basis"]
    assert "no gate reads it" in view["basis"]
    assert (view["rate"], view["annual_exempt_amount"]) == (0.24, 3000.0)
    assert view["simplifications"] == list(tax.SIMPLIFICATIONS)
    assert view["benchmark"] == backtest.BENCHMARK == "SPY"
    assert view["buy_and_hold"] is not None, "the research lake holds the benchmark"

    taxed = view["strategy"]
    # Everything bought is sold or deemed sold at the final close, so the gains net to exactly
    # what the account made: the diagnostic taxes the report's own return, not a different one.
    assert taxed["realised_gains"] - taxed["realised_losses"] == pytest.approx(
        results["final_value"] - REFERENCE.init_cash, abs=0.05
    )
    assert [year["year"] for year in taxed["years"]] == ["2024/25", "2025/26", "2026/27"]
    assert sum(year["taxable"] for year in taxed["years"]) == pytest.approx(taxed["taxable_gains"])
    assert taxed["tax"] == pytest.approx(0.24 * taxed["taxable_gains"], abs=0.05)
    assert taxed["tax"] > 0, "a monthly rebalance realises its gains, so there is a bill"
    assert taxed["final_value"] == results["final_value"]
    assert taxed["total_return"] == pytest.approx(results["total_return"])
    assert taxed["after_tax_final_value"] == pytest.approx(taxed["final_value"] - taxed["tax"])
    assert taxed["after_tax_return"] == pytest.approx(
        taxed["after_tax_final_value"] / REFERENCE.init_cash - 1
    )
    assert taxed["after_tax_return"] < results["total_return"]
    # Every sell is a disposal, and whatever is still held at the end is deemed disposed of.
    sells = sum(1 for fill in reference.simulation.fills if fill.side is cost.Side.SELL)
    assert taxed["disposals"] == sells + len(
        [q for q in reference.simulation.positions.values() if q]
    )
    json.dumps(view)  # what the ledger holds


def test_a_lake_without_the_benchmark_still_backtests(tmp_path, ledger):
    """The real lake holds equity prices and no index fund. A plain backtest on such a lake
    completes as before: the buy-and-hold comparison reports nothing and names the ticker it
    wanted, and each month's benchmark return is missing rather than guessed, which is what
    certification reads as an excess Sharpe it cannot measure (issue #167)."""
    root = Market(tmp_path / "lake").names(12).land()
    panel = panel_module.build(root, PANEL_START, FORCED_END)

    run = backtest.run(panel, backtest.Options(**SMALL), ledger, lake_dir=root)

    view = run.results["after_tax"]
    assert view["benchmark"] == backtest.BENCHMARK and view["buy_and_hold"] is None
    assert run.results["monthly"], "the run traded"
    assert all(month["benchmark"] is None for month in run.results["monthly"])


def benchmark_close(root, day: date) -> Decimal:
    """The benchmark's adjusted close on one session, at the precision the engine reads it."""
    with lake.connect(root) as conn:
        [(close,)] = conn.execute(
            "SELECT closeadj FROM sep WHERE ticker = ? AND date = ?", [backtest.BENCHMARK, day]
        ).fetchall()
    return Decimal(str(round(close, backtest.PRECISION)))


def test_the_benchmark_is_taxed_the_same_way_when_the_lake_holds_it(tmp_path, ledger, monkeypatch):
    """Buy-and-hold under the same treatment: whole shares of the benchmark bought at the first
    trade session's close with the same starting cash, the rest left in cash, held to the final
    exit and deemed disposed of there, so one disposal in one tax year takes one allowance and
    the rest of the gain is still untaxed.

    The benchmark is priced but never traded: it is an index fund, so the universe leaves it
    out of the panel and the strategy can neither rank nor buy it."""
    market = Market(tmp_path / "lake").names(12)
    market.name(backtest.BENCHMARK, drift=0.003, noise=0.001, category="ETF")
    root = market.land()
    panel = panel_module.build(root, PANEL_START, FORCED_END)
    assert backtest.BENCHMARK not in set(panel.frame["ticker"]), "not a domestic common stock"
    captured: list = []
    capturing(monkeypatch, captured)

    run = backtest.run(panel, backtest.Options(**SMALL), ledger, lake_dir=root)

    months = panel.months
    cash = Decimal(str(backtest.Options.init_cash))
    start, end = benchmark_close(root, months[0].entry), benchmark_close(root, months[-1].exit)
    shares = int(cash // start)
    gain = shares * (end - start)
    assert gain > tax.EXEMPT_AMOUNT, "the one disposal is big enough for the allowance to bite"
    bill = float(((gain - tax.EXEMPT_AMOUNT) * tax.RATE).quantize(Decimal("0.01")))

    hold = run.results["after_tax"]["buy_and_hold"]
    assert hold["shares"] == shares and hold["disposals"] == 1
    assert [year["year"] for year in hold["years"]] == ["2024/25"]
    assert hold["realised_gains"] == pytest.approx(float(gain), abs=0.05)
    assert hold["tax"] == pytest.approx(bill, abs=0.05)
    assert hold["final_value"] == pytest.approx(float(cash + gain), abs=0.05), (
        "the residual cash the whole shares left over is still there at the end"
    )
    assert hold["after_tax_return"] == pytest.approx(hold["total_return"] - bill / float(cash))
    assert hold["loss_carried_forward"] == 0.0
    _, simulation = captured[0]
    assert backtest.BENCHMARK not in {fill.ticker for fill in simulation.fills}
    assert run.results["after_tax"]["strategy"]["tax"] >= 0


def test_no_gate_reads_the_after_tax_view(research_panel, research_lake, ledger, monkeypatch):
    """The diagnostic certifies nothing: tax depends on the operator's other income, rate band
    and allowance usage rather than on the strategy, so the specification keeps it out of
    certification. Under a rate that would change any verdict, every other figure in the report
    — the monthly net and gross series and the participation the gates read among them — comes
    out identical, and neither the certification nor the gates module mentions it."""
    panel = sliced(research_panel, 6)
    options = backtest.Options(**SMALL)

    plain = backtest.run(panel, options, ledger, lake_dir=research_lake).results
    monkeypatch.setattr(tax, "RATE", Decimal("0.90"))
    taxing = backtest.run(panel, options, ledger, lake_dir=research_lake).results

    assert taxing["after_tax"]["rate"] == 0.9
    assert taxing["after_tax"]["strategy"]["tax"] > plain["after_tax"]["strategy"]["tax"]
    assert {key: value for key, value in taxing.items() if key != "after_tax"} == {
        key: value for key, value in plain.items() if key != "after_tax"
    }
    for module in (certify, gates):
        assert "after_tax" not in Path(module.__file__).read_text(), module.__name__


# Combinatorial paths --------------------------------------------------------------------------


def test_each_combinatorial_path_is_reported_for_the_gates(research_panel, research_lake, ledger):
    """Given one score series per path, the run reports each path's monthly net and gross
    series and summary statistics separately, keyed by the path's label, with a summary across
    paths, which is what the deflated Sharpe and overfitting gates read."""
    panel = sliced(research_panel, 6)
    frame = panel.frame
    scores = {"0": frame["mom_12_1"].to_numpy(), "1": -frame["mom_12_1"].to_numpy()}
    options = backtest.Options(signal="score", **SMALL)

    run = backtest.run(panel, options, ledger, lake_dir=research_lake, scores=scores)

    results = run.results
    assert set(results) == {"months", "paths", "summary"}, "no start dates per path"
    assert results["months"] == 6
    paths = results["paths"]
    assert [path["path"] for path in paths] == ["0", "1"]
    for path in paths:
        assert {"total_return", "gross_return", "sharpe", "max_drawdown", "turnover"} <= set(path)
        assert {"participation_max", "delisting_exits", "orders", "fees_paid"} <= set(path)
        assert len(path["monthly"]) == 6
        assert all(
            set(month) == {"as_of", "net", "gross", "benchmark"} for month in path["monthly"]
        )
    # Each month's benchmark return runs over the same two sessions as the book's, from that
    # month's entry to the next one's and from the last entry to the final exit, so the gates
    # can subtract the one from the other month by month (issue #167).
    days = [month.entry for month in panel.months] + [panel.months[-1].exit]
    closes = [float(benchmark_close(research_lake, day)) for day in days]
    expected = [end / start - 1 for start, end in zip(closes[:-1], closes[1:], strict=True)]
    for path in paths:
        assert [month["benchmark"] for month in path["monthly"]] == pytest.approx(expected)
    assert paths[0]["total_return"] != paths[1]["total_return"], "the reversed score buys the tail"
    summary = results["summary"]
    assert summary["paths"] == 2
    assert summary["total_return_max"] == max(path["total_return"] for path in paths)
    assert summary["total_return_min"] == min(path["total_return"] for path in paths)
    assert summary["total_return_mean"] == pytest.approx(
        np.mean([path["total_return"] for path in paths])
    )
    assert summary["sharpe_mean"] == pytest.approx(np.mean([path["sharpe"] for path in paths]))
    assert json.loads(ledger.get(run.trial_id)["features"]) == ["score"]
    json.dumps(results)  # what the ledger holds and the gates load

    # The first path is the feature itself: the same run as naming the feature.
    plain = backtest.run(
        panel, backtest.Options(signal="mom_12_1", **SMALL), ledger, lake_dir=research_lake
    )
    assert paths[0]["monthly"] == plain.results["monthly"]


# Parity with the scan -------------------------------------------------------------------------


def test_the_engine_agrees_with_vectorbt_on_the_reference_configuration(
    reference, research_panel, research_lake
):
    """The same names on the same sides on the same sessions, and the summary statistics within
    the stated tolerance."""
    options = scan.Options(signal="mom_12_1", size=5, hold_through=8)
    with lake.connect(research_lake) as conn:
        orders = scan.build_orders(research_panel.frame, research_panel, options, conn)
    vectorbt = scan.simulate(orders, options)

    engine = reference.run.results
    for key, tolerance in TOLERANCE.items():
        assert abs(engine[key] - vectorbt.results[key]) < tolerance, key
    assert engine["orders"] == vectorbt.results["orders"]
    assert engine["months"] == vectorbt.results["months"] == 30
    by_engine = {(fill.day, fill.ticker, fill.side.value) for fill in reference.simulation.fills}
    by_vectorbt = {
        (trade.Timestamp.date(), trade.Column, trade.Side.lower())
        for trade in vectorbt.trades.itertuples(index=False)
    }
    assert by_engine == by_vectorbt
    # The engine sizes whole shares and compounds through the account, so its fees differ from
    # the scan's estimate at a fixed notional, but not by much.
    assert engine["fees_paid"] == pytest.approx(vectorbt.results["fees_paid"], rel=0.15)


# The ledger, reproduction and the command -----------------------------------------------------


def test_a_backtest_is_a_trial_that_reproduces(research_panel, research_lake, ledger):
    panel = sliced(research_panel, 6)
    options = backtest.Options(starts=("2024-04-01",), **SMALL)

    run = backtest.run(panel, options, ledger, lake_dir=research_lake)

    row = ledger.get(run.trial_id)
    assert (row["kind"], row["status"], row["refits"]) == ("backtest", "succeeded", 0)
    assert json.loads(row["features"]) == ["mom_12_1"]
    assert json.loads(row["config"]) == {
        "backtest": {
            "signal": "mom_12_1",
            "size": 3,
            "hold_through": 5,
            "init_cash": 1e6,
            "cost_multiple": 1.0,
            "starts": ["2024-04-01"],
        }
    }
    assert (row["train_start"], row["train_end"]) == (date(2024, 1, 31), date(2024, 6, 28))
    assert json.loads(row["results"]) == run.results

    result = reproduce.reproduce(ledger, run.trial_id, panel, lake_dir=research_lake)

    assert result.matches
    assert result.rerun.trial_id != run.trial_id
    assert ledger.get(result.rerun.trial_id)["config"] == row["config"]


def test_a_backtest_signal_is_a_feature_or_a_supplied_score(research_panel, research_lake, ledger):
    with pytest.raises(backtest.BacktestError, match="not a feature"):
        backtest.run(
            research_panel, backtest.Options(signal="alpha"), ledger, lake_dir=research_lake
        )
    assert ledger.count() == 0
    with pytest.raises(ValueError, match="hold-through"):
        backtest.Options(size=30, hold_through=20)
    with pytest.raises(ValueError, match="size"):
        backtest.Options(size=0)
    with pytest.raises(ValueError, match="cost_multiple"):
        backtest.Options(cost_multiple=0)
    with pytest.raises(ValueError):
        backtest.Options(starts=("yesterday",))
    with pytest.raises(backtest.BacktestError, match="no paths"):
        backtest.run(research_panel, backtest.Options(), ledger, lake_dir=research_lake, scores={})


def test_a_lake_without_the_cost_columns_is_refused(tmp_path, research_panel, ledger):
    from market_fake import Market as ServiceMarket

    root = ServiceMarket(tmp_path / "lake").names(3).land()
    with pytest.raises(backtest.BacktestError, match=r"lacks \['closeadj'\]"):
        backtest.run(sliced(research_panel, 2), backtest.Options(**SMALL), ledger, lake_dir=root)
    [row] = ledger.rows()
    assert row["status"] == "failed"


def test_the_cli_runs_a_backtest(research_panel, research_lake, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("AGB_LAKE_DIR", str(research_lake))
    monkeypatch.setenv("AGB_RESEARCH_DIR", str(tmp_path))
    sliced(research_panel, 6).save(tmp_path / "panel.parquet")

    cli.main(["backtest", "--size", "3", "--hold-through", "5", "--cost-multiple", "2",
              "--start", "2024-04-01", "--start", "2024-05-01"])  # fmt: skip

    first, rest = capsys.readouterr().out.split("\n", 1)
    assert first.startswith("trial ")
    results = json.loads(rest)
    assert results["months"] == 6
    assert [start["start"] for start in results["start_dates"]] == ["2024-04-30", "2024-05-31"]
    [row] = Ledger(tmp_path / "trials.parquet").rows()
    assert row["trial_id"] == first.removeprefix("trial ")
    assert json.loads(row["config"])["backtest"]["cost_multiple"] == 2.0
    with pytest.raises(SystemExit, match="backtest: 'alpha' is not a feature"):
        cli.main(["backtest", "--signal", "alpha"])
