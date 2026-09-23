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

import numpy as np
import pytest
from research_fake import PANEL_START, Market

import agb.rebalance
from agb import cost, lake, rebalance
from agb.research import __main__ as cli
from agb.research import backtest, reproduce, scan
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

    def spy(feed, months, rankings, options):
        outcome = real(feed, months, rankings, options)
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
        assert all(set(month) == {"as_of", "net", "gross"} for month in path["monthly"])
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
    from market_fake import Market as PlaceholderMarket

    root = PlaceholderMarket(tmp_path / "lake").names(3).land()
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
