"""A proposal against the real lake. Skipped unless AGB_LIVE_LAKE=1, with AGB_LAKE_DIR pointing
at a lake the operator has already pulled or copied down:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=/mnt/data/lake uv run pytest -m live tests/test_basket_live.py

Nothing here talks to the vendor, the broker or the deployment; the database is the test
harness's throwaway Postgres and the lake is only read. It checks what the fixtures cannot: that
the placeholder ranker and the cost model run over a real month-end universe, that the
proposal comes out at twenty names with a costed order plan and the provenance the ticket asks
for, and that a real month passes every pre-trade check, so the gate approves it in paper
mode. The gate's clock is the evening of the signal session, before the trade session's MOC
cutoff, whatever the date the test runs on.

The lake is on the deployment server and the deployed image carries no tests, so run this on a
workstation against a copy of it:

    ssh <server> 'cd <deploy dir> && tar -cf - lake' | tar -C <local dir> -xf -"""

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from agb import lake, nyse, schedule, universe
from agb.placeholder_ranker import PlaceholderMomentum
from agb.rebalance import SIZE
from agb.schedule import Expected, Planned
from agb.services.pipeline import Pipeline

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("AGB_LIVE_LAKE") != "1", reason="set AGB_LIVE_LAKE=1 to run"),
]


@pytest.fixture(scope="module")
def lake_dir() -> Path:
    root = Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR))
    if not (root / "sep").is_dir():
        pytest.skip(f"no lake at {root}; set AGB_LAKE_DIR")
    return root


def test_the_latest_month_end_in_the_lake_proposes_a_twenty_name_basket(db, lake_dir):
    with lake.connect(lake_dir) as lake_db:
        latest = lake_db.execute("SELECT max(date) FROM sep").fetchone()[0]
    signal = universe.month_ends(end=latest)[-1]
    trade = nyse.NyseCalendar(signal, signal + timedelta(days=10)).next_session(signal)
    period = f"{trade:%Y-%m}"
    start = datetime.now(UTC) - timedelta(hours=1)
    job = Planned(
        schedule.REBALANCE,
        period,
        trade,
        start,
        (Expected(schedule.PROPOSAL, period, start + timedelta(hours=15), True),),
    )
    schedule.write(db.connect("scheduler"), [job])

    evening = nyse.NyseCalendar(signal, trade).close_at(signal) + timedelta(hours=2)
    Pipeline(lake_dir, "", clock=lambda: evening)(db.connect("pipeline"))

    cli = db.connect("cli")
    [(state,)] = cli.execute("SELECT state FROM agb.job_state").fetchall()
    assert state == "completed"
    [row] = cli.execute(
        "SELECT reference, session_date, signal_date, snapshot_date, partition_hash,"
        " universe_size, ranker, cost_model_version FROM agb.basket"
    ).fetchall()
    reference, session_date, signal_date, snapshot_date, digest, size, ranker, version = row
    print(f"{reference}: {size} names as of {snapshot_date}, ranked by {ranker}")
    assert (reference, session_date, signal_date, snapshot_date) == (period, trade, signal, signal)
    assert digest == lake.partition_digest(lake_dir, signal)
    assert universe.size_is_plausible(size)
    assert (ranker, version) == (PlaceholderMomentum().name, "1")
    failed = cli.execute(
        "SELECT level, name, symbol, measured, threshold FROM agb.gate_check WHERE NOT passed"
    ).fetchall()
    for level, name, symbol, measured, threshold in failed:
        print(f"{level} {name} {symbol or ''}: measured {measured}, needs {threshold}")
    assert failed == []
    assert cli.execute("SELECT state, actor FROM agb.basket_state").fetchall() == [
        ("approved", "system")
    ]

    targets = cli.execute(
        "SELECT symbol, weight, shares, held, carried, rank FROM agb.basket_target ORDER BY rank"
    ).fetchall()
    assert len(targets) == SIZE
    assert {t[1] for t in targets} == {Decimal(1) / SIZE}
    assert all(
        shares > 0 and not held and not carried for _, _, shares, held, carried, _ in targets
    )
    assert [t[5] for t in targets] == list(range(1, SIZE + 1))
    orders = cli.execute(
        "SELECT symbol, side, qty, notional_usd, modelled_cost_bps FROM agb.basket_order"
    ).fetchall()
    assert len(orders) == SIZE
    assert all(side == "buy" and qty > 0 and cost > 0 for _, side, qty, _, cost in orders)
    for symbol, _, qty, notional, cost in sorted(orders, key=lambda o: -o[4])[:3]:
        print(f"{symbol}: {qty} shares, ${notional:,.0f}, {cost:.2f} bps modelled")
