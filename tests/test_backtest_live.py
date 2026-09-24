"""The validation backtest against the real lake. Skipped unless AGB_LIVE_LAKE=1, with
AGB_LAKE_DIR pointing at a lake the operator has already synced or copied down:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=/mnt/data/lake uv run pytest -m live -s tests/test_backtest_live.py

Nothing here talks to the vendor, the broker or the deployment; the lake is only read. It
checks what the fake market cannot: that the engine and vectorbt agree on the reference
configuration, the live twenty-of-thirty basket, over real names with real gaps, splits and
delistings, within the tolerance the fake-market test states. The panel is the saved one under
AGB_RESEARCH_DIR if there is one, else it is built over the last five years of the lake, which
takes a while."""

import os
from datetime import date
from pathlib import Path

import pytest
from test_backtest import TOLERANCE

from agb import lake
from agb.research import backtest, scan
from agb.research import panel as panel_module
from agb.research.panel import Panel
from agb.research.trials import FILE, Ledger

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


@pytest.fixture(scope="module")
def real_panel(lake_dir) -> Panel:
    saved = Path(os.environ.get("AGB_RESEARCH_DIR", "/research")) / "panel.parquet"
    if saved.exists():
        return Panel.load(saved)
    with lake.connect(lake_dir) as conn:
        latest = conn.execute("SELECT max(date) FROM sep").fetchone()[0]
    return panel_module.build(lake_dir, date(latest.year - 5, 1, 1), None)


def test_the_engine_agrees_with_vectorbt_on_the_real_lake(lake_dir, real_panel, tmp_path):
    ledger = Ledger(tmp_path / FILE)
    engine = backtest.run(real_panel, backtest.Options(), ledger, lake_dir=lake_dir).results
    fast = scan.run(real_panel, scan.Options(), ledger, lake_dir=lake_dir).results
    print(
        f"\n{engine['months']} months: engine total {engine['total_return']:+.4f} sharpe"
        f" {engine['sharpe']:.3f} drawdown {engine['max_drawdown']:+.4f}, {engine['orders']}"
        f" orders, {engine['delisting_exits']} delisting exits, {engine['trimmed']} trims;"
        f" vectorbt total {fast['total_return']:+.4f} sharpe {fast['sharpe']:.3f} drawdown"
        f" {fast['max_drawdown']:+.4f}, {fast['orders']} orders"
    )
    assert engine["months"] == fast["months"]
    for key, tolerance in TOLERANCE.items():
        assert abs(engine[key] - fast[key]) < tolerance, key
    assert engine["unfilled"] == 0
    assert engine["participation_max"] < 0.05, "an order is a sliver of the day's volume"
