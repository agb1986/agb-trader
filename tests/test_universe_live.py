"""The universe against the real lake. Skipped unless AGB_LIVE_LAKE=1, with AGB_LAKE_DIR
pointing at a lake the operator has already pulled or copied down:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=/mnt/data/lake uv run pytest -m live tests/test_universe_live.py

Nothing here talks to the vendor or writes to the lake; it only reads. It checks the two claims
the fixtures cannot: that the current universe's size lands in the pre-trade gate's band, and
that a universe from the middle of the history holds names that have since been delisted."""

import os
from datetime import date
from pathlib import Path

import pytest

from agb import lake, universe

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("AGB_LIVE_LAKE") != "1", reason="set AGB_LIVE_LAKE=1 to run"),
]

# Month-ends spread across the history, to show a universe builds anywhere from 1998 on.
SAMPLE = [date(1998, 1, 30), date(2008, 8, 29), date(2020, 5, 29)]


@pytest.fixture(scope="module")
def lake_dir() -> Path:
    root = Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR))
    if not (root / "sep").is_dir():
        pytest.skip(f"no lake at {root}; set AGB_LAKE_DIR")
    return root


def test_the_current_universe_is_the_size_the_gate_expects(lake_dir):
    result = universe.snapshot(universe.month_ends()[-1], lake_dir)

    print(f"{result.as_of}: {result.size} names from {result.snapshot_date}")
    assert universe.size_is_plausible(result.size), (
        f"{result.size} names is outside the gate's band of {universe.MIN_SIZE}-{universe.MAX_SIZE}"
    )
    assert 3_000 <= result.size <= 4_000, "the spec expects roughly 3,000 to 4,000 names"
    assert result.partition_hash == lake.partition_digest(lake_dir, result.snapshot_date)


def test_a_universe_builds_for_month_ends_across_the_history(lake_dir):
    with lake.connect(lake_dir) as db:
        for as_of in SAMPLE:
            result = universe.snapshot(as_of, lake_dir, db=db)
            print(f"{as_of}: {result.size} names from {result.snapshot_date}")
            assert result.snapshot_date <= as_of
            assert result.size > 500


def test_an_old_universe_holds_names_that_have_since_been_delisted(lake_dir):
    with lake.connect(lake_dir) as db:
        old = universe.snapshot(date(2008, 8, 29), lake_dir, db=db)
        current = universe.snapshot(universe.month_ends()[-1], lake_dir, db=db)
        delisted = {
            row[0]
            for row in db.execute(
                "SELECT ticker FROM tickers WHERE \"table\" = 'stocks' AND isdelisted = 'Y'"
            ).fetchall()
        }

    gone = delisted & set(old.tickers)
    assert gone, "the 2008 universe holds no name that has since been delisted"
    assert not gone & set(current.tickers)
