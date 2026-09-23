"""The universe against the real lake. Skipped unless AGB_LIVE_LAKE=1, with AGB_LAKE_DIR
pointing at a lake the operator has already pulled or copied down:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=/mnt/data/lake uv run pytest -m live tests/test_universe_live.py

Nothing here talks to the vendor or writes to the lake; it only reads. It checks the two claims
the fixtures cannot: that the current universe's size lands in the pre-trade gate's band, and
that a universe from the middle of the history holds names that have since been delisted.

The lake is on the deployment server and the deployed image carries no tests, so run these on a
workstation against a copy of it:

    ssh <server> 'cd <deploy dir> && tar -cf - lake' | tar -C <local dir> -xf -"""

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
# The history starts in June rather than January because SF1's filings effectively begin in 1998:
# only 616 tickers have an as-reported filing by 1998-01-30, against 7,365 trading that session,
# so a January universe is 132 names. By June there are 7,377 filers and the universe is 1,891.
# The datekey gate is doing its job in both; only the second is a universe worth asserting on.
SAMPLE = [date(1998, 6, 30), date(2008, 8, 29), date(2020, 5, 29)]
# The universe as measured on 2026-08-31, with room for the lake to move under it.
CURRENT_SIZE = (2_200, 2_800)


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
    low, high = CURRENT_SIZE
    assert low <= result.size <= high, (
        f"the lake measured {low} to {high} names for a recent month-end"
    )
    assert result.partition_hash == lake.partition_digest(lake_dir, result.snapshot_date)


def test_a_universe_builds_for_month_ends_across_the_history(lake_dir):
    with lake.connect(lake_dir) as db:
        for as_of in SAMPLE:
            result = universe.snapshot(as_of, lake_dir, db=db)
            print(f"{as_of}: {result.size} names from {result.snapshot_date}")
            assert result.snapshot_date <= as_of
            assert result.size > 1_000


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
