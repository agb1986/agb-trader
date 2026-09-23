"""The features against the real lake. Skipped unless AGB_LIVE_LAKE=1, with AGB_LAKE_DIR
pointing at a lake the operator has already pulled or copied down:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=/mnt/data/lake uv run pytest -m live -s tests/test_features_live.py

Nothing here talks to the vendor or writes to the lake; it only reads. It checks what the
fixtures cannot: that the features compute for month-ends across the whole history with the
coverage a ranker can use, that the vendor's sectors are the fixed vocabulary, and it prints the
sanity report of each feature's rank correlation with forward return. The report is shown, not
asserted on: a published factor can go quiet for years without the features being wrong.

The whole history is a command rather than a test, because it takes the better part of half an
hour:

    uv run python -m agb.features --lake <lake> history --out features.parquet
    uv run python -m agb.features --lake <lake> report"""

import os
from datetime import date
from pathlib import Path

import pytest

from agb import features, lake, universe

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("AGB_LIVE_LAKE") != "1", reason="set AGB_LIVE_LAKE=1 to run"),
]

# Month-ends spread across the history. The first is a year after SEP starts, the first month
# every window -- twelve months of prices, a year-ago filing -- can be full.
SAMPLE = [date(1999, 12, 31), date(2008, 8, 29), date(2020, 5, 29)]
# The share of a universe each numeric feature must cover. Measured on the sample it is 85% at
# the least (roe in 1999, from negative equity and young filers) and 92% or more since 2008.
MIN_COVERAGE = 0.80
# The report's span: two years, enough months to show each feature's sign, in a few minutes.
REPORT = (date(2015, 1, 1), date(2016, 12, 31))


@pytest.fixture(scope="module")
def lake_dir() -> Path:
    root = Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR))
    if not (root / "sep").is_dir():
        pytest.skip(f"no lake at {root}; set AGB_LAKE_DIR")
    return root


def coverage(result: features.Features, name: str) -> float:
    return 1 - result.raw.column(name).null_count / result.raw.num_rows


def number(value: float | None, spec: str) -> str:
    return "-" if value is None else format(value, spec)


def test_features_compute_for_month_ends_across_the_history(lake_dir):
    with lake.connect(lake_dir) as db:
        for as_of in [*SAMPLE, universe.month_ends()[-1]]:
            members = universe.snapshot(as_of, lake_dir, db=db)
            result = features.compute(members, lake_dir, db=db)

            covered = {name: coverage(result, name) for name in features.FEATURES}
            print(as_of, result.raw.num_rows, " ".join(f"{k}={v:.2f}" for k, v in covered.items()))
            assert result.tickers == members.tickers
            for name in features.NUMERIC:
                assert covered[name] >= MIN_COVERAGE, (as_of, name, covered[name])
                # Not 0 and 1 exactly: names tied at an end -- every debt-free name at the bottom
                # of debt_to_equity -- share the mean of their places.
                ranks = [r for r in result.ranked.column(name).to_pylist() if r is not None]
                assert 0.0 <= min(ranks) < max(ranks) <= 1.0, (as_of, name)
            assert covered["sector"] >= 0.99, as_of


def test_the_first_months_compute_with_what_the_lake_holds(lake_dir):
    # SEP starts in January 1998, so mid-1998 has no year of prices behind it: the momentum and
    # risk features are missing, and the rest are there. SF1's filing dates start then too, so a
    # year-ago filing exists only for the handful of late filers that filed a 1997 period in 1998.
    result = features.compute(universe.snapshot(date(1998, 6, 30), lake_dir), lake_dir)

    for name in ("rev_1m", "book_to_price", "log_mktcap", "amihud", "turnover"):
        assert coverage(result, name) >= 0.95, name
    for name in ("mom_12_1", "vol_12m"):
        assert coverage(result, name) == 0.0, name
    for name in ("accruals", "asset_growth"):
        assert coverage(result, name) < 0.01, name


def test_the_vendor_sectors_are_the_fixed_vocabulary(lake_dir):
    with lake.connect(lake_dir) as db:
        sectors = {
            row[0]
            for row in db.execute(
                "SELECT DISTINCT sector FROM tickers WHERE \"table\" = 'stocks'"
            ).fetchall()
        }

    assert sectors - {None, ""} <= set(features.SECTORS)


def test_the_sanity_report_shows_each_feature_against_forward_return(lake_dir):
    start, end = REPORT
    monthly = []
    with lake.connect(lake_dir) as db:
        for month in features.history(start, end, lake_dir, db=db):
            returns = features.forward_returns(month, lake_dir, db=db)
            assert returns is not None, month.as_of
            monthly.append(features.information_coefficients(month, returns))

    report = features.summarise(monthly)

    print(f"\nrank IC against the next month's return, {start} to {end}")
    for line in report:
        print(
            f"{line.feature:20} {line.expected_sign:+d} {line.months:3d}"
            f" {number(line.mean_ic, '+.4f')} t={number(line.t_stat, '+.2f')}"
            f" hit={number(line.hit_rate, '.2f')} {line.as_expected}"
        )
    assert all(line.months == len(monthly) == 24 for line in report)
