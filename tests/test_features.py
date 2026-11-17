"""The twenty features: each numeric one against figures worked out by hand, the `datekey` gate
that keeps a filing out of the months before it was filed, the cross-sectional percentile rank,
`sector` as a fixed categorical, and the sanity report's forward returns and rank correlations."""

import ast
import hashlib
import itertools
import math
import random
import re
import shutil
import statistics
import sys
import tomllib
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from agb import features, lake
from agb.universe import Member, Universe, snapshot

SESSION = date(2026, 8, 31)  # a Monday, the last session of August
REPO = Path(__file__).resolve().parents[1]

_SEP = pa.schema([
    ("ticker", pa.string()), ("date", pa.date32()), ("close", pa.float64()),
    ("closeadj", pa.float64()), ("closeunadj", pa.float64()), ("volume", pa.float64()),
])  # fmt: skip
_SF1 = pa.schema([
    ("ticker", pa.string()), ("dimension", pa.string()), ("calendardate", pa.date32()),
    ("datekey", pa.date32()), ("lastupdated", pa.date32()), ("reportperiod", pa.date32()),
    ("sharesbas", pa.float64()), ("sharefactor", pa.float64()), ("netinc", pa.float64()),
    ("revenue", pa.float64()), ("fcf", pa.float64()), ("gp", pa.float64()),
    ("assets", pa.float64()), ("equity", pa.float64()), ("debt", pa.float64()),
    ("cashneq", pa.float64()), ("liabilities", pa.float64()),
])  # fmt: skip
_TICKERS = pa.schema([
    ("table", pa.string()), ("permaticker", pa.int64()), ("ticker", pa.string()),
    ("category", pa.string()), ("industry", pa.string()), ("siccode", pa.string()),
    ("sector", pa.string()),
])  # fmt: skip

# One set of statements, filed for the quarter to June 2026, and the year-ago quarter's.
STATEMENTS = {
    "netinc": 50e6,
    "revenue": 800e6,
    "fcf": 40e6,
    "gp": 300e6,
    "assets": 1_500e6,
    "equity": 600e6,
    "debt": 400e6,
    "cashneq": 100e6,
    "liabilities": 900e6,
}
YEAR_AGO = {"assets": 1_250e6, "cashneq": 50e6, "liabilities": 800e6, "debt": 350e6}


def weekdays(start: date, end: date) -> list[date]:
    """Every weekday from `start` to `end`. The features take their windows from the sessions
    the lake holds, so weekdays stand in for sessions without a calendar."""
    days = (start + timedelta(days=i) for i in range((end - start).days + 1))
    return [day for day in days if day.weekday() < 5]


class Lake:
    """A lake built a name at a time, with every column the universe and the features read."""

    def __init__(self, root):
        self.root = root
        self.rows = {"sep": [], "sf1": [], "tickers": []}
        self.permaticker = 100_000

    def trades(self, ticker, prices, *, close=None, unadj=None, volume=1_000_000.0):
        """`prices` maps each session to its adjusted close. `close` and `unadj`, the split-
        adjusted and the as-traded close, default to it: no split and no dividend."""
        for day, price in prices.items():
            self.rows["sep"].append((
                ticker, day, price if close is None else close, price,
                price if unadj is None else unadj, volume,
            ))  # fmt: skip
        return self

    def files(
        self, ticker, *, calendardate, datekey, dimension="ART", shares=None,
        lastupdated=SESSION, reportperiod=None, **figures,
    ):  # fmt: skip
        values = [figures.get(column) for column in _SF1.names[8:]]
        period = calendardate if reportperiod is None else reportperiod
        self.rows["sf1"].append((
            ticker, dimension, calendardate, datekey, lastupdated, period, shares, 1.0, *values
        ))  # fmt: skip
        return self

    def listed(self, ticker, sector="Technology", *, category="Domestic Common Stock"):
        self.permaticker += 1
        self.rows["tickers"].append(
            ("stocks", self.permaticker, ticker, category, "Software", "7372", sector)
        )
        return self

    def land(self, *, without=()):
        """Write the lake. `without` names SF1 columns to leave out of it."""
        if not self.rows["sf1"]:  # a real lake always holds fundamentals, if not these names'
            self.files("NOBODY", calendardate=date(2026, 3, 31), datekey=date(2026, 5, 8))
        for dataset, schema in ((lake.SEP, _SEP), (lake.SF1, _SF1), (lake.TICKERS, _TICKERS)):
            rows = self.rows[dataset.name]
            table = pa.Table.from_pylist(
                [dict(zip(schema.names, row, strict=True)) for row in rows], schema=schema
            )
            if dataset is lake.SF1:
                table = table.drop_columns(list(without))
            lake.land(self.root, dataset, table, SESSION)
        return self.root


def flat(sessions, price=100.0):
    return dict.fromkeys(sessions, price)


def universe_of(*tickers, as_of=SESSION, session=SESSION, market_cap=1e9):
    """A universe with the given names in it, as if the universe's filters had let them in."""
    members = tuple(Member(t, 100.0, 1e8, market_cap) for t in tickers)
    return Universe(as_of=as_of, snapshot_date=session, partition_hash="", members=members)


def raw(result, ticker):
    """One name's features before ranking."""
    return next(row for row in result.raw.to_pylist() if row["ticker"] == ticker)


# Momentum ---------------------------------------------------------------------------------

YEAR = weekdays(date(2025, 8, 1), SESSION)


def stepped(sessions):
    """A price that holds at 50 through August 2025, 60 through February 2026, 80 through
    July 2026, and 72 in August 2026: marks of p12 = 50, p6 = 60, p1 = 80 and p0 = 72."""

    def price(day):
        if day <= date(2025, 8, 31):
            return 50.0
        if day <= date(2026, 2, 28):
            return 60.0
        if day <= date(2026, 7, 31):
            return 80.0
        return 72.0

    return {day: price(day) for day in sessions}


def test_momentum_is_measured_between_month_end_marks(tmp_path):
    root = Lake(tmp_path).trades("MOM", stepped(YEAR)).listed("MOM").land()

    row = raw(features.compute(universe_of("MOM"), root), "MOM")

    assert row["mom_12_1"] == pytest.approx(80 / 50 - 1)  # 0.6
    assert row["mom_6_1"] == pytest.approx(80 / 60 - 1)  # 1/3
    assert row["rev_1m"] == pytest.approx(72 / 80 - 1)  # -0.1


def test_volatility_scaled_momentum_divides_by_the_daily_volatility(tmp_path):
    root = Lake(tmp_path).trades("MOM", stepped(YEAR)).listed("MOM").land()

    row = raw(features.compute(universe_of("MOM"), root), "MOM")

    # The window is every weekday after 2025-08-29, the month t-12 mark. Three of them move the
    # price: up a fifth into September, up a third into March, down a tenth into August.
    days = sum(day > date(2025, 8, 29) for day in YEAR)
    assert days == 261
    vol = statistics.stdev([0.2, 1 / 3, -0.1] + [0.0] * (days - 3))
    assert row["vol_12m"] == pytest.approx(vol)
    assert row["mom_12_1_volscaled"] == pytest.approx(0.6 / vol)


def test_a_mark_is_the_last_close_in_its_month(tmp_path):
    # MOM stops printing on 2026-07-29, two sessions before the month's last; its July mark is
    # that close, not a missing value and not August's.
    prices = stepped(YEAR)
    prices[date(2026, 7, 29)] = 90.0
    for gone in (date(2026, 7, 30), date(2026, 7, 31)):
        del prices[gone]
    root = Lake(tmp_path).trades("MOM", prices).trades("ALL", flat(YEAR)).listed("MOM").land()

    row = raw(features.compute(universe_of("MOM"), root), "MOM")

    assert row["mom_12_1"] == pytest.approx(90 / 50 - 1)
    assert row["rev_1m"] == pytest.approx(72 / 90 - 1)


def test_a_young_listing_has_no_long_momentum_and_no_risk(tmp_path):
    # Listed in June: a month t-1 mark but no t-6 or t-12, and 60-odd daily returns.
    young = weekdays(date(2026, 6, 1), SESSION)
    root = (
        Lake(tmp_path)
        .trades("OLD", stepped(YEAR))
        .trades("NEW", {day: 30.0 if day.month < 8 else 33.0 for day in young})
        .listed("OLD")
        .listed("NEW")
        .land()
    )

    row = raw(features.compute(universe_of("OLD", "NEW"), root), "NEW")

    assert row["rev_1m"] == pytest.approx(0.1)
    for name in ("mom_12_1", "mom_6_1", "mom_12_1_volscaled", "vol_12m", "beta_12m", "ivol_12m"):
        assert row[name] is None, name


def test_momentum_ignores_a_later_dividend_adjustment(tmp_path):
    # The vendor scales every close before a dividend by the same factor, today. Ratios of two
    # adjusted closes are untouched by it, however far after the rebalance date it happened.
    scaled = {day: price * 0.97 for day, price in stepped(YEAR).items()}
    root = Lake(tmp_path).trades("MOM", scaled).listed("MOM").land()

    row = raw(features.compute(universe_of("MOM"), root), "MOM")

    assert row["mom_12_1"] == pytest.approx(0.6)
    assert row["rev_1m"] == pytest.approx(-0.1)


# Risk -------------------------------------------------------------------------------------

# 129 sessions give 128 daily returns: past MIN_RETURNS, and a multiple of four.
RISK_SESSIONS = weekdays(date(2026, 3, 4), SESSION)
A, B, C = 0.01, 0.02, 0.03


def path(returns, start=100.0):
    """A price path whose daily returns are exactly `returns`, over RISK_SESSIONS."""
    prices, price = {RISK_SESSIONS[0]: start}, start
    for day, r in zip(RISK_SESSIONS[1:], returns, strict=True):
        price *= 1 + r
        prices[day] = price
    return prices


def test_risk_features_match_a_closed_form_market_model(tmp_path):
    # u alternates +1, -1; v runs +1, +1, -1, -1. Over 128 days both average zero and are
    # uncorrelated, so with names moving a*u, b*u and c*v the equal-weighted market is
    # ((a + b) u + c v) / 3 and every regression has a closed form. With k = (a + b)^2 + c^2:
    #   vol_i  = s_i * sqrt(n / (n - 1))
    #   beta_A = 3a(a + b) / k,  beta_B = 3b(a + b) / k,  beta_C = 3c^2 / k
    #   ivol_A = a c sqrt(n / ((n - 2) k)),  ivol_B = b c ..., ivol_C = c (a + b) ...
    assert len(RISK_SESSIONS) == 129
    n = 128
    u = [(-1) ** i for i in range(n)]
    v = [1 if i % 4 < 2 else -1 for i in range(n)]
    root = (
        Lake(tmp_path)
        .trades("A", path([A * x for x in u]))
        .trades("B", path([B * x for x in u]))
        .trades("C", path([C * x for x in v]))
        .listed("A")
        .listed("B")
        .listed("C")
        .land()
    )

    result = features.compute(universe_of("A", "B", "C"), root)

    k = (A + B) ** 2 + C**2  # 0.0018
    scale = math.sqrt(n / ((n - 2) * k))
    expected = {
        "A": (A * math.sqrt(n / (n - 1)), 0.5, A * C * scale),
        "B": (B * math.sqrt(n / (n - 1)), 1.0, B * C * scale),
        "C": (C * math.sqrt(n / (n - 1)), 1.5, C * (A + B) * scale),
    }
    for ticker, (vol, beta, ivol) in expected.items():
        row = raw(result, ticker)
        assert row["vol_12m"] == pytest.approx(vol, rel=1e-9), ticker
        assert row["beta_12m"] == pytest.approx(beta, rel=1e-9), ticker
        assert row["ivol_12m"] == pytest.approx(ivol, rel=1e-9), ticker


def test_risk_needs_min_returns_daily_returns(tmp_path):
    short = RISK_SESSIONS[-features.MIN_RETURNS :]  # MIN_RETURNS closes, one return fewer
    enough = RISK_SESSIONS[-features.MIN_RETURNS - 1 :]
    root = (
        Lake(tmp_path)
        .trades("SHORT", {day: 100.0 + i % 2 for i, day in enumerate(short)})
        .trades("ENOUGH", {day: 100.0 + i % 2 for i, day in enumerate(enough)})
        .listed("SHORT")
        .listed("ENOUGH")
        .land()
    )

    result = features.compute(universe_of("SHORT", "ENOUGH"), root)

    assert raw(result, "SHORT")["vol_12m"] is None
    assert raw(result, "ENOUGH")["vol_12m"] is not None


# Size and liquidity -----------------------------------------------------------------------

LIQUIDITY = weekdays(date(2026, 6, 1), SESSION)[-61:]  # 60 returns


def test_amihud_is_the_mean_price_impact_per_dollar_traded(tmp_path):
    # The close alternates 110 and 100 on a million shares: +10% on $110M traded, then -1/11 on
    # $100M. Both days move the price 1/1.1e9 per dollar, so that is the mean.
    prices = {day: 100.0 if i % 2 == 0 else 110.0 for i, day in enumerate(LIQUIDITY)}
    root = Lake(tmp_path).trades("LIQ", prices).listed("LIQ").land()

    row = raw(features.compute(universe_of("LIQ"), root), "LIQ")

    assert row["amihud"] == pytest.approx(1 / 1.1e9)


def test_amihud_reads_only_the_last_sixty_sessions(tmp_path):
    older = weekdays(date(2026, 3, 2), LIQUIDITY[0] - timedelta(days=1))
    prices = {day: 100.0 + 50 * (i % 2) for i, day in enumerate(older)}  # wild swings
    prices |= {day: 100.0 if i % 2 == 0 else 110.0 for i, day in enumerate(LIQUIDITY)}
    root = Lake(tmp_path).trades("LIQ", prices).listed("LIQ").land()

    row = raw(features.compute(universe_of("LIQ"), root), "LIQ")

    assert row["amihud"] == pytest.approx(1 / 1.1e9)


def test_turnover_is_daily_volume_over_shares_outstanding(tmp_path):
    # A million shares a day of a $10B company trading at $100: 100M shares, 1% a day.
    root = Lake(tmp_path).trades("TRN", flat(LIQUIDITY)).listed("TRN").land()

    row = raw(features.compute(universe_of("TRN", market_cap=1e10), root), "TRN")

    assert row["turnover"] == pytest.approx(0.01)
    assert row["log_mktcap"] == pytest.approx(math.log(1e10))


def test_a_split_after_the_rebalance_date_changes_neither_turnover_nor_amihud(tmp_path):
    # The same name, split two-for-one after the snapshot: the vendor halves every earlier
    # split-adjusted close and doubles every earlier volume. The as-traded close, and the
    # market cap the universe built from it, are what they were on the day.
    prices = {day: 100.0 if i % 2 == 0 else 110.0 for i, day in enumerate(LIQUIDITY)}
    halved = {day: price / 2 for day, price in prices.items()}
    split = Lake(tmp_path / "split")
    for day, price in prices.items():
        split.trades("SPL", {day: halved[day]}, close=halved[day], unadj=price, volume=2e6)
    split_root = split.listed("SPL").land()
    whole_root = Lake(tmp_path / "whole").trades("SPL", prices).listed("SPL").land()

    after = raw(features.compute(universe_of("SPL", market_cap=1e10), split_root), "SPL")
    before = raw(features.compute(universe_of("SPL", market_cap=1e10), whole_root), "SPL")

    assert after["turnover"] == pytest.approx(before["turnover"]) == pytest.approx(0.01)
    assert after["amihud"] == pytest.approx(before["amihud"])


# Value and quality ------------------------------------------------------------------------


def value_lake(root, *statements, prices=None):
    """A lake with one name, VAL, trading flat, and the given (calendardate, datekey, figures)
    ART filings."""
    built = Lake(root).trades("VAL", prices or flat(LIQUIDITY)).listed("VAL")
    for calendardate, datekey, figures in statements:
        built.files("VAL", calendardate=calendardate, datekey=datekey, **figures)
    return built.land()


FILED = (date(2026, 6, 30), date(2026, 8, 10), STATEMENTS)
FILED_YEAR_AGO = (date(2025, 6, 30), date(2025, 8, 8), YEAR_AGO)


def test_value_and_quality_ratios_from_the_latest_filing(tmp_path):
    root = value_lake(tmp_path, FILED, FILED_YEAR_AGO)

    row = raw(features.compute(universe_of("VAL", market_cap=1e9), root), "VAL")

    assert row["earnings_yield"] == pytest.approx(0.05)  # 50M / 1B
    assert row["book_to_price"] == pytest.approx(0.6)  # 600M / 1B
    assert row["fcf_yield"] == pytest.approx(40 / 1_300)  # 40M / (1B + 400M debt - 100M cash)
    assert row["sales_to_price"] == pytest.approx(0.8)  # 800M / 1B
    assert row["gross_profitability"] == pytest.approx(0.2)  # 300M / 1.5B
    assert row["roe"] == pytest.approx(50 / 600)
    assert row["debt_to_equity"] == pytest.approx(400 / 600)
    assert row["asset_growth"] == pytest.approx(0.2)  # 1.5B / 1.25B - 1
    # Net operating assets are operating assets less operating liabilities:
    #   now      (1,500 - 100) - (900 - 400) = 900
    #   year ago (1,250 -  50) - (800 - 350) = 750
    # and Sloan scales the change by average total assets, (1,500 + 1,250) / 2 = 1,375.
    assert row["accruals"] == pytest.approx(150 / 1_375)


def test_no_fundamental_is_visible_before_its_datekey(tmp_path):
    # The June quarter is filed on the rebalance date and restated the day after it. The
    # rebalance sees the filing, and not the restatement.
    restated = STATEMENTS | {"netinc": 90e6}
    root = value_lake(
        tmp_path,
        (date(2026, 3, 31), date(2026, 5, 8), STATEMENTS | {"netinc": 10e6}),
        (date(2026, 6, 30), SESSION, STATEMENTS),
        (date(2026, 6, 30), SESSION + timedelta(days=1), restated),
        (date(2026, 9, 30), SESSION + timedelta(days=40), restated),
    )

    on_the_day = raw(features.compute(universe_of("VAL"), root), "VAL")
    the_day_before = raw(
        features.compute(
            universe_of("VAL", as_of=date(2026, 8, 30), session=date(2026, 8, 28)), root
        ),
        "VAL",
    )

    assert on_the_day["earnings_yield"] == pytest.approx(0.05)
    assert the_day_before["earnings_yield"] == pytest.approx(0.01)  # only March is filed yet


def test_a_filing_restated_later_is_read_as_first_filed_before_then(tmp_path):
    # The vendor corrects June's figures in place on the 20th: same filing, a later
    # `lastupdated`. A rebalance before the 20th reads the figures as they stood.
    built = Lake(tmp_path).trades("VAL", flat(LIQUIDITY)).listed("VAL")
    june, filed, corrected = date(2026, 6, 30), date(2026, 8, 10), date(2026, 8, 20)
    built.files("VAL", calendardate=june, datekey=filed, lastupdated=filed, **STATEMENTS)
    built.files(
        "VAL", calendardate=june, datekey=filed, lastupdated=corrected,
        **STATEMENTS | {"netinc": 90e6},
    )  # fmt: skip
    root = built.land()
    before = date(2026, 8, 19)

    then = raw(features.compute(universe_of("VAL", as_of=before, session=before), root), "VAL")
    now = raw(features.compute(universe_of("VAL"), root), "VAL")

    assert then["earnings_yield"] == pytest.approx(0.05)
    assert now["earnings_yield"] == pytest.approx(0.09)


def test_a_year_ago_filing_counts_only_once_filed(tmp_path):
    late = (date(2025, 6, 30), SESSION + timedelta(days=1), YEAR_AGO)
    root = value_lake(tmp_path, FILED, late)

    row = raw(features.compute(universe_of("VAL"), root), "VAL")

    assert row["asset_growth"] is None
    assert row["accruals"] is None
    assert row["earnings_yield"] == pytest.approx(0.05)


def test_an_amended_old_period_does_not_displace_a_newer_one(tmp_path):
    # The March quarter is amended after June's was filed. June is still the latest period.
    amended_march = (date(2026, 3, 31), date(2026, 8, 20), STATEMENTS | {"netinc": 10e6})
    root = value_lake(tmp_path, FILED, amended_march)

    row = raw(features.compute(universe_of("VAL"), root), "VAL")

    assert row["earnings_yield"] == pytest.approx(0.05)


def test_an_amendment_of_the_latest_period_replaces_it_once_filed(tmp_path):
    amended = (date(2026, 6, 30), date(2026, 8, 20), STATEMENTS | {"netinc": 70e6})
    root = value_lake(tmp_path, FILED, amended)

    row = raw(features.compute(universe_of("VAL"), root), "VAL")

    assert row["earnings_yield"] == pytest.approx(0.07)


def test_only_as_reported_trailing_figures_are_read(tmp_path):
    # MRT is the vendor's restated series. It never reaches a feature, even filed on time.
    restated = (date(2026, 6, 30), date(2026, 8, 11), STATEMENTS | {"netinc": 90e6})
    built = Lake(tmp_path).trades("VAL", flat(LIQUIDITY)).listed("VAL")
    built.files("VAL", calendardate=FILED[0], datekey=FILED[1], **STATEMENTS)
    built.files(
        "VAL", calendardate=restated[0], datekey=restated[1], dimension="MRT", **restated[2]
    )
    root = built.land()

    row = raw(features.compute(universe_of("VAL"), root), "VAL")

    assert row["earnings_yield"] == pytest.approx(0.05)


@pytest.mark.parametrize(
    "figures, missing, present",
    [
        pytest.param(
            {"equity": -100e6},
            ("roe", "debt_to_equity"),
            ("book_to_price",),
            id="negative equity",
        ),
        pytest.param({"equity": 0.0}, ("roe", "debt_to_equity"), (), id="zero equity"),
        pytest.param(
            {"debt": 0.0, "cashneq": 2_000e6}, ("fcf_yield",), ("earnings_yield",), id="EV below 0"
        ),
        pytest.param({"assets": 0.0}, ("gross_profitability",), (), id="no assets"),
    ],
)
def test_a_meaningless_denominator_leaves_the_ratio_missing(tmp_path, figures, missing, present):
    root = value_lake(tmp_path, (FILED[0], FILED[1], STATEMENTS | figures))

    row = raw(features.compute(universe_of("VAL"), root), "VAL")

    for name in missing:
        assert row[name] is None, name
    for name in present:
        assert row[name] is not None, name


def test_a_name_with_no_filing_has_no_fundamental_features(tmp_path):
    built = Lake(tmp_path).trades("VAL", flat(LIQUIDITY)).trades("OTHER", flat(LIQUIDITY))
    built.files("OTHER", calendardate=FILED[0], datekey=FILED[1], **STATEMENTS)
    root = built.listed("VAL").listed("OTHER").land()

    result = features.compute(universe_of("VAL", "OTHER"), root)

    row = raw(result, "VAL")
    assert row["earnings_yield"] is None
    assert row["accruals"] is None
    assert row["log_mktcap"] == pytest.approx(math.log(1e9))


# Ranking ----------------------------------------------------------------------------------


def test_percentile_rank_maps_order_onto_zero_to_one():
    assert features.percentile_rank([3.0, 1.0, 2.0]) == [1.0, 0.0, 0.5]
    assert features.percentile_rank([-5.0, 10.0]) == [0.0, 1.0]


def test_percentile_rank_gives_ties_the_mean_of_their_places():
    assert features.percentile_rank([1.0, 2.0, 2.0, 3.0]) == [0.0, 0.5, 0.5, 1.0]
    assert features.percentile_rank([7.0, 7.0, 7.0]) == [0.5, 0.5, 0.5]


def test_percentile_rank_leaves_missing_values_missing():
    assert features.percentile_rank([2.0, None, math.nan, 1.0]) == [1.0, None, None, 0.0]
    assert features.percentile_rank([4.0]) == [0.5]
    assert features.percentile_rank([None, None]) == [None, None]
    assert features.percentile_rank([]) == []


def test_percentile_rank_is_bounded_and_rank_preserving():
    rng = random.Random(29)
    for _ in range(50):
        values = [rng.choice([None, rng.randint(-5, 5), rng.gauss(0, 1e6)]) for _ in range(40)]
        ranks = features.percentile_rank(values)
        pairs = [(v, r) for v, r in zip(values, ranks, strict=True) if v is not None]
        assert all(0.0 <= r <= 1.0 for _, r in pairs)
        for v1, r1 in pairs:
            for v2, r2 in pairs:
                if v1 < v2:
                    assert r1 < r2
                elif v1 == v2:
                    assert r1 == r2


def cross_section(root, count=12):
    """A universe of `count` names whose every feature differs, with one name, N0, missing
    all of its fundamentals and one sector unknown to the vocabulary."""
    built = Lake(root)
    for i in range(count):
        ticker = f"N{i}"
        prices = {
            day: (50.0 + i) * (1 + 0.001 * i * ((-1) ** j) + 0.0005 * j * (i % 3))
            for j, day in enumerate(YEAR)
        }
        built.trades(ticker, prices, volume=1e6 * (1 + i % 5))
        built.listed(ticker, features.SECTORS[i % 4] if i != 7 else "Shell Companies")
        if i:
            # Each figure scaled by its own factor, so that every ratio of two differs by name.
            scaled = {
                key: value * (1 + 0.1 * ((i * (j + 2)) % 11))
                for j, (key, value) in enumerate(STATEMENTS.items())
            }
            built.files(ticker, calendardate=FILED[0], datekey=FILED[1], **scaled)
            built.files(
                ticker, calendardate=FILED_YEAR_AGO[0], datekey=FILED_YEAR_AGO[1], **YEAR_AGO
            )
    built.trades("OUTSIDER", flat(YEAR)).listed("OUTSIDER")
    built.files(
        "OUTSIDER", calendardate=FILED[0], datekey=FILED[1], **STATEMENTS | {"netinc": 1e12}
    )
    members = tuple(Member(f"N{i}", 100.0, 1e8, 1e9 * (1 + (i * 5) % count)) for i in range(count))
    return features.compute(Universe(SESSION, SESSION, "", members), built.land())


def test_ranks_are_computed_within_the_cross_section(tmp_path):
    result = cross_section(tmp_path)

    assert result.ranked.column_names == ["ticker", "sector", *features.NUMERIC]
    assert result.ranked.column("ticker").to_pylist() == result.raw.column("ticker").to_pylist()
    for name in features.NUMERIC:
        values = result.raw.column(name).to_pylist()
        ranks = result.ranked.column(name).to_pylist()
        assert ranks == features.percentile_rank(values), name
        pairs = [(v, r) for v, r in zip(values, ranks, strict=True) if v is not None]
        assert len(pairs) > 1, name
        assert all(0.0 <= r <= 1.0 for _, r in pairs), name
        for (v1, r1), (v2, r2) in itertools.combinations(pairs, 2):
            assert (v1 > v2) - (v1 < v2) == (r1 > r2) - (r1 < r2), name
    assert result.ranked.column("earnings_yield")[0].as_py() is None  # N0 never filed
    # OUTSIDER, in the lake but not the universe, earns far more than any member. It takes no
    # place in the ranking, so the best member's earnings yield still ranks at the top.
    ranks = result.ranked.column("earnings_yield").to_pylist()
    values = result.raw.column("earnings_yield").to_pylist()
    best = max((v, i) for i, v in enumerate(values) if v is not None)[1]
    assert ranks[best] == 1.0
    assert "OUTSIDER" not in result.tickers


def test_sector_is_a_fixed_categorical_and_is_not_ranked(tmp_path):
    result = cross_section(tmp_path)

    sector = result.ranked.column("sector")
    assert sector.type == features.SECTOR_TYPE
    assert sector.combine_chunks().dictionary.to_pylist() == list(features.SECTORS)
    assert "sector" not in features.NUMERIC
    assert result.ranked.column("sector").equals(result.raw.column("sector"))
    by_ticker = dict(zip(result.tickers, sector.to_pylist(), strict=True))
    assert by_ticker["N1"] == features.SECTORS[1]
    assert by_ticker["N7"] is None  # outside the vocabulary


def test_sector_codes_are_the_same_whichever_sectors_a_month_holds():
    one = features.sector_array(["Utilities"])
    other = features.sector_array(["Energy", "Utilities", "", None])

    assert one.indices[0].as_py() == other.indices[1].as_py() == len(features.SECTORS) - 1
    assert other.to_pylist() == ["Energy", "Utilities", None, None]


def test_the_features_are_the_twenty_the_spec_names():
    assert len(features.FEATURES) == 20
    assert features.FEATURES[-1] == "sector"
    assert set(features.EXPECTED_SIGN) == set(features.NUMERIC)
    assert set(features.EXPECTED_SIGN.values()) == {-1, 1}


# The universe, a run of months, and the lake's shape ---------------------------------------


def listed_lake(root):
    """Three names that clear the universe's filters, with prices, shares and statements."""
    built = Lake(root)
    for i, ticker in enumerate(("AAA", "BBB", "CCC")):
        built.trades(ticker, {day: 20.0 + i + (day.day % 3) for day in YEAR}, volume=1e6)
        built.files(
            ticker,
            calendardate=date(2025, 12, 31),
            datekey=date(2026, 2, 20),
            dimension="ARQ",
            shares=100e6,
        )
        built.files(ticker, calendardate=FILED[0], datekey=FILED[1], **STATEMENTS)
        built.listed(ticker, "Energy")
    return built.land()


def test_features_cover_exactly_the_universe_snapshot(tmp_path):
    root = listed_lake(tmp_path)
    universe = snapshot(SESSION, root)

    result = features.compute(universe, root)

    assert result.tickers == universe.tickers == ("AAA", "BBB", "CCC")
    assert (result.as_of, result.snapshot_date) == (SESSION, SESSION)
    assert result.partition_hash == universe.partition_hash
    assert raw(result, "BBB")["earnings_yield"] == pytest.approx(
        50e6 / universe.members[1].market_cap
    )


def test_history_walks_the_month_ends(tmp_path):
    root = listed_lake(tmp_path)

    months = list(features.history(date(2026, 6, 1), SESSION, root))

    assert [m.as_of for m in months] == [date(2026, 6, 30), date(2026, 7, 31), SESSION]
    assert all(m.tickers == ("AAA", "BBB", "CCC") for m in months)
    # June's universe predates the June-quarter filing, so it has no earnings yet.
    assert raw(months[0], "AAA")["earnings_yield"] is None
    assert raw(months[2], "AAA")["earnings_yield"] is not None


def test_an_empty_universe_has_empty_features(tmp_path):
    root = listed_lake(tmp_path)

    result = features.compute(universe_of(), root)

    assert result.raw.num_rows == result.ranked.num_rows == 0
    assert result.ranked.column_names == ["ticker", "sector", *features.NUMERIC]


def test_a_lake_missing_a_column_is_refused_by_name(tmp_path):
    root = Lake(tmp_path).trades("X", flat(LIQUIDITY)).listed("X").land(without=("gp",))

    with pytest.raises(features.FeatureError, match=r"sf1 lacks \['gp'\]"):
        features.compute(universe_of("X"), root)


def test_features_import_only_what_the_base_image_installs():
    # Live and research both run agb.features; only the base target's packages are in both.
    source = REPO / "src" / "agb" / "features.py"
    assert Path(features.__file__).resolve() == source
    project = tomllib.loads((REPO / "pyproject.toml").read_text())["project"]
    base = {
        dep.split("[")[0].split(">")[0].split("=")[0].replace("-", "_").strip()
        for dep in project["dependencies"]
    }
    imported = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module.split(".")[0])

    assert imported - sys.stdlib_module_names - {"agb"} <= base


# The sanity report ------------------------------------------------------------------------


def report_lake(root, through):
    """Three names over July and August, with prices chosen so the forward month's returns
    are +10%, 0% and -10% from the first August session to the first September one."""
    sessions = weekdays(date(2026, 6, 1), through)
    built = Lake(root)
    for ticker, move in (("UP", 1.1), ("FLAT", 1.0), ("DOWN", 0.9)):
        built.trades(
            ticker, {day: 50.0 if day < date(2026, 9, 1) else 50.0 * move for day in sessions}
        )
        built.listed(ticker)
    return built.land()


def test_forward_returns_run_from_the_session_after_to_the_session_after_next(tmp_path):
    root = report_lake(tmp_path, date(2026, 9, 1))
    july = features.compute(
        universe_of("UP", "FLAT", "DOWN", as_of=date(2026, 7, 31), session=date(2026, 7, 31)),
        root,
    )

    returns = features.forward_returns(july, root)

    assert returns == pytest.approx({"UP": 0.1, "FLAT": 0.0, "DOWN": -0.1})


def test_forward_returns_wait_for_the_exit_session(tmp_path):
    root = report_lake(tmp_path, SESSION)
    july = features.compute(
        universe_of("UP", as_of=date(2026, 7, 31), session=date(2026, 7, 31)), root
    )

    assert features.forward_returns(july, root) is None


def test_information_coefficients_are_spearman_correlations():
    tickers = ["A", "B", "C", "D"]
    columns = {name: [None] * 4 for name in features.NUMERIC}
    columns["mom_12_1"] = [1.0, 2.0, 3.0, 4.0]
    columns["rev_1m"] = [4.0, 3.0, 100.0, 1.0]
    table = pa.table(
        {
            "ticker": tickers,
            "sector": features.sector_array([None] * 4),
            **{name: pa.array(values, pa.float64()) for name, values in columns.items()},
        }
    )
    month = features.Features(SESSION, SESSION, "", table, table)

    ics = features.information_coefficients(month, {"A": 0.01, "B": 0.02, "C": 0.03, "D": 0.5})

    assert ics["mom_12_1"] == pytest.approx(1.0)
    assert ics["rev_1m"] == pytest.approx(-0.4)  # ranks 3,2,4,1 against 1,2,3,4
    assert ics["roe"] is None


def test_the_report_summarises_months_against_the_expected_sign():
    months = [{"mom_12_1": 0.1, "rev_1m": 0.1}, {"mom_12_1": 0.3, "rev_1m": 0.3}, {}]

    lines = {line.feature: line for line in features.summarise(months)}

    momentum = lines["mom_12_1"]
    assert (momentum.months, momentum.mean_ic) == (2, pytest.approx(0.2))
    assert momentum.t_stat == pytest.approx(0.2 / (statistics.stdev([0.1, 0.3]) / math.sqrt(2)))
    assert (momentum.hit_rate, momentum.as_expected) == (1.0, True)
    reversal = lines["rev_1m"]
    assert (reversal.hit_rate, reversal.as_expected) == (0.0, False)
    assert (lines["roe"].months, lines["roe"].mean_ic, lines["roe"].as_expected) == (0, None, None)


# The lake's reads: the same answers, with fewer files opened and the filings ranked once ------


def legacy_connect(root):
    """The connection `lake.connect` built before it pruned `sep` by date and kept the SF1 versions
    in a table: every view reads the whole of its dataset and ranks the filings per query."""
    conn = duckdb.connect()
    period = ", ".join(lake.SF1.primary_key)
    sep = f"read_parquet('{root}/sep/date=*/part.parquet', hive_partitioning = false)"
    sf1 = f"read_parquet('{root}/sf1/lastupdated=*/part.parquet', hive_partitioning = false)"
    newest = sorted(root.glob("tickers/snapshot=*/part.parquet"))[-1]
    conn.execute(f"CREATE VIEW sep AS SELECT * FROM {sep}")
    conn.execute(
        f"CREATE VIEW sf1 AS SELECT * FROM {sf1} "
        f"QUALIFY row_number() OVER (PARTITION BY {period} ORDER BY lastupdated DESC) = 1"
    )
    conn.execute(f"""
        CREATE MACRO sf1_as_of(as_of) AS TABLE
        SELECT * FROM {sf1}
        WHERE datekey <= as_of
        QUALIFY row_number() OVER (
            PARTITION BY {period}
            ORDER BY CASE WHEN lastupdated <= as_of THEN lastupdated END DESC NULLS LAST,
                     lastupdated
        ) = 1
    """)
    conn.execute(f"CREATE VIEW tickers AS SELECT * FROM read_parquet('{newest}')")
    return conn


def restated_lake(root):
    """Six names over a year of sessions, each filing a quarter and then restating it in a later
    version, so that the month-ends before and after the restatement read different figures."""
    built = Lake(root)
    for i, ticker in enumerate(("AAA", "BBB", "CCC", "DDD", "EEE", "FFF")):
        built.trades(
            ticker,
            {day: 20.0 + i + (day.day % 5) * 0.25 + day.month * 0.1 * i for day in YEAR},
            volume=1e6 * (1 + i),
        )
        built.listed(ticker, features.SECTORS[i % 4])
        built.files(
            ticker, calendardate=date(2025, 12, 31), datekey=date(2026, 2, 20),
            dimension="ARQ", shares=100e6 + i * 1e6, lastupdated=date(2026, 2, 20),
        )  # fmt: skip
        built.files(
            ticker, calendardate=date(2025, 6, 30), datekey=date(2025, 8, 8),
            lastupdated=date(2025, 8, 8), **YEAR_AGO,
        )  # fmt: skip
        built.files(
            ticker, calendardate=FILED[0], datekey=FILED[1], lastupdated=FILED[1],
            **STATEMENTS | {"netinc": (3 + i) * 1e7},
        )  # fmt: skip
        if i % 2:  # the vendor restates the quarter a fortnight later
            built.files(
                ticker, calendardate=FILED[0], datekey=FILED[1], lastupdated=date(2026, 8, 24),
                **STATEMENTS | {"netinc": (9 + i) * 1e7},
            )  # fmt: skip
    return built.land()


def digest(result):
    """A SHA-256 of a month's features as serialised, so equal digests mean equal bytes."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, result.raw.schema) as writer:
        writer.write_table(result.raw)
    with pa.ipc.new_stream(sink, result.ranked.schema) as writer:
        writer.write_table(result.ranked)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def test_features_are_byte_identical_to_the_unpruned_reads(tmp_path):
    root = restated_lake(tmp_path)
    start, end = date(2025, 10, 1), SESSION

    with lake.connect(root) as pruned, legacy_connect(root) as full:
        now = list(features.history(start, end, root, db=pruned))
        before = list(features.history(start, end, root, db=full))

    assert len(now) == len(before) == 11
    assert all(month.raw.num_rows == 6 for month in now[-2:])
    for new, old in zip(now, before, strict=True):
        assert new.raw.equals(old.raw)
        assert new.ranked.equals(old.ranked)
        assert (new.as_of, new.snapshot_date, new.partition_hash) == (
            old.as_of,
            old.snapshot_date,
            old.partition_hash,
        )
        assert digest(new) == digest(old)
    # The restatement changes the August month-end, so the equality above is not vacuous.
    assert raw(now[-1], "BBB")["earnings_yield"] != raw(now[-2], "BBB")["earnings_yield"]


def test_the_views_answer_every_query_as_the_unpruned_ones_did(tmp_path):
    root = restated_lake(tmp_path)
    queries = [
        "SELECT * FROM sep ORDER BY ALL",
        "SELECT * FROM sf1 ORDER BY ALL",
        "SELECT * FROM tickers ORDER BY ALL",
        "SELECT * FROM sep WHERE date BETWEEN DATE '2026-03-02' AND DATE '2026-03-13' ORDER BY ALL",
        "SELECT * FROM sep WHERE date > DATE '2026-08-20' ORDER BY ALL",
        "SELECT ticker, min(date), max(date), count(*) FROM sep GROUP BY ticker ORDER BY ticker",
    ]
    days = [date(2025, 7, 1), date(2026, 2, 20), date(2026, 8, 10), date(2026, 8, 23), SESSION]
    queries += [f"SELECT * FROM sf1_as_of(DATE '{day}') ORDER BY ALL" for day in days]

    with lake.connect(root) as pruned, legacy_connect(root) as full:
        for query in queries:
            relation = pruned.sql(query)
            assert relation.fetchall() == full.sql(query).fetchall(), query
            assert relation.columns == full.sql(query).columns, query
            assert relation.types == full.sql(query).types, query


def files_read(conn, query, *params):
    """How many of `sep`'s partition files DuckDB opens to answer `query`."""
    plan = "\n".join(row[1] for row in conn.execute(f"EXPLAIN ANALYZE {query}", params).fetchall())
    (count,) = re.findall(r"Total Files Read: (\d+)", plan)
    return int(count)


def test_a_date_filtered_read_of_sep_opens_only_the_sessions_it_asks_for(tmp_path):
    root = restated_lake(tmp_path)
    sessions = len(list(root.glob("sep/date=*")))
    day = date(2026, 3, 2)
    conn = lake.connect(root)

    assert sessions > 200
    assert files_read(conn, "SELECT * FROM sep") == sessions
    assert files_read(conn, "SELECT * FROM sep WHERE date = ?", day) == 1
    assert (
        files_read(conn, "SELECT * FROM sep WHERE date BETWEEN ? AND ?", day, date(2026, 3, 6)) == 5
    )
    # The universe's recency window: the newest sessions on or before the rebalance date.
    recent = "SELECT DISTINCT date FROM sep WHERE date <= ? ORDER BY date DESC LIMIT 5"
    assert files_read(conn, recent, day) < sessions
    # `legacy_connect` opens every file whatever the filter, which is what this pruning replaces.
    assert files_read(legacy_connect(root), "SELECT * FROM sep WHERE date = ?", day) == sessions


def test_the_sf1_versions_are_read_from_the_lake_once_per_connection(tmp_path):
    root = restated_lake(tmp_path)
    conn = lake.connect(root)
    latest = conn.sql("SELECT * FROM sf1 ORDER BY ALL").fetchall()
    as_of = conn.sql("SELECT * FROM sf1_as_of(DATE '2026-08-23') ORDER BY ALL").fetchall()

    shutil.rmtree(root / "sf1")  # nothing of SF1 is left to read

    assert conn.sql("SELECT * FROM sf1 ORDER BY ALL").fetchall() == latest
    assert conn.sql("SELECT * FROM sf1_as_of(DATE '2026-08-23') ORDER BY ALL").fetchall() == as_of
    assert len(latest) == 18 and len(as_of) == 18
    assert "_latest" not in conn.sql("SELECT * FROM sf1").columns
