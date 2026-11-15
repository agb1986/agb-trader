"""The twenty stage-1 features: for every name in a universe snapshot, what the ranker knows
about it on the rebalance date, computed from nothing the pipeline could not have seen then.

Research and live both import `compute`, so a backtest ranks exactly the numbers the live
pipeline would have ranked. The definitions are the ones in
docs/architecture/02-classical-ranker.md:

    momentum            mom_12_1, mom_6_1, rev_1m, mom_12_1_volscaled
    value               earnings_yield, book_to_price, fcf_yield, sales_to_price
    quality             gross_profitability, roe, accruals, debt_to_equity, asset_growth
    risk                vol_12m, beta_12m, ivol_12m
    size and liquidity  log_mktcap, amihud, turnover
    context             sector

Each numeric feature is ranked within its own date's cross-section and mapped onto [0, 1], so
the model sees where a name stands among the names it could have bought that month, never a raw
ratio whose tails swamp the rest. `sector` is not ranked: it is carried as a categorical with a
fixed vocabulary, so a sector's code is the same in every month, in research and in live.

What keeps the numbers point-in-time:

  * **Fundamentals are gated on `datekey`**, SF1's SEC filing date, exactly as the universe is.
    Each name's figures come from its latest period filed on or before the rebalance date, and
    within that period from the latest filing, so an amendment counts only once it was filed.
    Only ART, the as-reported trailing twelve months, is read: its flows are the TTM figures the
    ratios want, and its balance sheet is the one filed for the period. The year-ago figures that
    `accruals` and `asset_growth` compare against pass the same gate.
  * **Returns come from `closeadj`**, SEP's close adjusted for splits and dividends. The vendor
    adjusts backwards from today, so a later split or dividend rescales every earlier close by the
    same factor and cancels out of any ratio of two of them. Momentum and daily returns are
    ratios, so no event after the rebalance date can reach them.
  * **Volume is put on the rebalance session's share basis.** SEP's `volume` is adjusted for
    every split up to today, so a 2008 volume is inflated by the splits since. Scaling it by that
    session's `close` makes it a dollar amount, which splits do not change, and dividing by market
    cap -- as-traded price times as-reported shares, from the universe -- turns it back into a
    fraction of the shares outstanding. Turnover built straight from adjusted volume and
    as-reported shares would rank next decade's winners, the names that went on to split, as the
    most traded.

The windows are the lake's sessions, not calendar arithmetic. "Month t-k" is the calendar month
k months before the snapshot session's, and its price is the name's last close in that month on
or before the lake's last session in it. The risk features use daily returns after the month
t-12 session, and need MIN_RETURNS of them. The liquidity features use the same 60 sessions the
universe's dollar-volume screen does. The market in `beta_12m` and `ivol_12m` is the
equal-weighted daily return of the universe itself, the cross-section the ranker ranks against;
it is conditioned on the names that survived to the rebalance date, which shifts its mean but
barely moves co-movement with it, and co-movement is all the two features measure.

A ratio with a denominator that makes it meaningless is missing, not extreme: `roe` and
`debt_to_equity` need positive equity, `fcf_yield` positive enterprise value, and the asset
ratios positive assets. Missing values stay missing through the ranking, which LightGBM handles
natively.

    python -m agb.features show --as-of 2026-08-31      one month's features
    python -m agb.features history --out features.parquet  every month-end since 1998
    python -m agb.features report                       rank correlation with forward return
"""

import argparse
import math
import os
import statistics
import sys
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from agb import lake
from agb.universe import LIQUIDITY_SESSIONS, Universe, UniverseError, month_ends, snapshot

NUMERIC = (
    "mom_12_1",
    "mom_6_1",
    "rev_1m",
    "mom_12_1_volscaled",
    "earnings_yield",
    "book_to_price",
    "fcf_yield",
    "sales_to_price",
    "gross_profitability",
    "roe",
    "accruals",
    "debt_to_equity",
    "asset_growth",
    "vol_12m",
    "beta_12m",
    "ivol_12m",
    "log_mktcap",
    "amihud",
    "turnover",
)
CATEGORICAL = ("sector",)
FEATURES = NUMERIC + CATEGORICAL

# The sign of each feature's published relation with forward return, which the sanity report
# checks the lake against. debt_to_equity is scored as a quality measure, as the spec groups it;
# Bhandari (1988) found the opposite sign, so it is the one to read with most care.
EXPECTED_SIGN = {
    "mom_12_1": 1,  # Jegadeesh and Titman (1993)
    "mom_6_1": 1,
    "rev_1m": -1,  # short-term reversal, Jegadeesh (1990)
    "mom_12_1_volscaled": 1,
    "earnings_yield": 1,  # Basu (1977)
    "book_to_price": 1,  # Fama and French (1992)
    "fcf_yield": 1,
    "sales_to_price": 1,  # Barbee, Mukherji and Raines (1996)
    "gross_profitability": 1,  # Novy-Marx (2013)
    "roe": 1,
    "accruals": -1,  # Sloan (1996)
    "debt_to_equity": -1,
    "asset_growth": -1,  # Cooper, Gulen and Schill (2008)
    "vol_12m": -1,  # the low-volatility anomaly
    "beta_12m": -1,  # Frazzini and Pedersen (2014)
    "ivol_12m": -1,  # Ang, Hodrick, Xing and Zhang (2006)
    "log_mktcap": -1,  # Banz (1981)
    "amihud": 1,  # Amihud (2002)
    "turnover": -1,  # Datar, Naik and Radcliffe (1998)
}

# The vendor's sectors, in a fixed order so that a sector's categorical code never changes. A
# sector outside this list, or none at all, is carried as missing.
SECTORS = (
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
)
SECTOR_TYPE = pa.dictionary(pa.int8(), pa.string())

# Daily returns a name needs in its trailing year before its risk is measured. Half a year: a
# name listed last month has a volatility, but not one worth ranking against a year of history.
MIN_RETURNS = 126
MOMENTUM_MONTHS = (1, 6, 12)

_NEEDED = {
    "sep": ("ticker", "date", "close", "closeadj", "volume"),
    "sf1": (
        "ticker",
        "dimension",
        "calendardate",
        "datekey",
        "netinc",
        "revenue",
        "fcf",
        "gp",
        "assets",
        "equity",
        "debt",
        "cashneq",
        "liabilities",
    ),
    "tickers": ("table", "permaticker", "ticker", "sector"),
}

# The members table is registered on the connection under this name for the query's duration.
_MEMBERS = "_feature_members"

_QUERY = f"""
WITH prices AS (
    SELECT sep.ticker, sep.date, sep.close, sep.closeadj, sep.volume
    FROM sep JOIN {_MEMBERS} m USING (ticker)
    WHERE sep.date BETWEEN $first AND $session AND sep.closeadj > 0
),
daily AS (
    SELECT *, closeadj / lag(closeadj) OVER (PARTITION BY ticker ORDER BY date) - 1 AS r
    FROM prices
),
annual AS (SELECT ticker, date, r FROM daily WHERE date > $risk_start AND r IS NOT NULL),
market AS (SELECT date, avg(r) AS m FROM annual GROUP BY date),
risk AS (
    SELECT ticker,
           count(*) AS n,
           stddev_samp(r) AS vol,
           regr_slope(r, m) AS beta,
           -- The regression's standard error: the residual sum of squares over n - 2.
           sqrt(greatest(
               regr_syy(r, m) - regr_sxy(r, m) * regr_sxy(r, m) / nullif(regr_sxx(r, m), 0), 0
           ) / nullif(count(*) - 2, 0)) AS ivol
    FROM annual JOIN market USING (date)
    GROUP BY ticker
),
marks AS (
    SELECT ticker,
           arg_max(closeadj, date) FILTER (WHERE date = $session) AS p0,
           arg_max(close, date) FILTER (WHERE date = $session) AS c0,
           arg_max(closeadj, date) FILTER (WHERE date BETWEEN $from1 AND $ref1) AS p1,
           arg_max(closeadj, date) FILTER (WHERE date BETWEEN $from6 AND $ref6) AS p6,
           arg_max(closeadj, date) FILTER (WHERE date BETWEEN $from12 AND $ref12) AS p12
    FROM prices
    GROUP BY ticker
),
liquidity AS (
    SELECT ticker,
           avg(abs(r) / (close * volume)) FILTER (WHERE volume > 0 AND close > 0) AS amihud,
           avg(volume) AS volume
    FROM daily
    WHERE date >= $recent
    GROUP BY ticker
),
filed AS (
    SELECT sf1.ticker, calendardate, netinc, revenue, fcf, gp, assets, equity, debt, cashneq,
           liabilities
    FROM sf1_as_of($as_of) sf1 JOIN {_MEMBERS} m USING (ticker)
    WHERE dimension = 'ART'
    QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY calendardate DESC, datekey DESC) = 1
),
prior AS (
    SELECT sf1.ticker, sf1.assets, sf1.cashneq, sf1.liabilities, sf1.debt
    FROM sf1_as_of($as_of) sf1 JOIN filed
      ON sf1.ticker = filed.ticker
     AND sf1.calendardate = CAST(filed.calendardate - INTERVAL 1 YEAR AS DATE)
    WHERE sf1.dimension = 'ART'
    QUALIFY row_number() OVER (PARTITION BY sf1.ticker ORDER BY sf1.datekey DESC) = 1
),
listing AS (
    SELECT ticker, sector
    FROM tickers
    WHERE "table" = 'stocks'
    -- A reused symbol is judged by its newest holder, as the universe's common-stock test is.
    QUALIFY row_number() OVER (PARTITION BY ticker ORDER BY permaticker DESC) = 1
),
parts AS (
    SELECT m.ticker, m.market_cap, listing.sector,
           marks.p0, marks.c0, marks.p1, marks.p6, marks.p12,
           CASE WHEN risk.n >= $min_returns THEN risk.vol END AS vol,
           CASE WHEN risk.n >= $min_returns THEN risk.beta END AS beta,
           CASE WHEN risk.n >= $min_returns THEN risk.ivol END AS ivol,
           liquidity.amihud, liquidity.volume,
           f.netinc, f.revenue, f.fcf, f.gp, f.assets, f.equity, f.debt,
           m.market_cap + f.debt - f.cashneq AS ev,
           (f.assets - f.cashneq) - (f.liabilities - f.debt) AS noa,
           p.assets AS prior_assets,
           (p.assets - p.cashneq) - (p.liabilities - p.debt) AS prior_noa
    FROM {_MEMBERS} m
    LEFT JOIN marks USING (ticker)
    LEFT JOIN risk USING (ticker)
    LEFT JOIN liquidity USING (ticker)
    LEFT JOIN filed f USING (ticker)
    LEFT JOIN prior p USING (ticker)
    LEFT JOIN listing USING (ticker)
)
SELECT ticker, sector,
       p1 / p12 - 1 AS mom_12_1,
       p1 / p6 - 1 AS mom_6_1,
       p0 / p1 - 1 AS rev_1m,
       (p1 / p12 - 1) / nullif(vol, 0) AS mom_12_1_volscaled,
       netinc / market_cap AS earnings_yield,
       equity / market_cap AS book_to_price,
       CASE WHEN ev > 0 THEN fcf / ev END AS fcf_yield,
       revenue / market_cap AS sales_to_price,
       CASE WHEN assets > 0 THEN gp / assets END AS gross_profitability,
       CASE WHEN equity > 0 THEN netinc / equity END AS roe,
       CASE WHEN assets + prior_assets > 0
            THEN (noa - prior_noa) / ((assets + prior_assets) / 2) END AS accruals,
       CASE WHEN equity > 0 THEN debt / equity END AS debt_to_equity,
       CASE WHEN prior_assets > 0 THEN assets / prior_assets - 1 END AS asset_growth,
       vol AS vol_12m,
       beta AS beta_12m,
       ivol AS ivol_12m,
       ln(market_cap) AS log_mktcap,
       amihud,
       volume * c0 / market_cap AS turnover
FROM parts
ORDER BY ticker
"""

# The report's holding period: bought at the close of the session after the rebalance date and
# sold at the close of the session after the next one, as the live basket is.
_FORWARD = f"""
SELECT sep.ticker,
       arg_max(sep.closeadj, sep.date) FILTER (WHERE sep.date <= $exit)
         / arg_max(sep.closeadj, sep.date) FILTER (WHERE sep.date <= $entry) - 1
FROM sep JOIN {_MEMBERS} m USING (ticker)
WHERE sep.date BETWEEN $session AND $exit AND sep.closeadj > 0
GROUP BY sep.ticker
"""


class FeatureError(Exception):
    """Features that cannot be computed: the lake is missing a dataset or a column."""


@dataclass(frozen=True)
class Features:
    """One rebalance date's features, for every name in its universe, and where they came from.

    `ranked` is what the model reads: a row per name, `ticker`, `sector` as a categorical, and
    each numeric feature as its percentile within the date's cross-section. `raw` has the same
    shape with each feature's value before ranking, for inspection and for the tests."""

    as_of: date
    snapshot_date: date
    partition_hash: str
    raw: pa.Table
    ranked: pa.Table

    @property
    def tickers(self) -> tuple[str, ...]:
        return tuple(self.raw.column("ticker").to_pylist())


def compute(
    universe: Universe,
    lake_dir: Path = lake.DEFAULT_DIR,
    *,
    db: duckdb.DuckDBPyConnection | None = None,
) -> Features:
    """The features of every name in `universe`, as they could have been known on its date.

    Prices run through the universe's snapshot session and fundamentals through filings dated
    on or before its `as_of`. `db` is an open connection from `lake.connect(lake_dir)`; pass it
    when computing a run of dates, so the lake's views are not rebuilt for each."""
    conn = db if db is not None else lake.connect(lake_dir)
    try:
        _require(conn)
        rows = _raw_rows(conn, universe) if universe.members else []
    finally:
        if db is None:
            conn.close()
    columns = {name: [row[i + 2] for row in rows] for i, name in enumerate(NUMERIC)}
    tickers = pa.array([row[0] for row in rows], pa.string())
    sectors = sector_array([row[1] for row in rows])
    raw = pa.table(
        {
            "ticker": tickers,
            "sector": sectors,
            **{name: pa.array(values, pa.float64()) for name, values in columns.items()},
        }
    )
    ranked = pa.table(
        {
            "ticker": tickers,
            "sector": sectors,
            **{name: pa.array(percentile_rank(v), pa.float64()) for name, v in columns.items()},
        }
    )
    return Features(
        as_of=universe.as_of,
        snapshot_date=universe.snapshot_date,
        partition_hash=universe.partition_hash,
        raw=raw,
        ranked=ranked,
    )


def history(
    start: date = lake.SINCE,
    end: date | None = None,
    lake_dir: Path = lake.DEFAULT_DIR,
    *,
    db: duckdb.DuckDBPyConnection | None = None,
) -> Iterator[Features]:
    """The features at every month-end from `start` to `end`, oldest first, each computed over
    that month's universe exactly as the live pipeline would have computed it."""
    conn = db if db is not None else lake.connect(lake_dir)
    hashes: dict[object, str] = {}
    try:
        for as_of in month_ends(start, end):
            universe = snapshot(as_of, lake_dir, db=conn, hashes=hashes)
            yield compute(universe, lake_dir, db=conn)
    finally:
        if db is None:
            conn.close()


def percentile_rank(values: Sequence[float | None]) -> list[float | None]:
    """Each value's place among the others, mapped onto [0, 1]: the smallest is 0, the largest
    1, and tied values share the mean of the places they span. The order of the values is kept
    exactly -- a larger value never gets a smaller rank -- and nothing but the order survives.

    A missing value (None or NaN) stays missing and takes no place. A lone value has no one to
    be ranked against and sits at the middle, 0.5."""
    present = sorted(
        (value, i) for i, value in enumerate(values) if value is not None and not math.isnan(value)
    )
    ranks: list[float | None] = [None] * len(values)
    count = len(present)
    if count == 1:
        ranks[present[0][1]] = 0.5
        return ranks
    low = 0
    while low < count:
        high = low
        while high + 1 < count and present[high + 1][0] == present[low][0]:
            high += 1
        for _, i in present[low : high + 1]:
            ranks[i] = (low + high) / 2 / (count - 1)
        low = high + 1
    return ranks


def sector_array(sectors: Sequence[str | None]) -> pa.DictionaryArray:
    """Sectors as a categorical over the fixed SECTORS vocabulary, so a sector keeps its code
    across months and between research and live. An unknown or empty sector is missing."""
    codes = {sector: code for code, sector in enumerate(SECTORS)}
    return pa.DictionaryArray.from_arrays(
        pa.array([codes.get(s) if s else None for s in sectors], pa.int8()),
        pa.array(SECTORS, pa.string()),
    )


@dataclass(frozen=True)
class FeatureIC:
    """How one feature's ranks lined up with the next month's returns across a run of months:
    the sanity check that a published factor points the way the literature says it does. The
    report shows it; nothing gates on it, because a factor can go quiet for a decade."""

    feature: str
    expected_sign: int
    months: int  # months with an information coefficient
    mean_ic: float | None  # the mean monthly Spearman correlation with forward return
    t_stat: float | None  # the mean over its standard error, treating months as independent
    hit_rate: float | None  # the fraction of months whose IC has the expected sign

    @property
    def as_expected(self) -> bool | None:
        if self.mean_ic is None or self.mean_ic == 0:
            return None
        return math.copysign(1, self.mean_ic) == self.expected_sign


def forward_returns(
    features: Features,
    lake_dir: Path = lake.DEFAULT_DIR,
    *,
    db: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, float] | None:
    """Each name's return over the month the features were built to predict: bought at the
    close of the session after the snapshot, sold at the close of the session after the next
    month's last session, as the live basket is traded. A name that stops trading inside the
    month is marked at its last close. None if the lake does not reach the exit session yet."""
    conn = db if db is not None else lake.connect(lake_dir)
    try:
        session = features.snapshot_date
        window = forward_window(session, db=conn)
        if window is None:
            return None
        entry, exit_ = window
        conn.register(_MEMBERS, features.raw.select(["ticker"]))
        try:
            rows = conn.execute(
                _FORWARD, {"session": session, "entry": entry, "exit": exit_}
            ).fetchall()
        finally:
            conn.unregister(_MEMBERS)
    finally:
        if db is None:
            conn.close()
    return {ticker: value for ticker, value in rows if value is not None}


def forward_window(
    session: date, lake_dir: Path = lake.DEFAULT_DIR, *, db: duckdb.DuckDBPyConnection | None = None
) -> tuple[date, date] | None:
    """The sessions a forward return over `session`'s features runs between: the entry, the first
    session after `session`, and the exit, the first session after the following month's last.
    Research labels and the live basket's month are the same window. None if the lake does not
    reach the exit session yet."""
    conn = db if db is not None else lake.connect(lake_dir)
    try:
        following = _month_start(session, -2)
        entry, month_end = conn.execute(
            "SELECT min(date) FILTER (WHERE date > $session),"
            " max(date) FILTER (WHERE date < $following)"
            " FROM sep WHERE date > $session",
            {"session": session, "following": following},
        ).fetchone()
        if entry is None or month_end is None or month_end < _month_start(session, -1):
            return None
        exit_ = conn.execute("SELECT min(date) FROM sep WHERE date > ?", [month_end]).fetchone()[0]
    finally:
        if db is None:
            conn.close()
    return None if exit_ is None else (entry, exit_)


def information_coefficients(
    features: Features, returns: dict[str, float]
) -> dict[str, float | None]:
    """Each numeric feature's Spearman rank correlation with forward return across the names
    that have both, or None where fewer than three names do or either side is constant."""
    tickers = features.tickers
    result: dict[str, float | None] = {}
    for name in NUMERIC:
        pairs = [
            (value, returns[ticker])
            for ticker, value in zip(tickers, features.raw.column(name).to_pylist(), strict=True)
            if value is not None and not math.isnan(value) and ticker in returns
        ]
        try:
            result[name] = statistics.correlation(*zip(*pairs, strict=True), method="ranked")
        except (statistics.StatisticsError, TypeError, ValueError):
            result[name] = None
    return result


def summarise(monthly: Iterable[dict[str, float | None]]) -> list[FeatureIC]:
    """Fold a run of months' information coefficients into one line per feature."""
    series: dict[str, list[float]] = {name: [] for name in NUMERIC}
    for month in monthly:
        for name in NUMERIC:
            value = month.get(name)
            if value is not None:
                series[name].append(value)
    report = []
    for name in NUMERIC:
        ics, sign = series[name], EXPECTED_SIGN[name]
        mean = statistics.fmean(ics) if ics else None
        spread = statistics.stdev(ics) if len(ics) > 1 else 0.0
        report.append(
            FeatureIC(
                feature=name,
                expected_sign=sign,
                months=len(ics),
                mean_ic=mean,
                t_stat=mean / (spread / math.sqrt(len(ics)))
                if mean is not None and spread
                else None,
                hit_rate=sum(ic * sign > 0 for ic in ics) / len(ics) if ics else None,
            )
        )
    return report


def _raw_rows(conn: duckdb.DuckDBPyConnection, universe: Universe) -> list[tuple]:
    session = universe.snapshot_date
    first = _month_start(session, 12)
    sessions = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT date FROM sep WHERE date BETWEEN ? AND ? ORDER BY date",
            [first, session],
        ).fetchall()
    ]
    if not sessions:
        raise FeatureError(f"the lake has no session between {first} and {session}")
    params: dict[str, object] = {
        "as_of": universe.as_of,
        "session": session,
        "first": first,
        "recent": sessions[-min(LIQUIDITY_SESSIONS, len(sessions))],
        "min_returns": MIN_RETURNS,
    }
    for months in MOMENTUM_MONTHS:
        month = _month_start(session, months)
        in_month = [day for day in sessions if (day.year, day.month) == (month.year, month.month)]
        # A month the lake holds no session in leaves its mark missing, and with it every
        # feature that needs the mark.
        params[f"from{months}"] = month
        params[f"ref{months}"] = in_month[-1] if in_month else date.min
    # The risk window opens after the month t-12 session. A lake younger than a year opens it
    # at its first session, and MIN_RETURNS decides whether what it holds is enough.
    params["risk_start"] = max(params["ref12"], sessions[0])
    members = pa.table(
        {
            "ticker": pa.array([m.ticker for m in universe.members], pa.string()),
            "market_cap": pa.array([m.market_cap for m in universe.members], pa.float64()),
        }
    )
    conn.register(_MEMBERS, members)
    try:
        return conn.execute(_QUERY, params).fetchall()
    finally:
        conn.unregister(_MEMBERS)


def _month_start(day: date, months_back: int) -> date:
    """The first day of the calendar month `months_back` months before `day`'s."""
    index = day.year * 12 + day.month - 1 - months_back
    return date(index // 12, index % 12 + 1, 1)


def _require(db: duckdb.DuckDBPyConnection) -> None:
    """Fail with the missing dataset or columns named, rather than a DuckDB binder error."""
    for view, columns in _NEEDED.items():
        try:
            present = {row[0] for row in db.execute(f"DESCRIBE {view}").fetchall()}
        except duckdb.Error:
            raise FeatureError(f"the lake has no {view} partitions") from None
        missing = sorted(set(columns) - present)
        if missing:
            raise FeatureError(f"the lake's {view} lacks {missing}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m agb.features",
        description="Compute the stage-1 features as they could have been known on a date.",
    )
    parser.add_argument(
        "--lake",
        type=Path,
        default=Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR)),
        help="the lake directory (default %(default)s)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    show = commands.add_parser("show", help="one rebalance date's features")
    show.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=None,
        help="the rebalance date (default the last month-end on or before today)",
    )
    show.add_argument("--raw", action="store_true", help="print values before ranking")
    for name, text in (
        ("history", "every month-end's features, written to a Parquet file"),
        ("report", "each feature's rank correlation with the next month's return"),
    ):
        command = commands.add_parser(name, help=text)
        command.add_argument("--start", type=date.fromisoformat, default=lake.SINCE)
        command.add_argument("--end", type=date.fromisoformat, default=None)
    commands.choices["history"].add_argument("--out", type=Path, required=True)
    commands.choices["history"].add_argument(
        "--raw", action="store_true", help="write values before ranking"
    )
    args = parser.parse_args(argv)
    try:
        if args.command == "show":
            _show(args.lake, args.as_of, raw=args.raw)
        elif args.command == "history":
            _write_history(args.lake, args.start, args.end, args.out, raw=args.raw)
        else:
            _report(args.lake, args.start, args.end)
    except (FeatureError, UniverseError, lake.LakeError, OSError) as exc:
        raise SystemExit(str(exc)) from None


def _show(lake_dir: Path, as_of: date | None, *, raw: bool) -> None:
    if as_of is None:
        ends = month_ends()
        if not ends:
            raise SystemExit("no month-end has passed yet")
        as_of = ends[-1]
    features = compute(snapshot(as_of, lake_dir), lake_dir)
    table = features.raw if raw else features.ranked
    print(f"as_of\t{features.as_of}")
    print(f"snapshot_date\t{features.snapshot_date}")
    print(f"size\t{table.num_rows}")
    print(f"partition_hash\t{features.partition_hash}")
    print("\t".join(table.column_names))
    for row in table.to_pylist():
        print("\t".join(_cell(row[name]) for name in table.column_names))


def _write_history(lake_dir: Path, start: date, end: date | None, out: Path, *, raw: bool) -> None:
    began = time.monotonic()
    writer = None
    try:
        for features in history(start, end, lake_dir):
            table = features.raw if raw else features.ranked
            table = table.add_column(
                0, "as_of", pa.array([features.as_of] * table.num_rows, pa.date32())
            )
            if writer is None:
                writer = pq.ParquetWriter(out, table.schema)
            writer.write_table(table)
            print(f"{features.as_of}\t{table.num_rows}", file=sys.stderr, flush=True)
    finally:
        if writer is not None:
            writer.close()
    print(f"wrote {out} in {time.monotonic() - began:.0f}s", file=sys.stderr)


def _report(lake_dir: Path, start: date, end: date | None) -> None:
    conn = lake.connect(lake_dir)
    monthly = []
    try:
        for features in history(start, end, lake_dir, db=conn):
            returns = forward_returns(features, lake_dir, db=conn)
            if returns is not None:
                monthly.append(information_coefficients(features, returns))
                print(f"{features.as_of}\t{len(returns)}", file=sys.stderr, flush=True)
    finally:
        conn.close()
    print("feature\texpected\tmonths\tmean_ic\tt_stat\thit_rate\tas_expected")
    for line in summarise(monthly):
        print(
            f"{line.feature}\t{line.expected_sign:+d}\t{line.months}\t{_cell(line.mean_ic)}"
            f"\t{_cell(line.t_stat, 2)}\t{_cell(line.hit_rate, 2)}\t{line.as_expected}"
        )


def _cell(value: object, places: int = 4) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.{places}g}" if abs(value) < 1e-3 and value else f"{value:.{places}f}"
    return str(value)


if __name__ == "__main__":
    main()
