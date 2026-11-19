"""The point-in-time investable universe: the names that could have been traded on a rebalance
date, as they could have been known on that date.

Research and live both import `snapshot`, so a backtest ranks exactly the names the live
pipeline would have ranked. The filters are the ones in docs/architecture/02-classical-ranker.md:

    market capitalisation       > $300M
    60-day median dollar volume > $1M
    price                       > $5
    common stock only           (no ETFs, ADRs, preferred, warrants or REITs)

Three details make the result point-in-time rather than merely historical:

  * **Fundamentals are gated on `datekey`**, SF1's SEC filing date, so a figure filed after the
    rebalance date cannot reach a universe built for it. A late filer's numbers are invisible
    until the day it filed, which is what the pipeline would have seen at the time. Only the
    as-reported dimensions are read; the MR dimensions carry the vendor's later restatements,
    which is exactly the hindsight this module exists to keep out.
  * **Prices are the ones actually traded.** SEP's `close` and `volume` are adjusted for splits
    all the way back, so `close` is not the price the $5 screen is about, and multiplying it by
    a share count reported before a later split understates market cap by the split factor. The
    price screen and market cap use `closeunadj`, the close as traded, against `sharesbas`, the
    share count as reported. Dollar volume uses `close * volume`, both adjusted the same way,
    which makes the product split-invariant.
  * **The share count is put on the price's split basis.** `sharesbas` is as the filing reported
    it, so a split after the filing leaves it counting shares of the old size. It is scaled by
    the splits since the filing, read off SEP as the move in `closeunadj / close` between the
    session of the filing and the snapshot session; a ten-for-one split between filings leaves
    market capitalisation where it was instead of cutting it tenfold. A filing with no session
    in the days before it is taken as unsplit, as before.
  * **A name is in the universe only if it traded on the snapshot session.** Nothing is carried
    forward from a last known price, so a name delisted in 2008 is in every universe up to its
    last print and in none after it.

The tickers table is the vendor's snapshot as it stands, not a point-in-time record, so the
common-stock test uses today's classification of each name. Sharadar keeps delisted tickers in
it, which is what makes a 2008 universe classifiable at all. Two limits follow: a company
reclassified since, and a ticker symbol later reused by a different company, are both judged by
the newer row.

Each universe carries the session its prices came from and a digest of the lake partitions it
was built from, so a basket can be tied to the bytes behind it, and its size, so the pre-trade
gate can refuse a universe that has left the plausible band.

    python -m agb.universe --as-of 2026-08-31
"""

import argparse
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb

from agb import lake, nyse

MIN_MARKET_CAP = 300_000_000.0
MIN_DOLLAR_VOLUME = 1_000_000.0
MIN_PRICE = 5.0
# Sessions in the dollar-volume window: the 60 most recent the lake holds up to the rebalance
# date, not 60 calendar days, so a holiday-shortened month does not shorten the window.
LIQUIDITY_SESSIONS = 60
# Sessions a name must have inside that window for its median to count. A median of three
# sessions is not a liquidity measure, and a name that has just listed would pass on a handful
# of heavy first days. Two thirds of the window keeps a name listed in the last quarter and
# drops one listed last week.
MIN_LIQUIDITY_SESSIONS = 40
# Why a name that cleared every other screen was left out.
THIN_HISTORY = (
    f"fewer than {MIN_LIQUIDITY_SESSIONS} sessions in the {LIQUIDITY_SESSIONS}-session window"
)
# Calendar days before a filing's `datekey` in which its session is looked for; a filing dated
# on a weekend or holiday is priced at the session before it.
FILING_SESSION_DAYS = 10
# The pre-trade gate's provenance band (docs/architecture/04-risk-and-approval.md). A universe
# outside it is a provenance failure, not a small universe. The band is measured against the
# lake rather than estimated: the filters above yield 2,432 names on 2026-08-31, and between
# 1,703 (2000-12-29) and 2,432 across month-ends since mid-1998. The spec's earlier estimate of
# 3,000-4,000 counted the names the common-stock test removes -- the three quantitative screens
# alone pass 3,040 today, of which 359 are ADRs, 144 REITs and 105 Canadian.
MIN_SIZE, MAX_SIZE = 1_500, 3_500
# The vendor's category values for ordinary domestic common stock. Listed rather than matched by
# prefix because "Domestic Common Stock Warrant" shares the prefix and is not common stock.
COMMON = (
    "Domestic Common Stock",
    "Domestic Common Stock Primary Class",
    "Domestic Common Stock Secondary Class",
)
# SIC 6798 is the SEC's own code for a real estate investment trust. The vendor's `industry`
# names them too, and the two disagree often enough to be worth testing both.
REIT_SIC = "6798"

_NEEDED = {
    "sep": ("ticker", "date", "close", "closeunadj", "volume"),
    "sf1": ("ticker", "dimension", "datekey", "sharesbas", "sharefactor"),
    "tickers": ("table", "ticker", "category", "industry", "siccode"),
}

_QUERY = f"""
WITH recent AS (
    SELECT DISTINCT date FROM sep WHERE date <= ? ORDER BY date DESC LIMIT ?
),
span AS (SELECT min(date) AS earliest, max(date) AS latest FROM recent),
liquidity AS (
    SELECT sep.ticker, median(sep.close * sep.volume) AS dollar_volume, count(*) AS sessions
    FROM sep, span
    WHERE sep.date BETWEEN span.earliest AND span.latest
      AND sep.close IS NOT NULL
      AND sep.volume IS NOT NULL
    GROUP BY sep.ticker
),
traded AS (
    SELECT sep.ticker, sep.closeunadj AS price
    FROM sep, span
    WHERE sep.date = span.latest AND sep.closeunadj IS NOT NULL AND sep.volume > 0
),
filed AS (
    SELECT ticker, datekey, sharesbas * coalesce(sharefactor, 1.0) AS shares
    FROM sf1_as_of(?)
    WHERE dimension IN ('ARQ', 'ARY')
      AND sharesbas IS NOT NULL
      AND sharesbas > 0
    -- The latest filing on or before the rebalance date. ARQ beats ARY where a company filed
    -- both the same day, because the quarterly cover page carries the newer share count, and
    -- of two periods filed the same day the later one wins, for the same reason.
    QUALIFY row_number() OVER (
        PARTITION BY ticker ORDER BY datekey DESC, dimension ASC, reportperiod DESC
    ) = 1
),
common AS (
    SELECT DISTINCT ticker
    FROM tickers
    WHERE "table" = 'stocks'
      AND category IN ({", ".join("?" for _ in COMMON)})
      AND coalesce(industry, '') NOT LIKE 'REIT%'
      AND coalesce(siccode, '') <> ?
),
-- The split factor of each filing's session: what one share then is worth in today's adjusted
-- shares, 1 when the lake holds no session just before the filing. Only the names that clear
-- the cheaper screens are looked up.
screened AS (
    SELECT ticker, price, dollar_volume, sessions, shares, datekey
    FROM traded
    JOIN liquidity USING (ticker)
    JOIN filed USING (ticker)
    JOIN common USING (ticker)
    WHERE price > ? AND dollar_volume > ?
),
filing_session AS (
    SELECT screened.ticker, sep.closeunadj / sep.close AS factor
    FROM screened
    JOIN sep ON sep.ticker = screened.ticker
        AND sep.date BETWEEN screened.datekey - INTERVAL {FILING_SESSION_DAYS} DAY
                         AND screened.datekey
    WHERE sep.close > 0 AND sep.closeunadj > 0
    QUALIFY row_number() OVER (PARTITION BY screened.ticker ORDER BY sep.date DESC) = 1
),
latest_session AS (
    SELECT sep.ticker, sep.closeunadj / sep.close AS factor
    FROM sep, span
    WHERE sep.date = span.latest AND sep.close > 0 AND sep.closeunadj > 0
)
SELECT ticker, price, dollar_volume, sessions,
       price * shares * coalesce(filing_session.factor / latest_session.factor, 1.0) AS market_cap
FROM screened
LEFT JOIN filing_session USING (ticker)
LEFT JOIN latest_session USING (ticker)
WHERE price * shares * coalesce(filing_session.factor / latest_session.factor, 1.0) > ?
ORDER BY ticker
"""


class UniverseError(Exception):
    """A universe that cannot be built: the lake is missing a date, a dataset or a column."""


@dataclass(frozen=True)
class Member:
    """One name in the universe, with the three quantities it was admitted on."""

    ticker: str
    price: float  # the close as traded on the snapshot session
    dollar_volume: float  # the 60-session median of close * volume
    market_cap: float


@dataclass(frozen=True)
class Exclusion:
    """A name that cleared every screen but one, and the reason it was left out."""

    ticker: str
    reason: str


@dataclass(frozen=True)
class Universe:
    """The investable universe for one rebalance date, and where it came from."""

    as_of: date  # the rebalance date asked for
    snapshot_date: date  # the session its prices came from, at or before `as_of`
    partition_hash: str  # a digest of the lake partitions it was built from
    members: tuple[Member, ...]
    # Names that passed the price, size, liquidity and common-stock screens but lacked the
    # sessions for a liquidity median, so a thin listing is visible rather than silently absent.
    excluded: tuple[Exclusion, ...] = ()

    @property
    def size(self) -> int:
        return len(self.members)

    @property
    def tickers(self) -> tuple[str, ...]:
        return tuple(member.ticker for member in self.members)

    @property
    def plausible_size(self) -> bool:
        return size_is_plausible(self.size)


def size_is_plausible(size: int) -> bool:
    """Whether a universe's size is inside the pre-trade gate's provenance band. A universe
    outside it means the lake or the filters moved, so the gate stops the rebalance rather than
    trading a universe it does not recognise."""
    return MIN_SIZE <= size <= MAX_SIZE


def snapshot(
    as_of: date,
    lake_dir: Path = lake.DEFAULT_DIR,
    *,
    db: duckdb.DuckDBPyConnection | None = None,
    hashes: dict[object, str] | None = None,
) -> Universe:
    """The investable universe as it could have been known on `as_of`.

    Prices come from the last session the lake holds on or before `as_of`, so a month-end that
    falls on a weekend or a holiday resolves to the session before it. Fundamentals come from
    the latest filing whose `datekey` is on or before `as_of`.

    `db` is an open connection from `lake.connect(lake_dir)`, and `hashes` a dictionary of
    partition hashes; pass both when building a run of universes, so neither the connection nor
    the digest is rebuilt for each date. Both must belong to `lake_dir`.
    """
    conn = db if db is not None else lake.connect(lake_dir)
    try:
        lake.require_columns(conn, _NEEDED, UniverseError)
        session = conn.execute("SELECT max(date) FROM sep WHERE date <= ?", [as_of]).fetchone()[0]
        if session is None:
            raise UniverseError(f"the lake has no session on or before {as_of}")
        rows = conn.execute(
            _QUERY,
            [
                as_of,
                LIQUIDITY_SESSIONS,
                as_of,
                *COMMON,
                REIT_SIC,
                MIN_PRICE,
                MIN_DOLLAR_VOLUME,
                MIN_MARKET_CAP,
            ],
        ).fetchall()
    finally:
        if db is None:
            conn.close()
    return Universe(
        as_of=as_of,
        snapshot_date=session,
        partition_hash=lake.partition_digest(lake_dir, session, hashes=hashes),
        members=tuple(
            Member(str(t), float(p), float(v), float(c))
            for t, p, v, n, c in rows
            if n >= MIN_LIQUIDITY_SESSIONS
        ),
        excluded=tuple(
            Exclusion(str(t), THIN_HISTORY) for t, _, _, n, _ in rows if n < MIN_LIQUIDITY_SESSIONS
        ),
    )


def _last_finished_session(now: datetime | None = None) -> date:
    """Today's trading date if its session has closed, otherwise the day before it, which
    `month_ends` then resolves to a session. Today on a weekend or holiday is no session, so the
    calendar's last month-end is unaffected."""
    now = now or datetime.now(UTC)
    today = nyse.trading_date(now)
    calendar = nyse.NyseCalendar(today - timedelta(days=10), today)
    if calendar.sessions(today, today) and now < calendar.close_at(today):
        return today - timedelta(days=1)
    return today


def month_ends(start: date = lake.SINCE, end: date | None = None) -> list[date]:
    """The last NYSE session of each month from `start` to `end`: the rebalance dates a research
    run walks. Sharadar's SEP coverage starts in 1998, so that is the default start, and the last
    finished session the default end. A month whose last session has not happened yet is left
    out, and so is one that is still being traded: today counts only once it has closed."""
    end = end or _last_finished_session()
    if end < start:
        return []
    # A month's last session can fall a few days before its last day, so widen the calendar
    # rather than asking it for a month it does not cover.
    calendar = nyse.NyseCalendar(start - timedelta(days=40), end + timedelta(days=40))
    dates, year, month = [], start.year, start.month
    while (year, month) <= (end.year, end.month):
        session = calendar.last_session_of_month(year, month)
        if start <= session <= end:
            dates.append(session)
        year, month = year + month // 12, month % 12 + 1
    return dates


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m agb.universe",
        description="Print the investable universe as it could have been known on a date.",
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        default=None,
        help="the rebalance date (default the last month-end on or before today)",
    )
    parser.add_argument(
        "--lake",
        type=Path,
        default=Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR)),
        help="the lake directory (default %(default)s)",
    )
    parser.add_argument("--tickers", action="store_true", help="list the members, one per line")
    args = parser.parse_args(argv)
    as_of = args.as_of
    if as_of is None:
        ends = month_ends()
        if not ends:
            raise SystemExit("no month-end has passed yet")
        as_of = ends[-1]
    try:
        universe = snapshot(as_of, args.lake)
    except (UniverseError, lake.LakeError, OSError) as exc:
        raise SystemExit(str(exc)) from None
    print(f"as_of\t{universe.as_of}")
    print(f"snapshot_date\t{universe.snapshot_date}")
    print(f"size\t{universe.size}")
    print(f"plausible_size\t{universe.plausible_size}")
    print(f"partition_hash\t{universe.partition_hash}")
    if args.tickers:
        for member in universe.members:
            print(
                f"{member.ticker}\t{member.price:.4f}"
                f"\t{member.dollar_volume:.0f}\t{member.market_cap:.0f}"
            )


if __name__ == "__main__":
    main()
