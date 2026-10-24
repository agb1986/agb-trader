"""The NautilusTrader validation backtest: the scan's strategy replayed session by session
through an event-driven engine that fills and charges every order itself.

The point of the backtest is to check the vectorbt scan against an independent engine, not to
be a second implementation of the strategy. The strategy here ranks each month's names by one
signal, exactly as `scan.ranking` does, and hands the ranking to `agb.rebalance.select`,
`agb.rebalance.weights` and `agb.rebalance.orders`, the code the live basket runs, unchanged:
the 20-buy, 30-hold buffer, the tie-breaks, the equal weights and the whole-share sizing all
come from there. NautilusTrader has no cross-sectional abstraction of its own, so what the
strategy adds is only the clock: it decides at the close of each month's signal session and
trades at the close of the next session, as the live pipeline does.

    signal session L   `month.snapshot_date`: the ranking, the held names, the prints and the
                       liquidity are read from the bars through this session, and the plan is
                       sized to the equity at its close
    trade session R    `month.entry`: the plan's sells are submitted once every bar of the
                       session is in, then its buys once the sells have filled, so each fills
                       at the session's official close, where the panel's labels are measured

Fills are at the close with no slippage of their own; every fill is charged `cost.trade_cost`
over `cost.liquidity` on the sixty-one bars through the signal session, the live cost model on
both sides plus the sell-side fees, through NautilusTrader's own fee model, so the engine's cash
account is net of the same cost the pre-trade gate applies. `cost_multiple` scales that charge
for the two-times-cost checks. A buy that the account cannot fund whole after the sells is
trimmed to the free cash, never refused, and the report counts the trims.

The bars are every name's daily bars from the lake, with prices adjusted like the panel's
labels and the scan's prices, including names that have since been delisted: a held name whose
bars stop before the lake's end is sold at its last listed print, as the specification's
forced-exit rule requires, and the report counts those exits. A name without a print at the
signal session, or without the bars the cost model needs, or whose bars stop by the trade
session, is untradeable at that rebalance: carried if held and never bought, exactly as
`basket.propose` treats it.

`forced` passes the recorded corporate events (`agb.corporate`) a period's rebalance must
account for, and they are applied by the same rules the live basket applies, from the same
module: a cash acquisition and a stock-for-stock merger end the position on their effective
date, without a decision of the strategy's and without a commission, and a merger's acquirer
shares are bought with what the subject's shares raised; the cash either leaves raised sits
until the next rebalance, which is the specification's no-mid-month-redeployment rule; a
halted incumbent is carried and its replacement skipped, so nineteen names trade; and a
delisted or converted name is divested at the next rebalance whatever its rank. Events are
passed in rather than put in `Options` because the options are serialised into the trial row,
and because they are a fact about a month rather than a knob of the configuration.

Every run reports gross and net returns, monthly and in total, the two-sided turnover of each
rebalance, the maximum and mean participation (order dollars over the name's median daily
dollar volume), and the same configuration run from several later start dates. Given one
score series per combinatorial path, it reports each path's series separately, in the form
the deflated Sharpe and overfitting gates consume. It also reports the approximate UK
after-tax view of the run (`agb.research.tax`) beside the same treatment of buying `BENCHMARK`
and holding it, because the operator is a UK tax resident and a monthly rebalance realises its
gains every year where holding an index defers them; that view is labelled approximate, states
its simplifications and is read by no gate.

Four things separate this backtest from the scan by design, and bound how closely the two can
agree: a name that stops printing is sold here at its last listed print, where the scan carries
the last price forward and sells at the next rebalance that drops it from the ranking; a buy
the account cannot fund whole is trimmed here, and dropped if it rounds to no shares, where
vectorbt's shared cash group does neither; orders are sized in whole shares at the signal
session's close, as the live basket sizes them, where vectorbt sizes fractional shares at the
fill; and the engine's capital compounds through the account, where the scan's does through
vectorbt's cash group. On the fake market the first three never fire, and the
reference-configuration test states the tolerance the fourth leaves. On the real lake all four
fire, so `tests/test_backtest_live.py` reconciles the two order sets through the delisting-exit
and trim counts reported here rather than expecting the two to agree.

Each backtest is a trial, like a scan: its options and its results append a row."""

import warnings
from bisect import bisect_left, bisect_right
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.backtest.models import BestPriceFillModel, FeeModel
from nautilus_trader.config import LoggingConfig, RiskEngineConfig, StrategyConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import AccountType, OmsType, OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.trading.strategy import Strategy

from agb import corporate, cost, features, lake, rebalance
from agb.research import scan, tax
from agb.research.panel import Month, Panel
from agb.research.trials import Ledger, Run

KIND = "backtest"
VENUE = Venue("SIM")
MONTHS_PER_YEAR = 12
LIQUIDITY_BARS = scan.LIQUIDITY_BARS
# Decimal places of the adjusted prices the engine trades; split-adjusted closes run small.
PRECISION = 6
# The session's bars are stamped at this UTC hour, after the New York close all year round.
BENCHMARK = "SPY"  # the after-tax diagnostic's buy-and-hold comparison
CLOSE_HOUR_UTC = 21
# Calendar days of bars loaded before the first signal session, so the cost model has its
# sixty-one bars from the first rebalance on.
HISTORY_DAYS = 120
_NEEDED = ("ticker", "date", "open", "high", "low", "close", "closeadj", "volume")
# Each name gets its own window rather than one window for all of them. A name first ranked in
# 2015 is never looked at before 2015, and loading its 1998 bars costs a `Bar` object apiece to
# no end: over a validation's fifteen paths most of the bars loaded were leading history for a
# name that had not been ranked yet. `_LEAD` finds the date that still leaves a name
# LIQUIDITY_BARS bars of history before the first session a decision can read it -- every bar
# the liquidity gate counts and the cost model's window reads -- and `_BARS` loads from there.
#
# `_LEAD` keeps a bar on a stricter test than `_BARS` does. `_BARS` loads on `closeadj > 0`
# and the frame then keeps the bars whose adjusted close came out above zero, which is
# `closeadj` again by way of a division and a multiplication; an adjusted close above a
# millionth survives that round trip. So a bar `_LEAD` counted is always a bar the loaded
# frame keeps, and the history is never one bar short of the sixty-one the gate reads.
_LEAD = """
SELECT ticker, min(date) AS since
FROM (
    SELECT s.ticker, s.date,
           row_number() OVER (PARTITION BY s.ticker ORDER BY s.date DESC) AS back
    FROM sep s JOIN _backtest_first f ON s.ticker = f.ticker
    WHERE s.date < f.first AND s.close > 0 AND s.closeadj > 1e-6
)
WHERE back <= $bars
GROUP BY ticker
"""
_BARS = """
SELECT s.ticker, s.date, s.open, s.high, s.low, s.close, s.closeadj, s.volume
FROM sep s JOIN _backtest_window w ON s.ticker = w.ticker
WHERE s.date BETWEEN w.since AND $until AND s.close > 0 AND s.closeadj > 0
ORDER BY s.ticker, s.date
"""
# The lake's own first months cannot be traded at all. The liquidity gate reads LIQUIDITY_BARS
# bars of a name and the cost model reads the same window, so until the lake holds that many
# sessions nothing is tradeable, the top of every ranking included. A model's score ranks every
# row of the universe, where a feature such as momentum has no ranking until its trailing year
# has landed, so those months reach the selection with a full ranking and no tradeable name in
# it. `_SESSION` is the date the lake's LIQUIDITY_BARS'th session fell on.
_SESSION = """
SELECT date FROM (SELECT DISTINCT date FROM sep ORDER BY date) LIMIT 1 OFFSET $skip
"""


class BacktestError(Exception):
    pass


@dataclass(frozen=True)
class Options:
    signal: str = "mom_12_1"
    size: int = rebalance.SIZE
    hold_through: int = rebalance.HOLD_THROUGH
    init_cash: float = 1_000_000.0
    cost_multiple: float = 1.0
    # ISO dates: the same configuration is also run from the first month at or after each.
    # Empty means the months a quarter, a half and three quarters of the way through the panel.
    starts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.size < 1:
            raise ValueError("size must be at least one name")
        if self.size > self.hold_through:
            raise ValueError(f"size {self.size} exceeds the hold-through rank {self.hold_through}")
        if self.init_cash <= 0:
            raise ValueError("init_cash must be positive")
        if self.cost_multiple <= 0:
            raise ValueError("cost_multiple must be positive")
        starts = tuple(date.fromisoformat(str(start)).isoformat() for start in self.starts)
        object.__setattr__(self, "starts", starts)


def run(
    panel: Panel,
    options: Options,
    ledger: Ledger,
    *,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
    scores: pd.Series | Mapping[str, pd.Series] | None = None,
    forced: Sequence[corporate.Event] = (),
) -> Run:
    """Backtest `options.signal` over the panel. `scores`, aligned to the panel's rows, stands
    in for a signal that is not a feature; a mapping of them, one per combinatorial path, runs
    each path and reports them side by side. `forced` is the recorded corporate events the
    months must account for, each applied at the rebalance its period names."""
    frame = panel.frame
    months = panel.months
    if not months:
        raise BacktestError("the panel has no months")
    if scores is None and options.signal not in features.FEATURES:
        raise BacktestError(f"{options.signal!r} is not a feature; pass scores for a model signal")
    if isinstance(scores, Mapping):
        if not scores:
            raise BacktestError("no paths to run")
        runs = {str(label): rankings(frame, options, series) for label, series in scores.items()}
    else:
        runs = {"": rankings(frame, options, scores)}
    with ledger.trial(
        KIND,
        {KIND: asdict(options)},
        features=(options.signal,),
        train_start=months[0].as_of,
        train_end=months[-1].as_of,
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
    ) as trial:
        conn = db if db is not None else lake.connect(lake_dir)
        try:
            _require(conn)
            months = _priced(conn, months)
            feed = _feed(runs, months, options, conn, forced=forced)
            if isinstance(scores, Mapping):
                paths = _paths(feed, months, runs, options, forced)
                trial.results = {
                    "months": len(months),
                    "paths": paths,
                    "summary": _summary(paths),
                }
            else:
                ranked = runs[""]
                outcome = simulate(feed, months, ranked, options, forced)
                results = _report(outcome, feed, months, options, forced)
                results["start_dates"] = [
                    _sensitivity(feed, months[i:], ranked, options, forced)
                    for i in _starts(months, options)
                ]
                trial.results = results
        finally:
            if db is None:
                conn.close()
        return Run(trial.trial_id, trial.results)


def replay(
    panel: Panel,
    options: Options,
    scores: Mapping[str, pd.Series],
    *,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
    forced: Sequence[corporate.Event] = (),
) -> list[dict[str, Any]]:
    """Each path's report, as `run` gives them, without a trial: the certification's second
    pass at twice the modelled cost reruns the same paths with a different constant and, as
    the specification says, adds no trial."""
    months = panel.months
    if not months:
        raise BacktestError("the panel has no months")
    if not scores:
        raise BacktestError("no paths to run")
    runs = {str(label): rankings(panel.frame, options, series) for label, series in scores.items()}
    conn = db if db is not None else lake.connect(lake_dir)
    try:
        _require(conn)
        months = _priced(conn, months)
        feed = _feed(runs, months, options, conn, forced=forced)
        return _paths(feed, months, runs, options, forced)
    finally:
        if db is None:
            conn.close()


def _feed(
    runs: Mapping[str, Mapping[date, list[str]]],
    months: Sequence[Month],
    options: Options,
    conn: duckdb.DuckDBPyConnection,
    *,
    forced: Sequence[corporate.Event] = (),
) -> "Feed":
    """The bars of every name any ranking could trade, and of every name a recorded event
    names: a subject whose rank has gone and an acquirer that was never ranked both still have
    to be marked and traded."""
    depth = options.hold_through + options.size
    traded = {month.as_of for month in months}
    # A ranking of a month the backtest does not trade names nobody, so it feeds nobody.
    names = {
        name
        for ranked in runs.values()
        for as_of, month_names in ranked.items()
        if as_of in traded
        for name in month_names[:depth]
    }
    named = {event.symbol for event in forced}
    named |= {event.into_symbol for event in forced if event.into_symbol is not None}
    wanted = _first_read(runs, months, names | named)
    # An event falls where the filing puts it, which can be any month and need not be one
    # the name is ranked in, so a name a recorded event names is read from the first.
    wanted |= dict.fromkeys(named, months[0].snapshot_date)
    return load(conn, wanted, months, benchmark=BENCHMARK)


def _priced(conn: duckdb.DuckDBPyConnection, months: Sequence[Month]) -> list[Month]:
    """The months the lake can price, oldest first: the ones whose signal session has
    LIQUIDITY_BARS of the lake's sessions at or before it.

    A month the lake holds fewer sessions than that for has no tradeable name in it, because
    no name can hold the bars the liquidity gate counts. Trading it would buy nothing and
    still record a month of no return, so the backtest starts after it."""
    row = conn.execute(_SESSION, {"skip": LIQUIDITY_BARS - 1}).fetchone()
    if row is None:
        raise BacktestError(
            f"the lake holds fewer than the {LIQUIDITY_BARS} sessions a decision reads"
        )
    priced = [month for month in months if month.snapshot_date >= row[0]]
    if not priced:
        raise BacktestError(
            f"the lake's {LIQUIDITY_BARS}th session is {row[0]}, which is after every month's "
            "signal session, so no month of the panel can be traded"
        )
    return priced


def _first_read(
    runs: Mapping[str, Mapping[date, list[str]]],
    months: Sequence[Month],
    names: set[str],
) -> dict[str, date]:
    """The first signal session each fed name can be read on, which is the first month any
    path's ranking holds it at all, or the first month of all for a name no path ranks.

    The first month it is ranked in at all, not the first it is ranked near the top of:
    `decide` reads a print and a bar count for every name in the whole month's ranking, and
    calling a name untradeable there because its bars had not been loaded would change the
    selection."""
    signal = {month.as_of: month.snapshot_date for month in months}
    first: dict[str, date] = {}
    for as_of in sorted(signal):
        for ranked in runs.values():
            for name in ranked.get(as_of, ()):
                if name in names:
                    first.setdefault(name, signal[as_of])
    return {name: first.get(name, months[0].snapshot_date) for name in sorted(names)}


def _paths(
    feed: "Feed",
    months: Sequence[Month],
    runs: Mapping[str, Mapping[date, list[str]]],
    options: Options,
    forced: Sequence[corporate.Event] = (),
) -> list[dict[str, Any]]:
    return [
        {
            "path": label,
            **_report(
                simulate(feed, months, ranked, options, forced), feed, months, options, forced
            ),
        }
        for label, ranked in runs.items()
    ]


# Rankings and the start dates -----------------------------------------------------------------


def rankings(frame: pd.DataFrame, options: Options, scores=None) -> dict[date, list[str]]:
    """Each month's ranking, best first, by the scan's own rule, keyed by `as_of`."""
    if scores is not None:
        frame = frame.assign(**{options.signal: np.asarray(scores, dtype="float64")})
    return {as_of: scan.ranking(rows, options) for as_of, rows in frame.groupby("as_of", sort=True)}


def _starts(months: Sequence[Month], options: Options) -> list[int]:
    """The indices of the months the sensitivity runs start from, in order, without repeats."""
    if options.starts:
        indices = []
        for start in options.starts:
            day = date.fromisoformat(start)
            found = [i for i, month in enumerate(months) if month.as_of >= day]
            if not found or found[0] == len(months) - 1:
                raise BacktestError(f"no months to backtest from {start}")
            indices.append(found[0])
    else:
        count = len(months)
        indices = [count // 4, count // 2, 3 * count // 4] if count >= 8 else []
    return sorted({i for i in indices if 0 < i < len(months) - 1})


# The lake's bars ------------------------------------------------------------------------------


@dataclass(frozen=True)
class Series:
    """One name's bars, oldest first: the raw columns the cost model reads and the adjusted
    close the engine trades, rounded to the engine's precision."""

    dates: list[date]
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    adjusted: np.ndarray

    def index(self, day: date) -> int | None:
        """The position of the bar on `day`, if there is one."""
        i = bisect_left(self.dates, day)
        return i if i < len(self.dates) and self.dates[i] == day else None

    def through(self, day: date) -> int:
        """How many bars there are on or before `day`."""
        return bisect_right(self.dates, day)


class Feed:
    """The bars the engine replays and the strategy reads: every fed name's series, the Bar
    objects in session order, and the last session the lake holds for them.

    The benchmark's series is held apart from the fed names and is not streamed to the engine,
    because the strategy must not be able to trade it and because the lake's end for the fed
    names, which is what the forced-exit rule reads, must not move with it."""

    def __init__(
        self,
        series: dict[str, Series],
        bars: list[Bar],
        end: date,
        benchmark: Series | None = None,
    ) -> None:
        self.series = series
        self.benchmark = benchmark  # the buy-and-hold comparison's bars, or None if unlisted
        self.end = end  # the last session in the lake for these names
        self.last = {ticker: s.dates[-1] for ticker, s in series.items()}
        self._bars = bars
        self._bar_days = [_day(bar.ts_event) for bar in bars]

    def print_at(self, ticker: str, day: date) -> float | None:
        """The adjusted close of a print on `day`: a bar with volume."""
        s = self.series[ticker]
        i = s.index(day)
        return float(s.adjusted[i]) if i is not None and s.volume[i] > 0 else None

    def close_at(self, ticker: str, day: date) -> float | None:
        s = self.series[ticker]
        i = s.index(day)
        return None if i is None else float(s.adjusted[i])

    def close_through(self, ticker: str, day: date) -> float:
        """The last adjusted close on or before `day`."""
        s = self.series[ticker]
        n = s.through(day)
        if n == 0:
            raise BacktestError(f"{ticker} has no bar on or before {day}")
        return float(s.adjusted[n - 1])

    def liquidity(self, ticker: str, day: date) -> cost.Liquidity:
        """The cost model's inputs over its window of bars through `day`."""
        s = self.series[ticker]
        n = s.through(day)
        if n < LIQUIDITY_BARS:
            raise BacktestError(f"{ticker} has {n} bars through {day}, not {LIQUIDITY_BARS}")
        window = slice(n - LIQUIDITY_BARS, n)
        return cost.liquidity(
            s.high[window].tolist(),
            s.low[window].tolist(),
            s.close[window].tolist(),
            s.volume[window].tolist(),
        )

    def bars(self, start: date, end: date) -> list[Bar]:
        lo = bisect_left(self._bar_days, start)
        hi = bisect_right(self._bar_days, end)
        return self._bars[lo:hi]

    def last_session(self, start: date, end: date) -> date | None:
        lo = bisect_left(self._bar_days, start)
        hi = bisect_right(self._bar_days, end)
        return self._bar_days[hi - 1] if hi > lo else None

    def instruments(self) -> list[Equity]:
        return [_equity(ticker) for ticker in self.series]


def _windows(conn: duckdb.DuckDBPyConnection, wanted: Mapping[str, date]) -> dict[str, date]:
    """The first bar to load for each name in `wanted`: the one that leaves it LIQUIDITY_BARS
    bars of history before the first session a decision can read it on, or its own first bar
    where the lake holds fewer than that.

    Those are every bar the liquidity gate counts and the cost model's window reads, so each
    decision is the one the whole history would have given."""
    conn.register(
        "_backtest_first",
        pd.DataFrame({"ticker": list(wanted), "first": list(wanted.values())}),
    )
    try:
        lead = dict(conn.execute(_LEAD, {"bars": LIQUIDITY_BARS}).fetchall())
    finally:
        conn.unregister("_backtest_first")
    return {name: lead.get(name, read) for name, read in wanted.items()}


def load(
    conn: duckdb.DuckDBPyConnection,
    wanted: Mapping[str, date],
    months: Sequence[Month],
    benchmark: str | None = None,
) -> Feed:
    """The feed for `wanted`, each name against the first signal session a decision can read
    it on, and the benchmark's series alongside it if the lake holds one: a lake of equity
    prices need not carry an index fund, and the diagnostic that reads it reports nothing
    rather than failing the run."""
    start = months[0].snapshot_date
    until = months[-1].exit
    windows = _windows(conn, wanted)
    # The benchmark is marked at the first entry and the last exit and never ranked, so one
    # window serves it: the one every name used to share.
    opening = start - timedelta(days=HISTORY_DAYS)
    if benchmark:
        windows[benchmark] = min(windows.get(benchmark, opening), opening)
    conn.register(
        "_backtest_window",
        pd.DataFrame({"ticker": list(windows), "since": list(windows.values())}),
    )
    try:
        frame = conn.execute(_BARS, {"until": until}).df()
    finally:
        conn.unregister("_backtest_window")
    factor = frame["closeadj"] / frame["close"]
    adjusted = pd.DataFrame(
        {
            "open": (frame["open"] * factor).round(PRECISION),
            "high": (frame["high"] * factor).round(PRECISION),
            "low": (frame["low"] * factor).round(PRECISION),
            "close": frame["closeadj"].round(PRECISION),
        }
    )
    frame["adjusted"] = adjusted["close"]
    frame = frame[frame["adjusted"] > 0]
    adjusted = adjusted.loc[frame.index]
    stamps = pd.to_datetime(frame["date"]).to_numpy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    series = {
        ticker: Series(
            dates=group["date"].tolist(),
            high=group["high"].to_numpy(dtype="float64"),
            low=group["low"].to_numpy(dtype="float64"),
            close=group["close"].to_numpy(dtype="float64"),
            volume=group["volume"].to_numpy(dtype="float64"),
            adjusted=group["adjusted"].to_numpy(dtype="float64"),
        )
        for ticker, group in frame.groupby("ticker", sort=True)
    }
    codes, names = pd.factorize(frame["ticker"], sort=True)
    fed = np.array([name in wanted for name in names], dtype="bool")[codes]
    if not fed.any():
        since = min(windows[name] for name in wanted)
        raise BacktestError(f"no bars for the ranked names between {since} and {until}")
    # The lead bars a name's own window added are history for the gates to count, not sessions
    # to replay: streaming them would put the name's first bar before the first signal session
    # and move the engine's own first session with it.
    opens = np.array(
        [np.datetime64(max(wanted.get(name, start), start)) for name in names],
        dtype="datetime64[ns]",
    )
    streamed = frame[fed & (stamps >= opens[codes])]
    bars = [
        _bar(row.ticker, row.date, *adjusted.loc[row.Index], row.volume)
        for row in streamed.sort_values(["date", "ticker"]).itertuples()
    ]
    return Feed(
        {ticker: s for ticker, s in series.items() if ticker in wanted},
        bars,
        frame["date"][fed].max(),
        benchmark=series.get(benchmark) if benchmark else None,
    )


def _bar(ticker: str, day: date, open_, high, low, close, volume) -> Bar:
    top = max(open_, high, low, close)
    bottom = min(open_, high, low, close)
    stamp = _stamp(day)
    return Bar(
        _bar_type(ticker),
        Price(float(open_), PRECISION),
        Price(float(top), PRECISION),
        Price(float(bottom), PRECISION),
        Price(float(close), PRECISION),
        Quantity.from_int(int(volume) if volume == volume else 0),
        stamp,
        stamp,
    )


def _bar_type(ticker: str) -> BarType:
    return BarType.from_str(f"{ticker}.{VENUE}-1-DAY-LAST-EXTERNAL")


def _equity(ticker: str) -> Equity:
    instrument_id = InstrumentId(Symbol(ticker), VENUE)
    return Equity(
        instrument_id=instrument_id,
        raw_symbol=Symbol(ticker),
        currency=USD,
        price_precision=PRECISION,
        price_increment=Price(10**-PRECISION, PRECISION),
        lot_size=Quantity.from_int(1),
        ts_event=0,
        ts_init=0,
    )


def _stamp(day: date, nanos: int = 0) -> int:
    at = datetime(day.year, day.month, day.day, CLOSE_HOUR_UTC, tzinfo=UTC)
    return int(pd.Timestamp(at).value) + nanos


def _day(stamp: int) -> date:
    return pd.Timestamp(stamp, unit="ns", tz="UTC").date()


def _require(conn: duckdb.DuckDBPyConnection) -> None:
    columns = {row[0] for row in conn.execute("DESCRIBE sep").fetchall()}
    missing = sorted(set(_NEEDED) - columns)
    if missing:
        raise BacktestError(f"the lake's sep lacks {missing}, which the backtest reads")


# The engine -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Fill:
    """One fill as the engine reported it: the session, the whole shares, the close they
    filled at, the fee charged and the order's participation in the name's daily dollar
    volume."""

    day: date
    ticker: str
    side: cost.Side
    qty: int
    price: float
    fee: float
    participation: float


@dataclass
class _Book:
    """What the strategy and the fee model share: the liquidity priced for each name at its
    last decision, every charge the fee model has made, and the orders that are legs of a
    corporate action rather than trades of ours, which are charged nothing."""

    multiple: float
    liquidity: dict[str, cost.Liquidity] = field(default_factory=dict)
    charges: list[tuple[str, float, float]] = field(default_factory=list)  # ticker, fee, take
    free: set = field(default_factory=set)  # the client order ids a corporate action forced


class _Fees(FeeModel):
    """NautilusTrader's fee model, charging each fill the live cost model's total: the spread
    floor and impact on both sides, and the regulatory fees on sells."""

    def __init__(self, book: _Book) -> None:
        super().__init__()
        self.book = book

    def get_commission(self, order, fill_qty, fill_px, instrument) -> Money:
        ticker = instrument.id.symbol.value
        if order.client_order_id in self.book.free:
            # A cash acquisition or a merger conversion: the position changes without a trade
            # of ours behind it, so there is no commission and no participation to report.
            self.book.charges.append((ticker, 0.0, 0.0))
            return Money(0, USD)
        liq = self.book.liquidity.get(ticker)
        if liq is None:
            raise BacktestError(f"{ticker} filled without its liquidity having been priced")
        shares = int(fill_qty)
        notional = shares * float(fill_px)
        side = cost.Side.BUY if order.side == OrderSide.BUY else cost.Side.SELL
        priced = cost.trade_cost(side, notional, shares, liq)
        commission = Money(priced.total_usd * self.book.multiple, USD)
        self.book.charges.append((ticker, commission.as_double(), notional / liq.adv_usd))
        return commission


class _Rebalancer(Strategy):
    """The strategy: the live rebalance code on a monthly clock."""

    def __init__(
        self,
        feed: Feed,
        months: Sequence[Month],
        rankings: Mapping[date, list[str]],
        book: _Book,
        options: Options,
        forced: Sequence[corporate.Event] = (),
    ) -> None:
        super().__init__(StrategyConfig())
        self.feed = feed
        self.months = list(months)
        self.ranked = rankings
        self.book = book
        self.options = options
        self.forced = tuple(forced)
        # The events that end a position by themselves, waiting for their effective date.
        self.terminating = corporate.terminations(self.forced)
        self.alerts: dict[str, tuple[str, Month]] = {}
        self.plans: dict[date, list[rebalance.Order]] = {}
        self.positions: dict[str, int] = {}
        self.cash = options.init_cash
        self.fills: list[Fill] = []
        self.counts: Counter[str] = Counter()
        self.pending: set = set()  # the client order ids of the rebalance's unfilled sells
        # The buys waiting on those sells: session, name, shares, and whether a corporate
        # action forced the leg, which is charged no commission.
        self.queued: list[tuple[date, str, int, bool]] = []
        self.seen: date | None = None  # the last session a bar arrived for
        self.account_final: float | None = None

    def on_start(self) -> None:
        for ticker in self.feed.series:
            self.subscribe_bars(_bar_type(ticker))
        for month in self.months:
            for phase, day in (("decide", month.snapshot_date), ("trade", month.entry)):
                name = f"{phase} {month.as_of}"
                self.alerts[name] = (phase, month)
                at = pd.Timestamp(_stamp(day, nanos=1), unit="ns", tz="UTC")
                self.clock.set_time_alert(name, at, self.on_alert)

    def on_alert(self, event) -> None:
        phase, month = self.alerts[event.name]
        if phase == "decide":
            self.decide(month)
        else:
            self.trade(month)

    def on_bar(self, bar: Bar) -> None:
        ticker = bar.bar_type.instrument_id.symbol.value
        day = _day(bar.ts_event)
        self.seen = day
        if ticker not in self.positions:
            return
        event = self.terminating.get(ticker)
        if event is not None and day >= event.effective_date:
            self.terminate(ticker, day, event)
            return
        if self.feed.last[ticker] == day and day < self.feed.end:
            # The name's last listed print: the forced exit, at this close.
            self.book.liquidity[ticker] = self.feed.liquidity(ticker, day)
            self.submit(ticker, OrderSide.SELL, self.positions[ticker])
            self.counts["delisting_exits"] += 1

    def terminate(self, ticker: str, day: date, event: corporate.Event) -> None:
        """A recorded cash acquisition or stock-for-stock merger, on its effective date: the
        position ends with no decision of ours and no commission, because a corporate action
        is not a trade. A merger's acquirer shares are queued against the proceeds the
        subject's shares raise, which is what the conversion comes to in cash; the cash a
        cash acquisition raises simply waits for the next rebalance."""
        del self.terminating[ticker]
        qty = self.positions[ticker]
        if event.kind is corporate.Kind.STOCK_MERGER:
            shares = int((qty * event.ratio).to_integral_value(rounding=ROUND_FLOOR))
            if shares <= 0:
                raise BacktestError(
                    f"{ticker}: the {event.period} merger converts {qty} shares into"
                    f" {shares} {event.into_symbol}"
                )
            self.queued.append((day, str(event.into_symbol), shares, True))
        self.pending.add(self.submit(ticker, OrderSide.SELL, qty, free=True))
        self.counts[f"{event.kind}_exits"] += 1

    def decide(self, month: Month) -> None:
        """At the close of the signal session: the live selection and order plan."""
        feed, options, day = self.feed, self.options, month.snapshot_date
        ranking = self.ranked.get(month.as_of, [])
        held = frozenset(self.positions)
        events = corporate.in_period(self.forced, month.as_of.strftime("%Y-%m"))
        divest = corporate.divest(events)
        prints: dict[str, Decimal] = {}
        # A halted incumbent and a terminated position come from the record; the rest is read
        # from the bars, exactly as `basket.propose` reads them from the lake.
        untradeable: set[str] = set(corporate.untradeable(events))
        for ticker in {*held, *ranking}:
            if ticker not in feed.series:
                continue  # never reached by the ranking; checked below if it is
            print_ = feed.print_at(ticker, day)
            counted = feed.series[ticker].through(day) >= LIQUIDITY_BARS
            delisting = feed.last[ticker] <= month.entry and feed.last[ticker] < feed.end
            if print_ is None or not counted or delisting:
                untradeable.add(ticker)
            else:
                prints[ticker] = Decimal(str(print_))
        settled = corporate.exits(events)
        for ticker in sorted((divest & held) - prints.keys()):
            # A name the record divests has to be priced even where the bars will not price
            # it: a delisted stub settles at the last listed print the operator recorded.
            price = settled.get(ticker)
            if price is None and ticker in feed.series:
                price = Decimal(str(feed.close_through(ticker, day)))
            if price is None:
                raise BacktestError(
                    f"{ticker}: a forced event divests it at {month.as_of} and it has no price"
                )
            prints[ticker] = price
        selection = rebalance.select(
            ranking,
            held,
            untradeable=untradeable,
            divest=divest,
            size=options.size,
            hold_through=options.hold_through,
        )
        unfed = [ticker for ticker in selection.buy if ticker not in feed.series]
        if unfed:
            depth = options.hold_through + options.size
            raise BacktestError(
                f"{month.as_of}: {unfed} selected from below the {depth} names fed per month;"
                " the ranking's top is unusually untradeable"
            )
        if not selection.buy and not selection.sell:
            self.plans[month.as_of] = []
            return
        equity = self.cash + sum(
            qty * feed.close_through(ticker, day) for ticker, qty in self.positions.items()
        )
        if equity <= 0:
            raise BacktestError(f"{month.as_of}: the account is worth {equity:.2f}")
        weight = rebalance.weights(selection.names, options.size)
        plan = rebalance.orders(
            selection, weight, prints, self.positions, Decimal(str(round(equity, 2)))
        )
        for order in plan:
            self.book.liquidity[order.symbol] = feed.liquidity(order.symbol, day)
        self.plans[month.as_of] = plan

    def trade(self, month: Month) -> None:
        """At the close of the trade session: the plan's sells, then its buys."""
        day = month.entry
        plan = self.plans.pop(month.as_of, [])
        self.queued.extend(
            (day, order.symbol, order.qty, False)
            for order in plan
            if order.side is rebalance.Side.BUY
        )
        for order in plan:
            if order.side is not rebalance.Side.SELL:
                continue
            if self.feed.close_at(order.symbol, day) is None:
                self.counts["unfilled"] += 1  # no print this session; the name stays held
                continue
            self.pending.add(self.submit(order.symbol, OrderSide.SELL, order.qty))
        if not self.pending:
            self.buy()

    def buy(self) -> None:
        """Submit the queued buys, each trimmed to the cash still free after the ones before
        it, with the fee it will be charged counted in. A leg a corporate action forced is
        charged nothing, and is trimmed only where the proceeds will not cover it."""
        free = self.portfolio.account(VENUE).balance_free(USD).as_double()
        queued, self.queued = self.queued, []
        for day, ticker, wanted, no_fee in queued:
            price = self.feed.close_at(ticker, day)
            if price is None:
                self.counts["unfilled"] += 1
                continue
            liq = None if no_fee else self.book.liquidity[ticker]
            qty = wanted
            fee = 0.0 if liq is None else self.fee(cost.Side.BUY, qty, price, liq)
            while qty > 0 and qty * price + fee > free:
                qty = min(qty - 1, int((free - fee) // price))
                fee = self.fee(cost.Side.BUY, qty, price, liq) if qty > 0 and liq else 0.0
            if qty <= 0:
                self.counts["unfilled"] += 1
                continue
            if qty < wanted:
                self.counts["trimmed"] += 1
            self.submit(ticker, OrderSide.BUY, qty, free=no_fee)
            free -= qty * price + fee

    def fee(self, side: cost.Side, qty: int, price: float, liq: cost.Liquidity) -> float:
        """What the fee model will charge the fill, plus a cent of rounding headroom."""
        priced = cost.trade_cost(side, qty * price, qty, liq)
        return Money(priced.total_usd * self.options.cost_multiple, USD).as_double() + 0.01

    def submit(self, ticker: str, side: OrderSide, qty: int, *, free: bool = False):
        order = self.order_factory.market(
            InstrumentId(Symbol(ticker), VENUE),
            side,
            Quantity.from_int(qty),
            time_in_force=TimeInForce.DAY,
        )
        if free:
            self.book.free.add(order.client_order_id)
        self.submit_order(order)
        return order.client_order_id

    def on_order_filled(self, event) -> None:
        ticker = event.instrument_id.symbol.value
        day = _day(event.ts_event)
        qty, price = int(event.last_qty), float(event.last_px)
        fee = event.commission.as_double()
        expected = self.feed.close_at(ticker, day)
        if expected is None or abs(price - expected) > 10**-PRECISION:
            raise BacktestError(f"{ticker} filled at {price} on {day}, whose close is {expected}")
        charged, take = self.book.charges[len(self.fills)][1:]
        if abs(charged - fee) > 0.005:
            raise BacktestError(f"{ticker}: charged {fee}, the cost model priced {charged}")
        if event.order_side == OrderSide.BUY:
            side = cost.Side.BUY
            self.positions[ticker] = self.positions.get(ticker, 0) + qty
            self.cash -= qty * price + fee
        else:
            side = cost.Side.SELL
            left = self.positions.get(ticker, 0) - qty
            if left < 0:
                raise BacktestError(f"{ticker}: sold {qty} holding {left + qty}")
            if left:
                self.positions[ticker] = left
            else:
                self.positions.pop(ticker, None)
            self.cash += qty * price - fee
        self.fills.append(Fill(day, ticker, side, qty, price, fee, take))
        self.pending.discard(event.client_order_id)
        if not self.pending and self.queued:
            self.buy()

    def on_stop(self) -> None:
        self.account_final = self.portfolio.account(VENUE).balance_total(USD).as_double()


@dataclass(frozen=True)
class Simulation:
    """One run's fills in order, its counts of trimmed and unfilled orders and delisting
    exits, and the positions left at the end."""

    fills: list[Fill]
    counts: Counter
    positions: dict[str, int]


def simulate(
    feed: Feed,
    months: Sequence[Month],
    rankings: Mapping[date, list[str]],
    options: Options,
    forced: Sequence[corporate.Event] = (),
) -> Simulation:
    """One run of the strategy over `months`, from `options.init_cash`, with the recorded
    corporate events in `forced` applied as they fall due."""
    start, end = months[0].snapshot_date, months[-1].exit
    bars = feed.bars(start, end)
    if not bars:
        raise BacktestError(f"no bars to replay between {start} and {end}")
    book = _Book(multiple=options.cost_multiple)
    strategy = _Rebalancer(feed, months, rankings, book, options, forced)
    config = BacktestEngineConfig(
        logging=LoggingConfig(bypass_logging=True), risk_engine=RiskEngineConfig(bypass=True)
    )
    engine = BacktestEngine(config)
    try:
        engine.add_venue(
            venue=VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            starting_balances=[Money(options.init_cash, USD)],
            fee_model=_Fees(book),
            fill_model=BestPriceFillModel(),
        )
        for instrument in feed.instruments():
            engine.add_instrument(instrument)
        engine.add_data(list(bars))
        engine.add_strategy(strategy)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            engine.run()
    finally:
        engine.dispose()
    final = feed.last_session(start, end)
    if strategy.seen != final:
        raise BacktestError(
            f"the engine stopped after {strategy.seen}, before {final}; a buy the account could"
            " not fund is the usual cause"
        )
    if strategy.account_final is None or abs(strategy.account_final - strategy.cash) > 0.01 * (
        len(strategy.fills) + 1
    ):
        raise BacktestError(
            f"the account holds {strategy.account_final}, not the {strategy.cash:.2f} the fills"
            " add up to"
        )
    if len(book.charges) != len(strategy.fills):
        raise BacktestError(f"{len(book.charges)} charges for {len(strategy.fills)} fills")
    return Simulation(strategy.fills, strategy.counts, dict(strategy.positions))


# The report -----------------------------------------------------------------------------------


def _report(
    outcome: Simulation,
    feed: Feed,
    months: Sequence[Month],
    options: Options,
    forced: Sequence[corporate.Event] = (),
) -> dict[str, Any]:
    marks, fees_between, traded = _marks(outcome.fills, feed, months, options)
    net = [marks[i + 1] / marks[i] - 1.0 for i in range(len(months))]
    gross = [(marks[i + 1] + fees_between[i]) / marks[i] - 1.0 for i in range(len(months))]
    fees_paid = sum(fill.fee for fill in outcome.fills)
    takes = [fill.participation for fill in outcome.fills]
    return {
        **_metrics(net, marks, options),
        "gross_return": (marks[-1] + fees_paid) / options.init_cash - 1.0,
        "fees_paid": fees_paid,
        "fees_fraction": fees_paid / options.init_cash,
        "turnover": float(np.mean([traded[i] / marks[i] for i in range(len(months))])),
        "participation_max": max(takes) if takes else None,
        "participation_mean": float(np.mean(takes)) if takes else None,
        "orders": len(outcome.fills),
        "unfilled": outcome.counts["unfilled"],
        "trimmed": outcome.counts["trimmed"],
        "delisting_exits": outcome.counts["delisting_exits"],
        **corporate.counts(forced),
        **{
            f"{kind}_exits": outcome.counts[f"{kind}_exits"]
            for kind in (corporate.Kind.CASH_ACQUISITION, corporate.Kind.STOCK_MERGER)
        },
        "final_value": marks[-1],
        "after_tax": _after_tax(outcome, feed, months, options, marks[-1]),
        "monthly": [
            {"as_of": month.as_of.isoformat(), "net": n, "gross": g}
            for month, n, g in zip(months, net, gross, strict=True)
        ],
    }


def _after_tax(
    outcome: Simulation,
    feed: Feed,
    months: Sequence[Month],
    options: Options,
    final_value: float,
    *,
    benchmark: str = BENCHMARK,
) -> dict[str, Any]:
    """The approximate UK after-tax view of the run, beside the same treatment of buying the
    benchmark once and holding it.

    The operator is a UK tax resident and this strategy realises nearly all of its gains every
    year, where holding an index defers the whole bill to one disposal, so the pre-tax return
    is not the comparison that matters to him. `agb.research.tax` does the arithmetic and
    states its simplifications; they are carried in the row with the numbers, because a reader
    who sees a tax figure without them will take it for a computation. It is a diagnostic:
    certification reads none of it, and the specification is explicit that tax, which depends
    on the operator's other income and allowance usage rather than on the strategy, cannot
    enter a gate."""
    init_cash = Decimal(str(options.init_cash))
    return {
        "approximate": True,
        "basis": "approximate: UK capital gains tax at the higher rate on realised gains, in"
        " USD, as a diagnostic of what the turnover costs after tax; it certifies nothing and"
        " no gate reads it",
        "rate": float(tax.RATE),
        "annual_exempt_amount": float(tax.EXEMPT_AMOUNT),
        "simplifications": list(tax.SIMPLIFICATIONS),
        "strategy": tax.view(
            _disposed(outcome, feed, months[-1].exit), init_cash, Decimal(str(final_value))
        ),
        "benchmark": benchmark,
        "buy_and_hold": _buy_and_hold(feed, months, init_cash),
    }


def _disposed(outcome: Simulation, feed: Feed, day: date) -> list[tax.Trade]:
    """The run's fills as trades to be taxed, with whatever is still held on `day` deemed
    disposed of at that session's close and charged no dealing cost, so the bill covers the
    same final value the report marks. The fills keep their order within a session, where a
    sell funds the buys that follow it, because a Section 104 pool is a running average."""
    trades = [
        tax.Trade(
            day=fill.day,
            ticker=fill.ticker,
            side=fill.side,
            qty=fill.qty,
            price=Decimal(str(fill.price)),
            fee=Decimal(str(fill.fee)),
        )
        for fill in sorted(outcome.fills, key=lambda fill: fill.day)
    ]
    return trades + [
        tax.Trade(
            day=day,
            ticker=ticker,
            side=cost.Side.SELL,
            qty=qty,
            price=_mark(feed.series[ticker], day),
            fee=Decimal(0),
        )
        for ticker, qty in sorted(outcome.positions.items())
        if qty
    ]


def _buy_and_hold(feed: Feed, months: Sequence[Month], init_cash: Decimal) -> dict[str, Any] | None:
    """The benchmark bought once at the first trade session's close with the same starting
    cash, in whole shares with the rest left in cash, held to the final exit and deemed
    disposed of there: one disposal, one year's allowance, and no dealing cost either way.

    None when the lake holds no bars for the benchmark over these months, which a lake of
    equity prices alone will not: the comparison is a diagnostic and its absence is not a
    reason to fail a run, so the report says which name it wanted and leaves the row empty."""
    series = feed.benchmark
    if series is None:
        return None
    entry, exit_day = months[0].entry, months[-1].exit
    start, end = _mark(series, entry, missing=True), _mark(series, exit_day, missing=True)
    if start is None or end is None or start <= 0:
        return None
    qty = int((init_cash / start).to_integral_value(rounding=ROUND_FLOOR))
    if qty < 1:
        return None
    trades = [
        tax.Trade(entry, BENCHMARK, cost.Side.BUY, qty, start, Decimal(0)),
        tax.Trade(exit_day, BENCHMARK, cost.Side.SELL, qty, end, Decimal(0)),
    ]
    final_value = init_cash - qty * start + qty * end
    return {"shares": qty, **tax.view(trades, init_cash, final_value)}


def _mark(series: Series, day: date, *, missing: bool = False) -> Decimal | None:
    """The last adjusted close on or before `day` as a Decimal, for the tax arithmetic, which
    is in Decimal throughout. `missing` allows a name that had not started printing by `day`,
    which only the benchmark can be."""
    n = series.through(day)
    if n == 0:
        if missing:
            return None
        raise BacktestError(f"{series.dates[0]} is the first bar, after {day}")
    return Decimal(str(float(series.adjusted[n - 1])))


def _metrics(returns: list[float], marks: list[float], options: Options) -> dict[str, Any]:
    """The scan's summary of an equity curve, from the marks and the monthly returns between
    them."""
    months = len(returns)
    total = marks[-1] / options.init_cash - 1.0
    annual = (1.0 + total) ** (MONTHS_PER_YEAR / months) - 1.0 if months else 0.0
    vol = float(np.std(returns, ddof=1) * np.sqrt(MONTHS_PER_YEAR)) if months > 1 else None
    peaks = np.maximum.accumulate(marks)
    return {
        "months": months,
        "total_return": total,
        "annual_return": float(annual),
        "annual_volatility": vol,
        "sharpe": None if not vol else float(annual / vol),
        "max_drawdown": float((np.asarray(marks) / peaks - 1.0).min()),
    }


def _marks(
    fills: Sequence[Fill], feed: Feed, months: Sequence[Month], options: Options
) -> tuple[list[float], list[float], list[float]]:
    """The fills replayed: the account marked at the close of the first signal session, of
    every trade session after that session's fills, and of the final exit; the fees paid
    between each pair of marks; and the two-sided notional traded at each trade session."""
    days = [months[0].snapshot_date] + [month.entry for month in months] + [months[-1].exit]
    marks: list[float] = []
    fees_between: list[float] = []
    traded: list[float] = []
    cash, positions = options.init_cash, {}
    ordered = sorted(fills, key=lambda fill: fill.day)
    i = 0
    for day in days:
        fees, notional = 0.0, 0.0
        while i < len(ordered) and ordered[i].day <= day:
            fill = ordered[i]
            signed = fill.qty if fill.side is cost.Side.BUY else -fill.qty
            positions[fill.ticker] = positions.get(fill.ticker, 0) + signed
            cash += -signed * fill.price - fill.fee
            fees += fill.fee
            if fill.day == day:
                notional += fill.qty * fill.price
            i += 1
        positions = {ticker: qty for ticker, qty in positions.items() if qty}
        value = cash + sum(
            qty * feed.close_through(ticker, day) for ticker, qty in positions.items()
        )
        marks.append(value)
        fees_between.append(fees)
        traded.append(notional)
    # The first mark is the starting cash; the returns run from trade session to trade session.
    return marks[1:], fees_between[2:], traded[1:-1]


def _sensitivity(
    feed: Feed,
    months: Sequence[Month],
    rankings: Mapping[date, list[str]],
    options: Options,
    forced: Sequence[corporate.Event] = (),
) -> dict[str, Any]:
    outcome = simulate(feed, months, rankings, options, forced)
    marks, _, _ = _marks(outcome.fills, feed, months, options)
    net = [marks[i + 1] / marks[i] - 1.0 for i in range(len(months))]
    return {"start": months[0].as_of.isoformat(), **_metrics(net, marks, options)}


def _summary(paths: list[dict[str, Any]]) -> dict[str, Any]:
    totals = [path["total_return"] for path in paths]
    sharpes = [path["sharpe"] for path in paths if path["sharpe"] is not None]
    return {
        "paths": len(paths),
        "total_return_mean": float(np.mean(totals)),
        "total_return_min": min(totals),
        "total_return_max": max(totals),
        "sharpe_mean": float(np.mean(sharpes)) if sharpes else None,
    }
