"""Fast scans of a signal with vectorbt, rebalanced by the live rules and costed with the
live cost model.

A scan ranks the panel's names by one signal each month and hands the ranking to
`agb.rebalance.select`, so a scan holds what the live basket would hold: `size` names at a
fixed weight of 1/`size`, bought from the top of the ranking and kept until they fall below
the hold-through rank, taken in or out whole and never traded back to weight. Orders fill at
the adjusted close of each month's entry session, exactly where the panel's labels are
measured. The signal is a panel column: one of the shared features, or a model score joined
onto the panel.

Every order's fee is `cost.trade_cost` over `cost.liquidity` on the sixty sessions through the
snapshot, the same numbers the pre-trade gate will apply, so a scan's return is net of what
the basket would actually pay. The order's notional is estimated as the name's weight of the
initial capital, since the cost model's impact term needs it before vectorbt sizes the order.

Each scan is a trial, like a model fit: its options, its signal and its results append a
row."""

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from agb import cost, features, lake, rebalance
from agb.research.panel import Panel
from agb.research.trials import Ledger, Run

KIND = "scan"
MONTHS_PER_YEAR = 12
# The cost model needs one bar more than its window for the two-day spread estimator.
LIQUIDITY_BARS = cost.CURRENT.window + 1
_NEEDED = ("ticker", "date", "high", "low", "close", "closeadj", "volume")
_LIQUIDITY = f"""
SELECT ticker, high, low, close, volume
FROM (
    SELECT ticker, date, high, low, close, volume,
           row_number() OVER (PARTITION BY ticker ORDER BY date DESC) AS n
    FROM sep
    WHERE date <= $session AND ticker IN (SELECT ticker FROM _scan_names)
)
WHERE n <= {LIQUIDITY_BARS}
ORDER BY ticker, date
"""


class ScanError(Exception):
    pass


@dataclass(frozen=True)
class Options:
    signal: str = "mom_12_1"
    size: int = rebalance.SIZE
    hold_through: int = rebalance.HOLD_THROUGH
    init_cash: float = 1_000_000.0

    def __post_init__(self) -> None:
        if self.size < 1:
            raise ValueError("size must be at least one name")
        if self.size > self.hold_through:
            raise ValueError(f"size {self.size} exceeds the hold-through rank {self.hold_through}")
        if self.init_cash <= 0:
            raise ValueError("init_cash must be positive")


@dataclass(frozen=True)
class Orders:
    """What vectorbt is handed: prices at every session, and at every entry session a target
    weight for each name bought or sold (NaN leaves a name alone) and its fee fraction."""

    prices: pd.DataFrame
    weights: pd.DataFrame
    fees: pd.DataFrame


@dataclass(frozen=True)
class Simulation:
    results: dict[str, Any]
    trades: pd.DataFrame  # vectorbt's order records, one row per fill


def run(
    panel: Panel,
    options: Options,
    ledger: Ledger,
    *,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
    scores: pd.Series | None = None,
) -> Run:
    """Scan `options.signal` over the panel. `scores`, aligned to the panel's rows, stands in
    for a signal that is not a feature: a model's prediction, for example."""
    frame = panel.frame
    if scores is not None:
        frame = frame.assign(**{options.signal: np.asarray(scores, dtype="float64")})
    elif options.signal not in features.FEATURES:
        raise ScanError(f"{options.signal!r} is not a feature; pass scores for a model signal")
    months = panel.months
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
            orders = build_orders(frame, panel, options, conn)
        finally:
            if db is None:
                conn.close()
        trial.results = simulate(orders, options).results
        return Run(trial.trial_id, trial.results)


def ranking(month_rows: pd.DataFrame, options: Options) -> list[str]:
    """The month's names best first by the signal, ties broken by ticker. A name without a
    signal value is out of the ranking, so it is never bought and is sold if held."""
    ranked = month_rows.dropna(subset=[options.signal])
    ordered = ranked.sort_values([options.signal, "ticker"], ascending=[False, True])
    return ordered["ticker"].tolist()


def build_orders(
    frame: pd.DataFrame, panel: Panel, options: Options, conn: duckdb.DuckDBPyConnection
) -> Orders:
    _require(conn)
    months = panel.months
    tickers = sorted(frame["ticker"].unique().tolist())
    sessions = [month.entry for month in months] + [months[-1].exit]
    index = pd.DatetimeIndex(pd.to_datetime(sessions))
    prices = _prices(conn, tickers, sessions).reindex(index=index, columns=tickers).ffill()
    weights = pd.DataFrame(np.nan, index=index, columns=tickers)
    fees = pd.DataFrame(0.0, index=index, columns=tickers)
    weight = float(rebalance.weights(["x"], options.size)["x"])
    held: frozenset[str] = frozenset()
    for month in months:
        rows = frame[frame["as_of"] == month.as_of]
        selection = rebalance.select(
            ranking(rows, options), held, size=options.size, hold_through=options.hold_through
        )
        sides = {
            **dict.fromkeys(selection.buy, cost.Side.BUY),
            **dict.fromkeys(selection.sell, cost.Side.SELL),
        }
        liquidity = _liquidity(conn, list(sides), month.snapshot_date)
        at = pd.Timestamp(month.entry)
        for ticker, side in sides.items():
            price = prices.at[at, ticker] if ticker in prices.columns else np.nan
            if ticker not in liquidity or not np.isfinite(price) or price <= 0:
                raise ScanError(f"{ticker} has no price or liquidity at {month.entry}")
            notional = options.init_cash * weight
            traded = cost.trade_cost(side, notional, notional / price, liquidity[ticker])
            weights.at[at, ticker] = weight if side is cost.Side.BUY else 0.0
            fees.at[at, ticker] = traded.total_bps / cost.BPS
        held = frozenset(selection.names)
    return Orders(prices=prices, weights=weights, fees=fees)


def simulate(orders: Orders, options: Options) -> Simulation:
    """Run the orders through vectorbt and summarise the equity curve."""
    import vectorbt as vbt

    portfolio = vbt.Portfolio.from_orders(
        close=orders.prices,
        size=orders.weights,
        size_type="targetpercent",
        fees=orders.fees,
        init_cash=options.init_cash,
        cash_sharing=True,
        group_by=True,
        call_seq="auto",
        freq="30d",
    )
    value = portfolio.value()
    value = value.iloc[:, 0] if isinstance(value, pd.DataFrame) else value
    returns = value.pct_change().dropna()
    months = len(returns)
    total = float(value.iloc[-1] / options.init_cash - 1.0)
    annual = (1.0 + total) ** (MONTHS_PER_YEAR / months) - 1.0 if months else 0.0
    vol = float(returns.std(ddof=1) * np.sqrt(MONTHS_PER_YEAR)) if months > 1 else None
    drawdown = float((value / value.cummax() - 1.0).min())
    trades = portfolio.orders.records_readable
    # Under a shared cash group vectorbt names each column (group, ticker); keep the ticker.
    trades["Column"] = [col[-1] if isinstance(col, tuple) else col for col in trades["Column"]]
    fees_paid = float(trades["Fees"].sum())
    results = {
        "months": months,
        "total_return": total,
        "annual_return": float(annual),
        "annual_volatility": vol,
        "sharpe": None if not vol else float(annual / vol),
        "max_drawdown": drawdown,
        "fees_paid": fees_paid,
        "fees_fraction": fees_paid / options.init_cash,
        "orders": int(len(trades)),
        "final_value": float(value.iloc[-1]),
    }
    return Simulation(results, trades)


def _prices(conn, tickers: list[str], sessions: list[date]) -> pd.DataFrame:
    conn.register("_scan_names", pd.DataFrame({"ticker": tickers}))
    conn.register("_scan_sessions", pd.DataFrame({"date": pd.to_datetime(sessions).date}))
    try:
        rows = conn.execute(
            "SELECT date, ticker, closeadj FROM sep"
            " WHERE ticker IN (SELECT ticker FROM _scan_names)"
            " AND date IN (SELECT date FROM _scan_sessions) AND closeadj > 0"
        ).fetchall()
    finally:
        conn.unregister("_scan_names")
        conn.unregister("_scan_sessions")
    frame = pd.DataFrame(rows, columns=["date", "ticker", "closeadj"])
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.pivot(index="date", columns="ticker", values="closeadj").astype("float64")


def _liquidity(conn, tickers: list[str], session: date) -> dict[str, cost.Liquidity]:
    if not tickers:
        return {}
    conn.register("_scan_names", pd.DataFrame({"ticker": tickers}))
    try:
        rows = conn.execute(_LIQUIDITY, {"session": session}).fetchall()
    finally:
        conn.unregister("_scan_names")
    bars: dict[str, list[tuple]] = {}
    for ticker, *bar in rows:
        bars.setdefault(ticker, []).append(tuple(bar))
    result = {}
    for ticker, series in bars.items():
        if len(series) < LIQUIDITY_BARS:
            continue
        high, low, close, volume = (list(column) for column in zip(*series, strict=True))
        result[ticker] = cost.liquidity(high, low, close, volume)
    return result


def _require(conn: duckdb.DuckDBPyConnection) -> None:
    columns = {row[0] for row in conn.execute("DESCRIBE sep").fetchall()}
    missing = sorted(set(_NEEDED) - columns)
    if missing:
        raise ScanError(f"the lake's sep lacks {missing}, which the cost model reads")
