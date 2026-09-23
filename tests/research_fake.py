"""A lake for the research tests: a few dozen names over real NYSE sessions spanning several
years, with every column the universe, the features, the labels and the cost model read.

Each name's adjusted close follows its own drift plus seeded noise, so the panel's grades are
spread across every quintile and a ranker has something to learn. Each name files a quarter's
statements, as reported (ARQ for the share count the universe reads, ART for the trailing
figures the features read), forty days after each calendar quarter, so every fundamental
feature is populated.

The default span starts a year and a half before the first panel month, so the momentum and
volatility features have the trailing year they need from the first month-end on."""

import math
import random
from datetime import date, timedelta

import pyarrow as pa

from agb import features, lake, nyse

START = date(2022, 6, 1)
END = date(2026, 8, 31)
PANEL_START = date(2024, 1, 1)
COMMON = "Domestic Common Stock"
NAMES = 24

_SEP = pa.schema([
    ("ticker", pa.string()), ("date", pa.date32()), ("open", pa.float64()),
    ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("closeadj", pa.float64()), ("closeunadj", pa.float64()), ("volume", pa.float64()),
])  # fmt: skip
_SF1 = pa.schema([
    ("ticker", pa.string()), ("dimension", pa.string()), ("calendardate", pa.date32()),
    ("datekey", pa.date32()), ("lastupdated", pa.date32()), ("sharesbas", pa.float64()),
    ("sharefactor", pa.float64()), ("netinc", pa.float64()), ("revenue", pa.float64()),
    ("fcf", pa.float64()), ("gp", pa.float64()), ("assets", pa.float64()),
    ("equity", pa.float64()), ("debt", pa.float64()), ("cashneq", pa.float64()),
    ("liabilities", pa.float64()),
])  # fmt: skip
_TICKERS = pa.schema([
    ("table", pa.string()), ("permaticker", pa.int64()), ("ticker", pa.string()),
    ("category", pa.string()), ("industry", pa.string()), ("siccode", pa.string()),
    ("sector", pa.string()),
])  # fmt: skip


def quarter_ends(start: date, end: date) -> list[date]:
    """The calendar quarter-ends from the one before `start` up to `end`."""
    result = []
    year, month = start.year, 3 * ((start.month - 1) // 3)
    if month == 0:
        year, month = year - 1, 12
    while True:
        day = date(year, month, 1) + timedelta(days=32)
        day = day.replace(day=1) - timedelta(days=1)
        if day > end:
            return result
        result.append(day)
        year, month = (year, month + 3) if month < 12 else (year + 1, 3)


class Market:
    """Names default to values that clear every universe filter."""

    def __init__(self, root, start: date = START, end: date = END, *, seed: int = 0):
        self.root = root
        self.start = start
        self.end = end
        self.sessions = nyse.NyseCalendar(start, end).sessions(start, end)
        self.random = random.Random(seed)
        self.rows = {"sep": [], "sf1": [], "tickers": []}
        self.permaticker = 100_000

    def name(
        self,
        ticker,
        *,
        drift=0.0,
        noise=0.01,
        price=20.0,
        volume=1_000_000.0,
        shares=100_000_000.0,
        margin=0.1,
        sector=None,
        category=COMMON,
    ):
        """A name whose adjusted close starts at `price` and moves by `drift` plus a normal
        noise of `noise` per session, filing statements every quarter at a `margin` of its
        revenue."""
        close = price
        for day in self.sessions:
            close *= math.exp(drift + self.random.gauss(0.0, noise))
            self.rows["sep"].append(
                (ticker, day, close, close * 1.01, close * 0.99, close, close, close, volume)
            )
        revenue = 4.0 * shares
        for quarter in quarter_ends(self.start, self.end):
            filed = quarter + timedelta(days=40)
            figures = {
                "netinc": margin * revenue,
                "revenue": revenue,
                "fcf": margin * revenue * 0.8,
                "gp": 0.4 * revenue,
                "assets": 3.0 * revenue,
                "equity": 1.5 * revenue,
                "debt": 0.5 * revenue,
                "cashneq": 0.2 * revenue,
                "liabilities": 1.5 * revenue,
            }
            for dimension in ("ARQ", "ART"):
                self.rows["sf1"].append(
                    (ticker, dimension, quarter, filed, filed, shares, 1.0, *figures.values())
                )
        self.permaticker += 1
        self.rows["tickers"].append(
            (
                "stocks",
                self.permaticker,
                ticker,
                category,
                "Software",
                "7372",
                sector or features.SECTORS[self.permaticker % len(features.SECTORS)],
            )
        )
        return self

    def names(self, count=NAMES, *, prefix="N", **kwargs):
        """`count` names N01, N02, ... with drifts spread from +0.1% to -0.1% per session, so
        the top of the alphabet tends to outperform the bottom."""
        for i in range(1, count + 1):
            drift = 0.001 - 0.002 * (i - 1) / max(count - 1, 1)
            self.name(f"{prefix}{i:02d}", drift=drift, **kwargs)
        return self

    def land(self):
        for dataset, schema in ((lake.SEP, _SEP), (lake.SF1, _SF1), (lake.TICKERS, _TICKERS)):
            rows = self.rows[dataset.name]
            table = pa.Table.from_pylist(
                [dict(zip(schema.names, row, strict=True)) for row in rows], schema=schema
            )
            lake.land(self.root, dataset, table, self.end)
        return self.root
