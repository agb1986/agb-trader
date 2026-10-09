"""A lake built a name at a time, over real NYSE sessions, with every column the universe, the
tests' stand-in ranker (tests/registry_fake.py) and the cost model read. Prices follow a
per-name growth rate, so a test chooses a name's rank by choosing its growth, and every name's
last close is its `price`, so order sizes are easy to predict."""

from datetime import date, timedelta

import pyarrow as pa

from agb import lake, nyse

# The last session of September 2026, the signal session for October's rebalance, and the
# trade session after it.
SIGNAL = date(2026, 9, 30)
TRADE = date(2026, 10, 1)
PERIOD = "2026-10"
COMMON = "Domestic Common Stock"
# The sector the debate roles are told, which the panel reads from the lake.
SECTOR = "Technology"
SESSIONS = 90

_SEP = pa.schema([
    ("ticker", pa.string()), ("date", pa.date32()), ("open", pa.float64()),
    ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("closeunadj", pa.float64()), ("volume", pa.float64()),
])  # fmt: skip
_SF1 = pa.schema([
    ("ticker", pa.string()), ("dimension", pa.string()), ("datekey", pa.date32()),
    ("lastupdated", pa.date32()), ("sharesbas", pa.float64()), ("sharefactor", pa.float64()),
])  # fmt: skip
_TICKERS = pa.schema([
    ("table", pa.string()), ("permaticker", pa.int64()), ("ticker", pa.string()),
    ("category", pa.string()), ("sector", pa.string()), ("industry", pa.string()),
    ("siccode", pa.string()),
])  # fmt: skip


def sessions(end: date = SIGNAL, count: int = SESSIONS) -> list[date]:
    """The `count` NYSE sessions ending at `end`, oldest first."""
    start = end - timedelta(days=2 * count + 7)
    return nyse.NyseCalendar(start, end).sessions(start, end)[-count:]


class Market:
    """Names default to values that clear every universe filter."""

    def __init__(self, root, end: date = SIGNAL, count: int = SESSIONS):
        self.root = root
        self.sessions = sessions(end, count)
        self.end = end
        self.rows = {"sep": [], "sf1": [], "tickers": []}
        self.permaticker = 100_000

    def name(
        self,
        ticker,
        *,
        growth=0.0,
        price=20.0,
        volume=1_000_000.0,
        halted=(),
        shares=100_000_000.0,
        category=COMMON,
        sector=SECTOR,
    ):
        """A name whose close grows by `growth` per session to end at `price`, with no print on
        any session in `halted`."""
        last = len(self.sessions) - 1
        for i, day in enumerate(self.sessions):
            if day in halted:
                continue
            close = price * (1.0 + growth) ** (i - last)
            self.rows["sep"].append(
                (ticker, day, close, close * 1.01, close * 0.99, close, close, volume)
            )
        self.rows["sf1"].append((ticker, "ARQ", self.sessions[0], self.end, shares, 1.0))
        self.permaticker += 1
        self.rows["tickers"].append(
            ("stocks", self.permaticker, ticker, category, sector, "Software", "7372")
        )
        return self

    def names(self, count, *, prefix="N", **kwargs):
        """`count` names N01, N02, ... whose momentum falls with their number, so N01 ranks
        first."""
        for i in range(1, count + 1):
            self.name(f"{prefix}{i:02d}", growth=0.01 - i * 0.0002, **kwargs)
        return self

    def land(self):
        for dataset, schema in ((lake.SEP, _SEP), (lake.SF1, _SF1), (lake.TICKERS, _TICKERS)):
            rows = self.rows[dataset.name]
            table = pa.Table.from_pylist(
                [dict(zip(schema.names, row, strict=True)) for row in rows], schema=schema
            )
            lake.land(self.root, dataset, table, self.end)
        return self.root
