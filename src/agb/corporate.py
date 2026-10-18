"""Forced corporate events: what the market does to a holding without the system asking.

The ranker defines four of them (docs/architecture/02-classical-ranker.md), and research and
live must handle each the same way, so the rules live here and both callers import them:

  * **Cash acquisition.** The position terminates at the deal price. The cash waits for the
    next rebalance rather than being redeployed mid-month, because redeploying is an
    off-cadence decision with no signal behind it.
  * **Stock-for-stock merger.** The position terminates and acquirer shares arrive in its
    place. They are sold at the next rebalance whatever the acquirer's rank, because that
    position was never chosen.
  * **Delisting to OTC.** The name leaves the listed market and the position exits at the
    last listed print. It is modelled, not silently dropped.
  * **Trading halt spanning a rebalance.** The position is carried: it keeps its slot, gets
    no order, and its replacement is skipped, so one fewer name trades that month.

Which of the two sets a name falls into is the whole of it. A name that **cannot be traded**
is passed to `rebalance.select` as untradeable: a halt is carried, and a terminated position
cannot be bought back. A name that **must be sold whatever its rank** is passed as divest: a
delisted stub and the acquirer shares a merger paid in. Everything after that is the ordinary
rebalance, which is how the backtest and the live pipeline stay identical.

Nothing here is inferred. A name that has quietly stopped printing looks exactly like a
halted one, and the difference is a human's judgement: the operator records the event with
`agb corporate` once the broker's statement or the exchange notice is in hand. The record is
what makes a short basket legitimate at the gate and what keeps the snapshot mismatch alert
quiet for the share change the event caused.

An event is filed against the rebalance period it bears on, not the period it happened in: a
delisting in October that the November rebalance must sell out of is a November event with an
October effective date.
"""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Any

import psycopg

from agb import approval

PERIOD = re.compile(r"^\d{4}-\d{2}$")
VERB = "corporate"


class CorporateError(Exception):
    """An event the record will not take."""


class Kind(StrEnum):
    CASH_ACQUISITION = "cash_acquisition"
    STOCK_MERGER = "stock_merger"
    DELISTING = "delisting"
    HALT = "halt"


@dataclass(frozen=True)
class Event:
    """One forced event as the operator recorded it. `shares` is the holding the event found;
    what it left is `shares_after`, which the kind decides rather than the operator."""

    period: str  # the rebalance the event bears on, YYYY-MM
    symbol: str
    kind: Kind
    effective_date: date
    shares: Decimal
    detail: str  # where the operator learned the fact
    price_usd: Decimal | None = None  # the deal price, or the last listed print
    into_symbol: str | None = None  # the acquirer, for a stock-for-stock merger
    into_shares: Decimal | None = None

    def __post_init__(self) -> None:
        # The same rules the table's checks enforce, so a bad event is refused before it
        # reaches the database and reads the same from either side.
        if not PERIOD.match(self.period):
            raise CorporateError(f"{self.period} is not a period of the form YYYY-MM")
        try:
            object.__setattr__(self, "kind", Kind(self.kind))
        except ValueError:
            kinds = ", ".join(Kind)
            raise CorporateError(f"{self.kind} is not a forced event; they are {kinds}") from None
        if not self.symbol:
            raise CorporateError("an event needs a symbol")
        if self.shares <= 0:
            raise CorporateError(f"{self.symbol}: the holding before the event must be positive")
        if not self.detail:
            raise CorporateError(f"{self.symbol}: say where the fact came from")
        if (self.into_symbol is None) != (self.into_shares is None):
            raise CorporateError(f"{self.symbol}: name the acquirer and the shares, or neither")
        if (self.kind is Kind.STOCK_MERGER) != (self.into_symbol is not None):
            raise CorporateError(f"{self.symbol}: only a stock_merger pays in another name")
        if self.into_shares is not None and self.into_shares <= 0:
            raise CorporateError(f"{self.symbol}: a merger pays in a positive number of shares")
        if self.kind is Kind.CASH_ACQUISITION and self.price_usd is None:
            raise CorporateError(f"{self.symbol}: a cash_acquisition needs its deal price")
        if self.kind is Kind.HALT and self.price_usd is not None:
            raise CorporateError(f"{self.symbol}: a halt settles at no price")
        if self.price_usd is not None and self.price_usd <= 0:
            raise CorporateError(f"{self.symbol}: a price must be positive")

    @property
    def terminates(self) -> bool:
        """Whether the event ends the position itself, with no fill of ours behind it."""
        return self.kind in (Kind.CASH_ACQUISITION, Kind.STOCK_MERGER)

    @property
    def shares_after(self) -> Decimal:
        """The holding the event leaves. A delisting and a halt take nothing away: the exit,
        when it comes, is an ordinary sell."""
        return Decimal(0) if self.terminates else self.shares

    @property
    def ratio(self) -> Decimal:
        """The merger's exchange ratio: acquirer shares per share held."""
        if self.into_shares is None:
            raise CorporateError(f"{self.symbol}: a {self.kind} has no exchange ratio")
        return self.into_shares / self.shares


def in_period(events: Iterable[Event], period: str) -> tuple[Event, ...]:
    """The events filed against one rebalance, in the order they took effect."""
    return tuple(sorted((e for e in events if e.period == period), key=_filed))


def untradeable(events: Iterable[Event]) -> frozenset[str]:
    """Names no order may touch at this rebalance: a halted incumbent, which is carried, and
    a position the event already terminated, which must not be bought back."""
    return frozenset(e.symbol for e in events if e.kind is not Kind.DELISTING)


def divest(events: Iterable[Event]) -> frozenset[str]:
    """Names sold at this rebalance whatever their rank: a delisted stub, which exits at the
    last listed print, and the acquirer shares a merger paid in, which were never chosen."""
    names = {e.symbol for e in events if e.kind is Kind.DELISTING}
    return frozenset(names | {e.into_symbol for e in events if e.into_symbol is not None})


def explained(events: Iterable[Event]) -> frozenset[str]:
    """Every name a recorded event accounts for, subjects and acquirers alike. The gate reads
    this: a carried name in here is explained, and one that is not halts the rebalance."""
    events = tuple(events)
    return frozenset(e.symbol for e in events) | divest(events)


def exits(events: Iterable[Event]) -> dict[str, Decimal]:
    """The price each name the event terminated or delisted settled at, for a book that has
    no print of its own to mark the exit against."""
    return {e.symbol: e.price_usd for e in events if e.price_usd is not None}


def terminations(events: Iterable[Event]) -> dict[str, Event]:
    """The events that end a position by themselves, by symbol: what a simulation applies on
    the effective date instead of waiting for a rebalance."""
    return {e.symbol: e for e in events if e.terminates}


def applied(positions: Mapping[str, Decimal], events: Iterable[Event]) -> dict[str, Decimal]:
    """The book the rebalance starts from, with the recorded events folded in: a terminated
    position gone, and the acquirer shares a merger paid in standing where it stood. A
    delisting and a halt leave the holding alone, so they fold to nothing here and are dealt
    with by the selection. An event whose subject the book does not hold is ignored: it is
    somebody else's book, such as the shadow's."""
    book = dict(positions)
    for event in sorted(events, key=_filed):
        if event.symbol not in book:
            continue
        if event.terminates:
            del book[event.symbol]
        if event.into_symbol is not None and event.into_shares is not None:
            held = book.get(event.into_symbol, Decimal(0))
            book[event.into_symbol] = held + event.into_shares
    return book


def counts(events: Iterable[Event]) -> dict[str, int]:
    """How many of each kind, for a run's report. Every kind is counted, zero included, so
    the report reads the same whether a kind occurred or not."""
    events = tuple(events)
    return {f"{kind}_events": sum(1 for e in events if e.kind is kind) for kind in Kind}


_INSERT = """
    INSERT INTO agb.forced_event
        (period, symbol, kind, effective_date, shares_before, shares_after, price_usd,
         into_symbol, into_shares, detail, actor, surface, command_id)
    VALUES (%(period)s, %(symbol)s, %(kind)s, %(effective_date)s, %(shares_before)s,
            %(shares_after)s, %(price_usd)s, %(into_symbol)s, %(into_shares)s, %(detail)s,
            %(actor)s, %(surface)s, %(command_id)s)
    RETURNING id
"""
_FOR_PERIOD = """
    SELECT period, symbol, kind, effective_date, shares_before, detail, price_usd,
           into_symbol, into_shares
    FROM agb.forced_event
    WHERE period = %s
    ORDER BY effective_date, id
"""
_EXISTS = """
    SELECT actor, at FROM agb.forced_event
    WHERE period = %s AND symbol = %s AND kind = %s
"""


def record(conn: psycopg.Connection, event: Event, source: approval.Source) -> int:
    """Record a forced event, and return its id. The event and the command that carried it
    are written in one transaction, so the typed text is always one join from the row. A
    second event of the same kind for the same name and period is refused rather than
    doubling the share delta the snapshot check expects; the refusal is recorded too."""
    args: dict[str, Any] = {
        "period": event.period,
        "symbol": event.symbol,
        "kind": str(event.kind),
        "effective_date": event.effective_date.isoformat(),
        "shares": str(event.shares),
        "price_usd": None if event.price_usd is None else str(event.price_usd),
        "into_symbol": event.into_symbol,
        "into_shares": None if event.into_shares is None else str(event.into_shares),
        "detail": event.detail,
    }
    seen = conn.execute(_EXISTS, (event.period, event.symbol, str(event.kind))).fetchone()
    if seen is not None:
        why = (
            f"{event.symbol} already has a {event.kind} recorded for {event.period}, "
            f"by {seen[0]} on {seen[1]:%Y-%m-%d}"
        )
        approval.record(conn, source, VERB, args, why)
        raise CorporateError(why)
    with conn.transaction():
        command_id = approval.record(conn, source, VERB, args)
        row = conn.execute(
            _INSERT,
            {
                "period": event.period,
                "symbol": event.symbol,
                "kind": str(event.kind),
                "effective_date": event.effective_date,
                "shares_before": event.shares,
                "shares_after": event.shares_after,
                "price_usd": event.price_usd,
                "into_symbol": event.into_symbol,
                "into_shares": event.into_shares,
                "detail": event.detail,
                "actor": source.actor,
                "surface": source.surface,
                "command_id": command_id,
            },
        ).fetchone()
    assert row is not None, "the insert returns the id it wrote"
    return row[0]


def for_period(conn: psycopg.Connection, period: str) -> tuple[Event, ...]:
    """Every forced event filed against one rebalance period."""
    return tuple(
        Event(p, symbol, Kind(kind), effective, shares, detail, price, into, into_shares)
        for p, symbol, kind, effective, shares, detail, price, into, into_shares in conn.execute(
            _FOR_PERIOD, (period,)
        ).fetchall()
    )


def line(event: Event) -> str:
    """One recorded event in a line, for the terminal and the audit."""
    what = {
        Kind.CASH_ACQUISITION: lambda e: f"terminated at {e.price_usd} a share",
        Kind.STOCK_MERGER: lambda e: f"converted into {e.into_shares} {e.into_symbol}",
        Kind.DELISTING: lambda e: f"delisted, last listed print {e.price_usd}",
        Kind.HALT: lambda e: "halted across the rebalance, carried",
    }[event.kind](event)
    return (
        f"{event.symbol} {event.kind} {event.effective_date:%Y-%m-%d} "
        f"({event.shares} shares): {what}; {event.period} rebalance"
    )


def _filed(event: Event) -> tuple[date, str]:
    return event.effective_date, event.symbol
