"""Proposing a basket: the pipeline's monthly rebalance, from the scheduler's job to the first
`basket_event`.

Given a claimed `rebalance` job, `propose` builds the universe as of the signal session (the
last session before the job's trade session), asks the ranker for its stage-1 ranking, applies
agb.rebalance to the current book, prices the orders and freezes each one's modelled cost
under the current cost model, and writes the lot in one transaction: the basket with its
provenance (snapshot date, lake partition hash, universe size, ranker identity), every rank,
the twenty targets, the order plan and the basket's first event, `proposed`. The basket's
reference is the job's period, so October 2026's basket is `2026-10`. The proposal also
carries the universe and each name's mark, which the pre-trade gate (agb.gate) checks the
basket against before it can move on.

Proposing is safe to redo. A job whose lease expired mid-proposal is reclaimed and proposed
again; the second attempt finds the reference taken, writes nothing and completes the job with
the first basket. A lake that does not yet hold the signal session is not a failure: `NotReady`
tells the service to keep the job and try again later, as the data pull does.

A missed basket may be rerun at a later session's close when the operator asks (`agb rerun`,
agb.approval): `rerun` copies it, ranks, targets and orders as they were, under a new
reference such as `2026-10r1` whose `deviation_of` names the original, and proposes it for
the new session. The signal and the provenance are the original's, so the gate checks the
same universe and marks.

The book is what the last filled live basket holds. Until the execution ticket lands there is
no filled basket, so the book is empty and the first proposal buys twenty names. Execution
replaces `book` with the broker's positions; the rules that consume it do not change.
"""

import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import psycopg

from agb import cost, lake, nyse, rebalance, universe
from agb.jobs import Claim
from agb.ranker import Ranked, Ranker
from agb.roles import PIPELINE

CAPITAL_ENV = "AGB_CAPITAL_USD"
DEFAULT_CAPITAL = Decimal("100000")
SURFACE = "pipeline"

_PERIOD = re.compile(r"^\d{4}-\d{2}$")
# The daily bars the cost model reads, plus the one day the two-day spread needs.
_BARS = cost.CURRENT.window + 1

# The as-traded close of every name that printed on the signal session.
_PRINTS = """
    SELECT ticker, closeunadj FROM sep
    WHERE date = ? AND volume > 0 AND closeunadj IS NOT NULL AND closeunadj > 0
"""
# The last as-traded close of each held name that did not print on the signal session, within
# the quarter before it, so the gate can value the whole book.
_LAST_CLOSE = """
    SELECT ticker, arg_max(closeunadj, date) FROM sep
    WHERE list_contains(?, ticker) AND date <= ? AND date > ?
      AND volume > 0 AND closeunadj IS NOT NULL AND closeunadj > 0
    GROUP BY ticker
"""
_MARK_WINDOW = timedelta(days=92)
# How many of the cost model's trailing sessions each name has a bar for.
_BAR_COUNTS = """
    WITH recent AS (SELECT DISTINCT date FROM sep WHERE date <= ? ORDER BY date DESC LIMIT ?)
    SELECT ticker, count(*) FROM sep
    WHERE date IN (SELECT date FROM recent)
      AND high > 0 AND low > 0 AND close > 0 AND volume IS NOT NULL
    GROUP BY ticker
"""
# One name's trailing bars, oldest first, for the cost model.
_BARS_FOR = """
    SELECT high, low, close, volume FROM sep
    WHERE ticker = ? AND date <= ? AND high > 0 AND low > 0 AND close > 0 AND volume IS NOT NULL
    ORDER BY date DESC LIMIT ?
"""

_INSERT_BASKET = """
    INSERT INTO agb.basket
        (reference, period, job_id, session_date, signal_date, snapshot_date, partition_hash,
         universe_size, ranker, cost_model_version, capital_usd, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (reference) DO NOTHING
    RETURNING id
"""
_INSERT_RANK = (
    "INSERT INTO agb.basket_rank (basket_id, symbol, rank, score) VALUES (%s, %s, %s, %s)"
)
_INSERT_TARGET = """
    INSERT INTO agb.basket_target (basket_id, symbol, weight, shares, held, carried, rank)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
_INSERT_ORDER = """
    INSERT INTO agb.basket_order
        (basket_id, client_order_id, symbol, side, qty, reference_price, notional_usd,
         cost_model_version, floor_bps, impact_bps, fees_usd, modelled_cost_bps)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
_INSERT_EVENT = """
    INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface)
    VALUES (%s, 1, NULL, 'proposed', %s, %s)
"""
_BOOK = """
    SELECT t.symbol, t.shares
    FROM agb.basket_target t
    WHERE t.basket_id = (
        SELECT id FROM agb.basket_state
        WHERE kind = 'live' AND state = 'filled'
        ORDER BY session_date DESC, id DESC
        LIMIT 1
    )
"""


# A rerun: the missed basket's provenance under a new reference and session.
_INSERT_RERUN = """
    INSERT INTO agb.basket
        (reference, period, deviation_of, job_id, session_date, signal_date, snapshot_date,
         partition_hash, universe_size, ranker, cost_model_version, capital_usd, actor)
    SELECT %(reference)s, period, id, NULL, %(session)s, signal_date, snapshot_date,
           partition_hash, universe_size, ranker, cost_model_version, capital_usd, %(actor)s
    FROM agb.basket WHERE id = %(original)s
    ON CONFLICT (reference) DO NOTHING
    RETURNING id
"""
_COPY_RANKS = """
    INSERT INTO agb.basket_rank (basket_id, symbol, rank, score)
    SELECT %(basket)s, symbol, rank, score FROM agb.basket_rank
    WHERE basket_id = %(original)s ORDER BY id
"""
_COPY_TARGETS = """
    INSERT INTO agb.basket_target (basket_id, symbol, weight, shares, held, carried, rank)
    SELECT %(basket)s, symbol, weight, shares, held, carried, rank FROM agb.basket_target
    WHERE basket_id = %(original)s ORDER BY id
"""
_COPY_ORDERS = """
    INSERT INTO agb.basket_order
        (basket_id, client_order_id, symbol, side, qty, tif, reference_price, notional_usd,
         cost_model_version, floor_bps, impact_bps, fees_usd, modelled_cost_bps)
    SELECT %(basket)s, %(reference)s || '-' || symbol, symbol, side, qty, tif, reference_price,
           notional_usd, cost_model_version, floor_bps, impact_bps, fees_usd, modelled_cost_bps
    FROM agb.basket_order
    WHERE basket_id = %(original)s ORDER BY id
"""
_COUNTS = """
    SELECT (SELECT count(*) FROM agb.basket_target WHERE basket_id = %(basket)s),
           (SELECT count(*) FROM agb.basket_order WHERE basket_id = %(basket)s)
"""
_PROPOSE_RERUN = """
    INSERT INTO agb.basket_event
        (basket_id, seq, from_state, to_state, actor, surface, reason, command_id)
    VALUES (%s, 1, NULL, 'proposed', %s, %s, %s, %s)
"""


class BasketError(Exception):
    """A basket that cannot be proposed from what the lake holds."""


class NotReady(BasketError):
    """The lake does not yet hold the signal session; try again later."""


@dataclass(frozen=True)
class Proposal:
    """What `propose` wrote, for the log, and what the gate reads beside the basket: the
    universe it was ranked from and each name's mark, its last as-traded close on or before
    the signal session."""

    basket_id: int
    reference: str
    existing: bool  # an earlier attempt had already proposed this reference
    names: int
    orders: int
    members: frozenset[str] = frozenset()
    marks: dict[str, Decimal] = field(default_factory=dict)


def book(conn: psycopg.Connection) -> dict[str, Decimal]:
    """The positions the rebalance starts from: the targets of the last filled live basket,
    in shares. Empty until a basket has filled."""
    return {symbol: Decimal(shares) for symbol, shares in conn.execute(_BOOK).fetchall()}


def signal_session(session_date: date, calendar: nyse.NyseCalendar | None = None) -> date:
    """The signal session L for a trade session R: the last session before it."""
    start = session_date - timedelta(days=40)
    calendar = calendar or nyse.NyseCalendar(start, session_date + timedelta(days=5))
    sessions = calendar.sessions(start, session_date - timedelta(days=1))
    if not sessions:
        raise BasketError(f"no NYSE session in the 40 days before {session_date}")
    return sessions[-1]


def propose(
    conn: psycopg.Connection,
    claim: Claim,
    lake_dir: Path,
    ranker: Ranker,
    *,
    capital: Decimal = DEFAULT_CAPITAL,
    calendar: nyse.NyseCalendar | None = None,
) -> Proposal:
    """Propose the basket for a claimed rebalance job, and return what was written."""
    if not _PERIOD.match(claim.period):
        raise BasketError(f"rebalance period {claim.period!r} is not a month")
    if capital <= 0:
        raise BasketError(f"{CAPITAL_ENV} must be positive, not {capital}")
    row = conn.execute("SELECT session_date FROM agb.job WHERE id = %s", (claim.job_id,)).fetchone()
    if row is None:
        raise BasketError(f"job {claim.job_id} does not exist")
    session_date = row[0]
    signal = signal_session(session_date, calendar)

    held = book(conn)
    lake_db = lake.connect(lake_dir)
    try:
        _require_bars(lake_db)
        snapshot = universe.snapshot(signal, lake_dir, db=lake_db)
        if snapshot.snapshot_date != signal:
            raise NotReady(
                f"the lake's last session is {snapshot.snapshot_date}, before the signal"
                f" session {signal}"
            )
        ranking = ranker.rank(snapshot, lake_db)
        prints = {str(t): Decimal(str(p)) for t, p in lake_db.execute(_PRINTS, [signal]).fetchall()}
        counted = {
            str(t): int(n)
            for t, n in lake_db.execute(_BAR_COUNTS, [signal, _BARS]).fetchall()
            if n >= _BARS
        }
        marks = prints | _last_closes(lake_db, [s for s in held if s not in prints], signal)
        untradeable = {s for s in (*held, *snapshot.tickers) if s not in prints or s not in counted}
        selection = rebalance.select([r.symbol for r in ranking], held, untradeable=untradeable)
        weight = rebalance.weights(selection.names)
        plan = rebalance.orders(selection, weight, prints, held, capital)
        costs = [_cost(lake_db, order, signal) for order in plan]
    finally:
        lake_db.close()

    rank_of = {r.symbol: r for r in ranking}
    members = frozenset(snapshot.tickers)
    with conn.transaction():
        row = conn.execute(
            _INSERT_BASKET,
            (
                claim.period,
                claim.period,
                claim.job_id,
                session_date,
                signal,
                snapshot.snapshot_date,
                snapshot.partition_hash,
                snapshot.size,
                ranker.name,
                cost.CURRENT.version,
                capital,
                PIPELINE,
            ),
        ).fetchone()
        if row is None:
            existing = conn.execute(
                "SELECT id FROM agb.basket WHERE reference = %s", (claim.period,)
            ).fetchone()[0]
            return Proposal(existing, claim.period, True, 0, 0, members, marks)
        basket_id = row[0]
        with conn.cursor() as cur:
            cur.executemany(_INSERT_RANK, [(basket_id, r.symbol, r.rank, r.score) for r in ranking])
            cur.executemany(
                _INSERT_TARGET,
                [
                    (basket_id, *_target(symbol, selection, weight, held, plan, rank_of))
                    for symbol in selection.names
                ],
            )
            cur.executemany(
                _INSERT_ORDER,
                [
                    (
                        basket_id,
                        f"{claim.period}-{order.symbol}",
                        order.symbol,
                        str(order.side),
                        order.qty,
                        order.price,
                        order.notional,
                        modelled.model_version,
                        _bps(modelled.floor_bps),
                        _bps(modelled.impact_bps),
                        modelled.fees.total,
                        _bps(modelled.floor_bps)
                        + _bps(modelled.impact_bps)
                        + _bps(modelled.fees_bps),
                    )
                    for order, modelled in zip(plan, costs, strict=True)
                ],
            )
        conn.execute(_INSERT_EVENT, (basket_id, PIPELINE, SURFACE))
    return Proposal(basket_id, claim.period, False, len(selection.names), len(plan), members, marks)


def _bps(value: float) -> Decimal:
    """A basis-point figure frozen to a ten-thousandth of a point, so the recorded parts add up
    to the recorded total exactly."""
    return Decimal(repr(value)).quantize(Decimal("0.0001"))


def _target(
    symbol: str,
    selection: rebalance.Selection,
    weight: dict[str, Decimal],
    held: dict[str, Decimal],
    plan: list[rebalance.Order],
    rank_of: dict[str, Ranked],
) -> tuple:
    if symbol in selection.buy:
        shares = next(o.qty for o in plan if o.symbol == symbol)
    else:
        shares = held[symbol]
    ranked = rank_of.get(symbol)
    return (
        symbol,
        weight[symbol],
        shares,
        symbol in selection.held,
        symbol in selection.carry,
        ranked.rank if ranked else None,
    )


def _last_closes(
    db: duckdb.DuckDBPyConnection, symbols: list[str], signal: date
) -> dict[str, Decimal]:
    if not symbols:
        return {}
    rows = db.execute(_LAST_CLOSE, [symbols, signal, signal - _MARK_WINDOW]).fetchall()
    return {str(t): Decimal(str(p)) for t, p in rows}


def _cost(db: duckdb.DuckDBPyConnection, order: rebalance.Order, signal: date) -> cost.TradeCost:
    bars = db.execute(_BARS_FOR, [order.symbol, signal, _BARS]).fetchall()[::-1]
    high, low, close, volume = ([float(bar[i]) for bar in bars] for i in range(4))
    liq = cost.liquidity(high, low, close, volume)
    return cost.trade_cost(cost.Side(str(order.side)), order.notional, order.qty, liq)


def _require_bars(db: duckdb.DuckDBPyConnection) -> None:
    """The cost model needs the high and low, which the universe does not."""
    try:
        present = {row[0] for row in db.execute("DESCRIBE sep").fetchall()}
    except duckdb.Error:
        raise BasketError("the lake has no sep partitions") from None
    missing = sorted({"high", "low", "close", "volume", "closeunadj"} - present)
    if missing:
        raise BasketError(f"the lake's sep lacks {missing}")


def rerun(
    conn: psycopg.Connection,
    original: int,
    reference: str,
    session_date: date,
    command_id: int,
    lake_dir: Path,
) -> Proposal:
    """Propose a copy of a missed basket for a later session, under the rerun's reference,
    as the operator's `rerun` command asked. Safe to redo: a reference already taken writes
    nothing and returns the basket that holds it."""
    row = conn.execute("SELECT signal_date FROM agb.basket WHERE id = %s", (original,)).fetchone()
    if row is None:
        raise BasketError(f"basket {original} does not exist")
    signal = row[0]
    held = book(conn)
    lake_db = lake.connect(lake_dir)
    try:
        members = frozenset(universe.snapshot(signal, lake_dir, db=lake_db).tickers)
        prints = {str(t): Decimal(str(p)) for t, p in lake_db.execute(_PRINTS, [signal]).fetchall()}
        marks = prints | _last_closes(lake_db, [s for s in held if s not in prints], signal)
    finally:
        lake_db.close()

    with conn.transaction():
        row = conn.execute(
            _INSERT_RERUN,
            {
                "reference": reference,
                "session": session_date,
                "original": original,
                "actor": PIPELINE,
            },
        ).fetchone()
        if row is None:
            basket_id = conn.execute(
                "SELECT id FROM agb.basket WHERE reference = %s", (reference,)
            ).fetchone()[0]
            return Proposal(basket_id, reference, True, 0, 0, members, marks)
        basket_id = row[0]
        copy = {"basket": basket_id, "original": original, "reference": reference}
        conn.execute(_COPY_RANKS, copy)
        conn.execute(_COPY_TARGETS, copy)
        conn.execute(_COPY_ORDERS, copy)
        reason = f"rerun of basket {original}, as asked by command {command_id}"
        conn.execute(_PROPOSE_RERUN, (basket_id, PIPELINE, SURFACE, reason, command_id))
        names, orders = conn.execute(_COUNTS, {"basket": basket_id}).fetchone()
    return Proposal(basket_id, reference, False, names, orders, members, marks)
