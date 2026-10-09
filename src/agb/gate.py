"""The pre-trade gate: the last step of the rebalance, and the only way a basket moves on.

The limits are malfunction detectors, not risk management. They catch a broken pipeline, not
a bad month, so the thresholds are deliberately wide (docs/architecture/04-risk-and-approval.md).
The gate runs every check that does not need the broker, in three groups:

  * **Provenance.** The universe's snapshot is the month-end before the basket's period, the
    universe's size is inside the band agb.universe expects, and the ranks came from the
    promoted model: the basket's artifact digest is the live certification's, and that
    certification has not been voided (agb.registry). On a basket the panel blended
    (agb.blend), every shortlisted name was scored, and the scores are spread: a panel that
    scores every name alike is useless whatever each score is, because stage 2 ranks its
    output into percentiles and a flat distribution ranks to noise.
  * **The basket.** Twenty positions whose weights sum to one, no symbol twice, cash left
    over after the targets are bought, and turnover below 60% of the book.
  * **Each order.** Its symbol is in the universe, its side agrees with the book (a buy opens
    a position, a sell closes one it holds), its notional is at most 8% of equity, it is
    market-on-close, and the clock is before the session's MOC cutoff (15:45 ET on a full
    day).

Every check is recorded with what it measured and the threshold it had to meet, whether it
passed or not, so the result reads check by check. A single failure halts the whole
rebalance: the basket moves to halted and the halt latch's `gate_breach` cause opens, so
nothing trades and the book stays as last month left it until a human clears it. A basket
that passes waits for the human in live mode; in paper mode the system approves it, unless
the operator has set paper mode to ask the human too (`agb.paper_approval`), when it waits
as in live mode.

Equity is the capital the basket was sized against until execution reads the broker's
account, and the book is `basket.book`. Both change source later; the checks do not.

The gate is safe to redo. It runs once per basket, in one transaction with its verdict, and
a rebalance job reclaimed after the gate ran reads the recorded verdict back.
"""

from collections import Counter
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg

from agb import basket, blend, jobs, nyse, rebalance, schedule, universe
from agb.roles import PIPELINE

SYSTEM = "system"
PAPER = "paper"

PROVENANCE, BASKET, ORDER = "provenance", "basket", "order"


class GateError(Exception):
    """A basket the gate cannot run on."""


@dataclass(frozen=True)
class Limits:
    """The gate's thresholds."""

    universe_min: int = universe.MIN_SIZE
    universe_max: int = universe.MAX_SIZE
    positions: int = rebalance.SIZE
    weight_tolerance: Decimal = Decimal("0.0001")
    turnover: Decimal = Decimal("0.60")
    notional: Decimal = Decimal("0.08")
    dispersion: float = blend.DISPERSION_MIN


LIMITS = Limits()


@dataclass(frozen=True)
class Target:
    symbol: str
    weight: Decimal
    shares: Decimal


@dataclass(frozen=True)
class Order:
    symbol: str
    side: str
    qty: Decimal
    tif: str
    notional: Decimal


@dataclass(frozen=True)
class Panel:
    """The blend behind a basket, as `agb.basket_blend` records it: how many of the shortlist
    the panel scored, and how far apart those scores are."""

    run_id: int
    w: Decimal
    scored: int
    shortlisted: int
    spread: float | None  # the population standard deviation of the scores, None under two


@dataclass(frozen=True)
class Basket:
    """What the gate reads of a proposed basket."""

    id: int
    period: str
    session_date: date
    snapshot_date: date
    universe_size: int
    ranker: str
    model_sha256: str | None  # the artifact the ranker was loaded from; None before the registry
    capital: Decimal
    targets: tuple[Target, ...]
    orders: tuple[Order, ...]
    panel: Panel | None = None  # None on a basket proposed with no panel at all


@dataclass(frozen=True)
class Promoted:
    """The live model, as `agb.promoted_model` has it."""

    certification_id: int
    model_id: str
    artifact_sha256: str
    voided: bool


@dataclass(frozen=True)
class Check:
    level: str
    name: str
    symbol: str | None
    measured: str
    threshold: str
    passed: bool


@dataclass(frozen=True)
class Verdict:
    """The gate's result for one basket, and the state it left the basket in."""

    basket_id: int
    mode: str
    state: str
    checks: tuple[Check, ...]
    recorded: bool = False  # an earlier attempt had already run the gate

    @property
    def failed(self) -> tuple[Check, ...]:
        return tuple(c for c in self.checks if not c.passed)

    @property
    def passed(self) -> bool:
        return not self.failed


def evaluate(
    target: Basket,
    *,
    members: Collection[str],
    marks: Mapping[str, Decimal],
    book: Mapping[str, Decimal],
    now: datetime,
    promoted: Promoted | None,
    limits: Limits = LIMITS,
    calendar: nyse.NyseCalendar | None = None,
) -> list[Check]:
    """Every check on `target`, in order: provenance, the basket, then each order.

    `members` is the universe the basket was ranked from, `marks` each name's last as-traded
    close on or before the signal session, `book` the positions before the rebalance, `now`
    the moment the gate runs and `promoted` the live model, None if nothing is promoted."""
    year, month = (int(part) for part in target.period.split("-"))
    first = date(year, month, 1)
    calendar = calendar or nyse.NyseCalendar(first - timedelta(days=40), target.session_date)
    return [
        *_provenance(target, promoted, limits, calendar, first),
        *_basket(target, marks, book, limits),
        *(
            check
            for order in target.orders
            for check in _order(target, order, members, book, now, limits, calendar)
        ),
    ]


def _provenance(
    target: Basket,
    promoted: Promoted | None,
    limits: Limits,
    calendar: nyse.NyseCalendar,
    first: date,
) -> list[Check]:
    before = first - timedelta(days=1)
    month_end = calendar.last_session_of_month(before.year, before.month)
    size = target.universe_size
    return [
        Check(
            PROVENANCE,
            "snapshot_date",
            None,
            target.snapshot_date.isoformat(),
            month_end.isoformat(),
            target.snapshot_date == month_end,
        ),
        Check(
            PROVENANCE,
            "universe_size",
            None,
            str(size),
            f"{limits.universe_min} to {limits.universe_max}",
            limits.universe_min <= size <= limits.universe_max,
        ),
        _model(target, promoted),
        *_panel(target.panel, limits),
    ]


def _panel(panel: Panel | None, limits: Limits) -> list[Check]:
    """The panel's coverage and spread. A basket no panel scored has neither check: there is
    nothing to measure, and the stage-1 ranking is what it traded."""
    if panel is None:
        return []
    return [
        Check(
            PROVENANCE,
            "panel_coverage",
            None,
            f"{panel.scored} of {panel.shortlisted} scored",
            f"all {panel.shortlisted}",
            panel.scored == panel.shortlisted,
        ),
        Check(
            PROVENANCE,
            "panel_dispersion",
            None,
            "fewer than two names scored" if panel.spread is None else f"{panel.spread:.4f}",
            f">= {limits.dispersion}",
            panel.spread is not None and panel.spread >= limits.dispersion,
        ),
    ]


def _model(target: Basket, promoted: Promoted | None) -> Check:
    """The basket was ranked by the live model: its artifact digest is the promoted
    certification's, and that certification has not been voided since."""
    digest = target.model_sha256
    measured = f"{target.ranker} {digest[:12]}" if digest else f"{target.ranker}, uncertified"
    if promoted is None:
        return Check(PROVENANCE, "model", None, measured, "a promoted model", False)
    threshold = f"{promoted.model_id} {promoted.artifact_sha256[:12]}"
    if promoted.voided:
        threshold += ", which has been voided"
    passed = not promoted.voided and digest == promoted.artifact_sha256
    return Check(PROVENANCE, "model", None, measured, threshold, passed)


def _basket(
    target: Basket, marks: Mapping[str, Decimal], book: Mapping[str, Decimal], limits: Limits
) -> list[Check]:
    positions = len(target.targets)
    total = sum((t.weight for t in target.targets), Decimal(0))
    return [
        Check(
            BASKET,
            "positions",
            None,
            str(positions),
            str(limits.positions),
            positions == limits.positions,
        ),
        Check(
            BASKET,
            "weight_sum",
            None,
            str(total),
            f"1 within {limits.weight_tolerance}",
            abs(total - 1) <= limits.weight_tolerance,
        ),
        _duplicates(target),
        _cash(target, marks),
        _turnover(target, marks, book, limits),
    ]


def _duplicates(target: Basket) -> Check:
    found = []
    for what, symbols in (
        ("targets", [t.symbol for t in target.targets]),
        ("orders", [o.symbol for o in target.orders]),
    ):
        twice = sorted(s for s, n in Counter(symbols).items() if n > 1)
        if twice:
            found.append(f"{what} {', '.join(twice)}")
    return Check(BASKET, "duplicates", None, "; ".join(found) or "none", "none", not found)


def _cash(target: Basket, marks: Mapping[str, Decimal]) -> Check:
    """The cash left once the basket holds its targets: equity less the targets' value at
    their marks."""
    unpriced = sorted(t.symbol for t in target.targets if t.symbol not in marks)
    if unpriced:
        return Check(BASKET, "cash", None, f"no mark for {', '.join(unpriced)}", ">= 0", False)
    cash = target.capital - sum((t.shares * marks[t.symbol] for t in target.targets), Decimal(0))
    return Check(BASKET, "cash", None, _dollars(cash), ">= 0", cash >= 0)


def _turnover(
    target: Basket, marks: Mapping[str, Decimal], book: Mapping[str, Decimal], limits: Limits
) -> Check:
    """What the plan sells, as a share of the book's value at its marks. An empty book has
    nothing to turn over."""
    threshold = f"< {limits.turnover:.0%}"
    if not book:
        return Check(BASKET, "turnover", None, "no book", threshold, True)
    unpriced = sorted(s for s in book if s not in marks)
    if unpriced:
        measured = f"no mark for {', '.join(unpriced)}"
        return Check(BASKET, "turnover", None, measured, threshold, False)
    value = sum((shares * marks[s] for s, shares in book.items()), Decimal(0))
    sold = sum((o.notional for o in target.orders if o.side == "sell"), Decimal(0))
    if value <= 0:
        return Check(BASKET, "turnover", None, f"book worth {_dollars(value)}", threshold, False)
    ratio = sold / value
    return Check(BASKET, "turnover", None, f"{ratio:.2%}", threshold, ratio < limits.turnover)


def _order(
    target: Basket,
    order: Order,
    members: Collection[str],
    book: Mapping[str, Decimal],
    now: datetime,
    limits: Limits,
    calendar: nyse.NyseCalendar,
) -> list[Check]:
    symbol = order.symbol
    held = book.get(symbol, Decimal(0))
    # A name leaves the book by being sold, including one that has left the universe.
    in_universe = symbol in members
    if in_universe:
        membership = "in the universe"
    elif order.side == "sell" and held > 0:
        membership = "held, not in the universe"
    else:
        membership = "not in the universe"
    if order.side == "buy":
        side_ok = held == 0
    else:
        side_ok = order.side == "sell" and 0 < order.qty <= held
    share = order.notional / target.capital
    cutoff = calendar.moc_cutoff(target.session_date)
    return [
        Check(
            ORDER,
            "universe",
            symbol,
            membership,
            "in the universe, or held for a sell",
            in_universe or (order.side == "sell" and held > 0),
        ),
        Check(
            ORDER,
            "side",
            symbol,
            f"{order.side} {order.qty}, holding {held}",
            "a buy opens a position; a sell closes at most what is held",
            side_ok,
        ),
        Check(
            ORDER,
            "notional",
            symbol,
            f"{share:.2%} of {_dollars(target.capital)}",
            f"<= {limits.notional:.0%}",
            share <= limits.notional,
        ),
        Check(ORDER, "tif", symbol, order.tif, "cls", order.tif == "cls"),
        Check(ORDER, "clock", symbol, _eastern(now), f"before {_eastern(cutoff)}", now < cutoff),
    ]


def _dollars(amount: Decimal) -> str:
    return f"{amount:.2f}"


def _eastern(at: datetime) -> str:
    return at.astimezone(nyse.NEW_YORK).strftime("%Y-%m-%d %H:%M ET")


_BASKET = """
    SELECT id, period, session_date, snapshot_date, universe_size, ranker, model_sha256,
           capital_usd
    FROM agb.basket WHERE id = %s
"""
# The blend behind the basket, if a panel scored it. stddev_pop is NULL for no scored rows
# and 0 for one, so the count decides whether there is a spread to read at all.
_PANEL = """
    SELECT b.panel_run_id, b.panel_w,
           count(*) FILTER (WHERE NOT l.neutral), count(*),
           stddev_pop(l.llm_score) FILTER (WHERE NOT l.neutral)
    FROM agb.basket b
    JOIN agb.basket_blend l ON l.basket_id = b.id
    WHERE b.id = %s AND b.panel_run_id IS NOT NULL
    GROUP BY b.panel_run_id, b.panel_w
"""
_PROMOTED = "SELECT certification_id, model_id, artifact_sha256, voided FROM agb.promoted_model"
_TARGETS = "SELECT symbol, weight, shares FROM agb.basket_target WHERE basket_id = %s ORDER BY id"
_ORDERS = """
    SELECT symbol, side, qty, tif, notional_usd FROM agb.basket_order
    WHERE basket_id = %s ORDER BY id
"""
_RUN = "SELECT id, mode FROM agb.gate_run WHERE basket_id = %s"
_INSERT_RUN = """
    INSERT INTO agb.gate_run (basket_id, checks, mode, actor) VALUES (%s, %s, %s, %s)
    RETURNING id
"""
_CHECKS = """
    SELECT level, name, symbol, measured, threshold, passed FROM agb.gate_check
    WHERE run_id = %s ORDER BY id
"""
_INSERT_CHECK = """
    INSERT INTO agb.gate_check (run_id, level, name, symbol, measured, threshold, passed)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
_STATE = "SELECT state, seq FROM agb.basket_state WHERE id = %s"
_MOVE = """
    INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface, reason)
    VALUES (%s, %s, 'proposed', %s, %s, %s, %s)
    RETURNING id
"""
_BREACH = """
    INSERT INTO agb.halt_event (cause, action, basket_id, actor, reason)
    VALUES ('gate_breach', 'set', %s, %s, %s)
"""


def load(conn: psycopg.Connection, basket_id: int) -> Basket:
    """A basket as the gate reads it."""
    row = conn.execute(_BASKET, (basket_id,)).fetchone()
    if row is None:
        raise GateError(f"basket {basket_id} does not exist")
    targets = tuple(Target(*t) for t in conn.execute(_TARGETS, (basket_id,)).fetchall())
    orders = tuple(Order(*o) for o in conn.execute(_ORDERS, (basket_id,)).fetchall())
    blended = conn.execute(_PANEL, (basket_id,)).fetchone()
    panel = None
    if blended is not None:
        run_id, w, scored, shortlisted, spread = blended
        panel = Panel(run_id, w, scored, shortlisted, None if scored < 2 else float(spread))
    return Basket(*row, targets, orders, panel)


def run(
    conn: psycopg.Connection,
    basket_id: int,
    *,
    members: Collection[str],
    marks: Mapping[str, Decimal],
    now: datetime,
    limits: Limits = LIMITS,
    calendar: nyse.NyseCalendar | None = None,
) -> Verdict:
    """Gate a proposed basket, record every check, and move it on: to halted with a
    `gate_breach` on any failure, to approved by the system on a pass in paper mode, and
    nowhere on a pass in live mode or in a paper mode that asks the human. A basket already
    gated returns its recorded verdict."""
    with conn.transaction():
        recorded = conn.execute(_RUN, (basket_id,)).fetchone()
        current = conn.execute(_STATE, (basket_id,)).fetchone()
        if recorded is not None:
            run_id, mode = recorded
            checks = tuple(Check(*c) for c in conn.execute(_CHECKS, (run_id,)).fetchall())
            return Verdict(basket_id, mode, current[0], checks, recorded=True)
        if current is None or current[0] != "proposed":
            state = current[0] if current else "unproposed"
            raise GateError(f"basket {basket_id} is {state}, not proposed")
        seq = current[1] + 1

        target = load(conn, basket_id)
        live = conn.execute(_PROMOTED).fetchone()
        checks = evaluate(
            target,
            members=members,
            marks=marks,
            book=basket.book(conn),
            now=now,
            promoted=Promoted(*live) if live is not None else None,
            limits=limits,
            calendar=calendar,
        )
        (mode,) = conn.execute("SELECT mode FROM agb.current_mode").fetchone()
        (asks,) = conn.execute("SELECT required FROM agb.paper_approval").fetchone()
        (run_id,) = conn.execute(_INSERT_RUN, (basket_id, len(checks), mode, PIPELINE)).fetchone()
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_CHECK,
                [
                    (run_id, c.level, c.name, c.symbol, c.measured, c.threshold, c.passed)
                    for c in checks
                ],
            )
        failed = [c for c in checks if not c.passed]
        if failed:
            reason = "pre-trade gate failed: " + "; ".join(_describe(c) for c in failed)
            conn.execute(_MOVE, (basket_id, seq, "halted", PIPELINE, basket.SURFACE, reason))
            conn.execute(_BREACH, (basket_id, PIPELINE, reason))
            state = "halted"
        elif mode == PAPER and not asks:
            reason = "passed the pre-trade gate in paper mode"
            (event_id,) = conn.execute(
                _MOVE, (basket_id, seq, "approved", SYSTEM, basket.SURFACE, reason)
            ).fetchone()
            # Nobody is asked, so the approval the month expects is this one.
            jobs.fulfil(
                conn, schedule.APPROVAL_REQUEST, target.period, "basket_event", event_id, PIPELINE
            )
            state = "approved"
        else:
            state = "proposed"
    return Verdict(basket_id, mode, state, tuple(checks))


def _describe(check: Check) -> str:
    name = f"{check.name} {check.symbol}" if check.symbol else check.name
    return f"{name} measured {check.measured}, needs {check.threshold}"
