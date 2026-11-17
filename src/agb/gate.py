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
  * **The basket.** Twenty positions, no symbol twice, and turnover below 60% of the book, or
    a full rotation where the model behind the book is not the model that ranked the basket.
    Weights are measured on value, each target's shares at its mark against equity, not read
    off the stored weight: each name sits within one percentage point (`rebalance.TOP_UP`) or
    one share under its twentieth, a name the basket buys sits no more than that over it, the
    weights sum to the basket's share of the twenty less that slack and to no more than one,
    and the cash left once the targets are held and every order's modelled cost is paid is
    not negative. Fewer than twenty passes only where a recorded forced corporate event
    (agb.corporate) explains it, and a name the rebalance carried with no event on the
    record halts the month: a name that has quietly stopped printing looks exactly like a
    halted one, and the difference is a human's judgement. A basket short a slot keeps the
    same weight per name and leaves the missing slot's weight in cash.
  * **Each order.** Its symbol is in the universe, its side agrees with the book (a buy opens
    a position or tops a kept one up to its target, a sell closes one it holds), a buy's
    notional is at most 8% of equity (a sell is exempt, because it reduces exposure), it is
    market-on-close, and the clock is before the session's MOC cutoff (15:45 ET on a full
    day).

Every check is recorded with what it measured and the threshold it had to meet, whether it
passed or not, so the result reads check by check. A single failure halts the whole
rebalance: the basket moves to halted and the halt latch's `gate_breach` cause opens, so
nothing trades and the book stays as last month left it until a human clears it. A basket
that passes waits for the human in live mode; in paper mode the system approves it, unless
the operator has set paper mode to ask the human too (`agb.paper_approval`), when it waits
as in live mode. At L2 of the autonomy ladder the system approves a live basket too, and the
approval message becomes the notification `agb.approval.notify` raises.

The gate itself is not part of the ladder: it runs, and halts what fails, at every level.

Equity is the capital the basket was sized against until execution reads the broker's
account; that source changes later and the checks do not. The book is `basket.book`: the
positions the broker reported at its last read, not what any basket was planned to hold, and
a read older than the basket's signal session is `basket.NotReady` rather than a book.

The gate is safe to redo. It runs once per basket, in one transaction with its verdict, and
a rebalance job reclaimed after the gate ran reads the recorded verdict back.
"""

from collections import Counter
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

import psycopg

from agb import basket, blend, corporate, jobs, nyse, rebalance, schedule, universe
from agb.roles import PIPELINE
from agb.rows import one

SYSTEM = "system"
PAPER = "paper"
LIVE = "live"
# The level at which a rebalance proceeds unasked, told to the human afterwards.
UNASKED = "L2"

PROVENANCE, BASKET, ORDER = "provenance", "basket", "order"


class GateError(Exception):
    """A basket the gate cannot run on."""


@dataclass(frozen=True)
class Limits:
    """The gate's thresholds."""

    universe_min: int = universe.MIN_SIZE
    universe_max: int = universe.MAX_SIZE
    positions: int = rebalance.SIZE
    # How far a name's value may sit from its twentieth, as a share of equity.
    weight_tolerance: Decimal = rebalance.TOP_UP
    turnover: Decimal = Decimal("0.60")
    notional: Decimal = Decimal("0.08")
    dispersion: float = blend.DISPERSION_MIN


LIMITS = Limits()


@dataclass(frozen=True)
class Target:
    symbol: str
    weight: Decimal
    shares: Decimal
    # An incumbent the rebalance could not trade, kept in place with no order of its own.
    carried: bool = False


@dataclass(frozen=True)
class Order:
    symbol: str
    side: str
    qty: Decimal
    tif: str
    notional: Decimal
    # The modelled cost of the order in dollars, fees included, as the basket recorded it.
    cost: Decimal = Decimal(0)


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
    # The forced corporate events the operator recorded against the period (agb.corporate):
    # what the positions check reads a short basket against.
    forced: tuple[corporate.Event, ...] = ()


@dataclass(frozen=True)
class Promoted:
    """The live model, as `agb.promoted_model` has it."""

    certification_id: int
    model_id: str
    artifact_sha256: str
    voided: bool


@dataclass(frozen=True)
class Held:
    """The model behind the book: the ranker and artifact digest of the last live basket whose
    orders actually filled. None when nothing has ever filled, so the book, if there is one,
    has no model on the record."""

    ranker: str
    model_sha256: str | None


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
    held: Held | None = None,
    limits: Limits = LIMITS,
    calendar: nyse.NyseCalendar | None = None,
) -> list[Check]:
    """Every check on `target`, in order: provenance, the basket, then each order.

    `members` is the universe the basket was ranked from, `marks` each name's last as-traded
    close on or before the signal session, `book` the positions before the rebalance, `now`
    the moment the gate runs and `promoted` the live model, None if nothing is promoted.
    `held` is the model the book was bought under, which the turnover check reads; None, the
    default, measures turnover against the flat limit and exempts nothing.

    The book is read through the forced events on the record (agb.corporate), because that is
    the book the broker actually has: a position an acquisition terminated is gone, and the
    shares a merger paid in stand where it stood. The price an event settled at marks a name
    the lake has no print for, such as a delisted stub the plan has to sell."""
    book = corporate.applied(book, target.forced)
    marks = {**corporate.exits(target.forced), **marks}
    year, month = (int(part) for part in target.period.split("-"))
    first = date(year, month, 1)
    calendar = calendar or nyse.NyseCalendar(first - timedelta(days=40), target.session_date)
    return [
        *_provenance(target, promoted, limits, calendar, first),
        *_basket(target, marks, book, limits, held),
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
    nothing to measure, and the stage-1 ranking is what it traded. A basket that names a panel
    run but records no blend row has nothing to measure either, and that is itself the
    failure: the blend that chose its names is missing, so its single check fails."""
    if panel is None:
        return []
    if panel.shortlisted == 0:
        return [
            Check(
                PROVENANCE,
                "panel_blend",
                None,
                f"no blend rows for panel run {panel.run_id}",
                "a blend row for every shortlisted name",
                False,
            )
        ]
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
    target: Basket,
    marks: Mapping[str, Decimal],
    book: Mapping[str, Decimal],
    limits: Limits,
    held: Held | None,
) -> list[Check]:
    return [
        _positions(target, limits),
        _weight_sum(target, marks, limits),
        _weights(target, marks, limits),
        _duplicates(target),
        _cash(target, marks),
        _turnover(target, marks, book, limits, held),
    ]


def _positions(target: Basket, limits: Limits) -> Check:
    """Twenty positions, or fewer where the record explains the shortfall.

    A recorded forced corporate event (agb.corporate) is the only thing that may take a slot
    out of a basket: a halt spanning the rebalance carries its incumbent with no order, and a
    position an acquisition terminated cannot be bought back. So every name the rebalance
    carried has to be on the record, and the basket may be short by no more than the number
    of names the record accounts for. A basket short for any other reason — a name that has
    quietly stopped printing, a ranking that came back thin — still halts, because the
    difference between that and a halt is a human's judgement and not an inference."""
    positions = len(target.targets)
    threshold = f"{limits.positions}, or fewer where a recorded forced event explains each one"
    explained = corporate.explained(target.forced)
    names = {t.symbol for t in target.targets}
    unexplained = sorted(
        t.symbol for t in target.targets if t.carried and t.symbol not in explained
    )
    spare = sorted(s for s in explained if s not in names)
    missing = limits.positions - positions
    measured = str(positions)
    if unexplained:
        measured = f"{positions}, carrying {', '.join(unexplained)} with nothing on the record"
    elif missing:
        measured = f"{positions}, short {missing} against {len(spare)} on the record"
    passed = not unexplained and 0 <= missing <= len(spare)
    return Check(BASKET, "positions", None, measured, threshold, passed)


def _weight_sum(target: Basket, marks: Mapping[str, Decimal], limits: Limits) -> Check:
    """The real weights, each target's shares at its mark over equity, sum to no more than one
    and to no less than the share of the twenty slots the basket filled, less the tolerance
    for each name. A slot a forced event took out leaves its weight in cash rather than being
    spread over the rest (agb.rebalance), so a nineteen-name basket sums to just under 0.95,
    and one whose weights were rescaled to one would be a different portfolio at a weight
    nobody chose. Whole shares leave every bought name a little short, which the per-name
    tolerance absorbs; a basket short by more than that in total is underinvested."""
    count = len(target.targets)
    floor = (Decimal(count) / limits.positions - count * limits.weight_tolerance).max(0)
    threshold = f"{floor:.4f} to 1"
    unpriced = _unpriced(target, marks)
    if unpriced:
        return Check(BASKET, "weight_sum", None, unpriced, threshold, False)
    total = _value(target, marks) / target.capital
    return Check(BASKET, "weight_sum", None, f"{total:.4f}", threshold, floor <= total <= 1)


def _weights(target: Basket, marks: Mapping[str, Decimal], limits: Limits) -> Check:
    """Each name's real weight, its shares at its mark over equity, against its twentieth.

    Every name the rebalance could trade sits under its twentieth by no more than the
    tolerance or one share, whichever is more: a bought name is short by less than a share,
    because buys are whole shares rounded down, and a kept name further short than the
    tolerance is topped up (agb.rebalance). A name the basket buys, entering or topped up,
    sits over its twentieth by no more than the tolerance. A kept name that has grown past it
    is not trimmed, so it has no upper bound, and a carried name cannot trade at all, so it is
    not held to the band. The check passes when every name is in its band and names each one
    that is not; a name with no mark cannot be weighed and fails unless it is carried."""
    goal = Decimal(1) / limits.positions
    slack = target.capital * limits.weight_tolerance
    threshold = (
        f"each name {goal:.2%} within {limits.weight_tolerance:.2%}, or under by at most one"
        " share; a kept name not bought may sit over, and a carried name is exempt"
    )
    bought = {o.symbol for o in target.orders if o.side == "buy"}
    weighed, outside = [], []
    for name in target.targets:
        mark = marks.get(name.symbol)
        if mark is None:
            if not name.carried:
                outside.append(f"{name.symbol} no mark")
            continue
        value = name.shares * mark
        weighed.append(value / target.capital)
        short = target.capital * goal - value
        inside = name.carried or (
            short <= max(slack, mark) and (name.symbol not in bought or -short <= slack)
        )
        if not inside:
            outside.append(f"{name.symbol} {value / target.capital:.2%}")
    if outside:
        measured = ", ".join(outside)
    elif weighed:
        measured = f"{min(weighed):.2%} to {max(weighed):.2%}"
    else:
        measured = "no names"
    return Check(BASKET, "weights", None, measured, threshold, not outside)


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
    """The cash left once the basket holds its targets and has paid for getting there: equity
    less the targets' value at their marks and every order's modelled cost. Negative cash is a
    plan that buys more than the account holds, which the broker would refuse part of, and
    which part is not ours to leave to chance."""
    threshold = ">= 0 after modelled costs"
    unpriced = _unpriced(target, marks)
    if unpriced:
        return Check(BASKET, "cash", None, unpriced, threshold, False)
    costs = sum((o.cost for o in target.orders), Decimal(0))
    cash = target.capital - _value(target, marks) - costs
    return Check(BASKET, "cash", None, _dollars(cash), threshold, cash >= 0)


def _unpriced(target: Basket, marks: Mapping[str, Decimal]) -> str | None:
    """What a basket-wide value check measures when a target has no mark, or None."""
    unpriced = sorted(t.symbol for t in target.targets if t.symbol not in marks)
    return f"no mark for {', '.join(unpriced)}" if unpriced else None


def _value(target: Basket, marks: Mapping[str, Decimal]) -> Decimal:
    """The targets' value at their marks: what the basket holds once its orders fill."""
    return sum((t.shares * marks[t.symbol] for t in target.targets), Decimal(0))


def _turnover(
    target: Basket,
    marks: Mapping[str, Decimal],
    book: Mapping[str, Decimal],
    limits: Limits,
    held: Held | None = None,
) -> Check:
    """What the plan sells, as a share of the book's value at its marks. An empty book has
    nothing to turn over.

    The limit catches a broken pipeline, not a bad month, and a basket ranked by a different
    model than the one that bought the book has a declared reason to turn it over: the two
    rankings are not comparable, so there is no rank buffer between them to keep the overlap
    high. Where the change is on the record the check admits up to a full rotation and records
    what admitted it. Nothing above a full rotation is admitted under any model, because a
    plan cannot sell more than the book is worth and anything above 100% is arithmetic that
    does not add up. An unchanged model stays on the flat limit, which is the case the check
    was written for."""
    changed = _model_changed(target, held)
    threshold = f"< {limits.turnover:.0%}"
    if changed is not None:
        threshold = f"{threshold}, or <= 100% on a changed model ({changed})"
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
    passed = ratio < limits.turnover or (changed is not None and ratio <= 1)
    return Check(BASKET, "turnover", None, f"{ratio:.2%}", threshold, passed)


def _model_changed(target: Basket, held: Held | None) -> str | None:
    """How the model behind the book differs from the one that ranked the basket, or None.

    Fail closed: the exemption has to be positively evidenced, so an unknown model behind the
    book is not a changed one. Different ranker names are a change. The same name with two
    digests on the record that differ is a retrain under that name, which is also a change.
    The same name with either digest missing reads as unchanged, because nothing on the record
    says otherwise."""
    if held is None:
        return None
    if held.ranker != target.ranker:
        return f"{held.ranker} -> {target.ranker}"
    if held.model_sha256 and target.model_sha256 and held.model_sha256 != target.model_sha256:
        return f"{target.ranker} {held.model_sha256[:12]} -> {target.model_sha256[:12]}"
    return None


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
        # A buy opens a position, or tops a kept one up: then the target it buys towards is
        # what is held plus the order. The weight check bounds how far it may go.
        side_ok = held == 0 or any(
            t.symbol == symbol and not t.carried and t.shares == held + order.qty
            for t in target.targets
        )
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
            "a buy opens a position or tops a kept one up to its target;"
            " a sell closes at most what is held",
            side_ok,
        ),
        # The cap bounds the exposure a buy adds. A sell takes exposure off, so it is exempt:
        # a held name that has grown past the cap and leaves the ranking is sold whole.
        Check(
            ORDER,
            "notional",
            symbol,
            f"{share:.2%} of {_dollars(target.capital)}",
            f"<= {limits.notional:.0%}" if order.side == "buy" else "none on a sell",
            order.side != "buy" or share <= limits.notional,
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
# and 0 for one, so the count decides whether there is a spread to read at all. The join is
# outer so that a basket naming a panel run with no blend rows still reads as a panel basket,
# with nothing shortlisted, rather than as one no panel touched.
_PANEL = """
    SELECT b.panel_run_id, b.panel_w,
           count(l.id) FILTER (WHERE NOT l.neutral), count(l.id),
           stddev_pop(l.llm_score) FILTER (WHERE NOT l.neutral)
    FROM agb.basket b
    LEFT JOIN agb.basket_blend l ON l.basket_id = b.id
    WHERE b.id = %s AND b.panel_run_id IS NOT NULL
    GROUP BY b.panel_run_id, b.panel_w
"""
_PROMOTED = "SELECT certification_id, model_id, artifact_sha256, voided FROM agb.promoted_model"
# The model behind the book: the last live basket whose orders actually filled. Not the last
# basket in `filled`, which only says its session is over: a basket every order of which
# expired unfilled reaches that state having bought nothing, and its ranker never touched the
# book. A fill is the only evidence that a model is in the holdings.
_HELD = """
    SELECT b.ranker, b.model_sha256
    FROM agb.basket b
    WHERE b.kind = 'live' AND b.id <> %s AND EXISTS (
        SELECT 1 FROM agb.broker_order o JOIN agb.fill f ON f.order_id = o.id
        WHERE o.basket_id = b.id
    )
    ORDER BY b.session_date DESC, b.id DESC
    LIMIT 1
"""
_TARGETS = """
    SELECT symbol, weight, shares, carried FROM agb.basket_target
    WHERE basket_id = %s ORDER BY id
"""
_ORDERS = """
    SELECT symbol, side, qty, tif, notional_usd, notional_usd * modelled_cost_bps / 10000
    FROM agb.basket_order
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
    id_, period, session_date, snapshot_date, universe_size, ranker, model_sha256, capital = row
    return Basket(
        id_,
        period,
        session_date,
        snapshot_date,
        universe_size,
        ranker,
        model_sha256,
        capital,
        targets,
        orders,
        panel,
        corporate.for_period(conn, period),
    )


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
            assert current is not None  # a recorded verdict belongs to a basket with a state
            gated = tuple(Check(*c) for c in conn.execute(_CHECKS, (run_id,)).fetchall())
            return Verdict(basket_id, mode, current[0], gated, recorded=True)
        if current is None or current[0] != "proposed":
            state = current[0] if current else "unproposed"
            raise GateError(f"basket {basket_id} is {state}, not proposed")
        seq = current[1] + 1

        target = load(conn, basket_id)
        live = conn.execute(_PROMOTED).fetchone()
        anchor = conn.execute(_HELD, (basket_id,)).fetchone()
        checks = evaluate(
            target,
            members=members,
            marks=marks,
            book=basket.book(conn, basket.signal_session(target.session_date, calendar)),
            now=now,
            promoted=Promoted(*live) if live is not None else None,
            held=Held(*anchor) if anchor is not None else None,
            limits=limits,
            calendar=calendar,
        )
        (mode,) = one(conn.execute("SELECT mode FROM agb.current_mode"))
        (asks,) = one(conn.execute("SELECT required FROM agb.paper_approval"))
        (level,) = one(conn.execute("SELECT level FROM agb.current_level"))
        (run_id,) = one(conn.execute(_INSERT_RUN, (basket_id, len(checks), mode, PIPELINE)))
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
            (event_id,) = one(
                conn.execute(_MOVE, (basket_id, seq, "approved", SYSTEM, basket.SURFACE, reason))
            )
            # Nobody is asked, so the approval the month expects is this one.
            jobs.fulfil(
                conn, schedule.APPROVAL_REQUEST, target.period, "basket_event", event_id, PIPELINE
            )
            state = "approved"
        elif mode == LIVE and level == UNASKED:
            # At L2 the rebalance proceeds; the month's approval is the notification the bot
            # sends afterwards, which fulfils it when it goes out.
            reason = f"passed the pre-trade gate at {UNASKED}, which proceeds unasked"
            conn.execute(_MOVE, (basket_id, seq, "approved", SYSTEM, basket.SURFACE, reason))
            state = "approved"
        else:
            state = "proposed"
    return Verdict(basket_id, mode, state, tuple(checks))


def _describe(check: Check) -> str:
    name = f"{check.name} {check.symbol}" if check.symbol else check.name
    return f"{name} measured {check.measured}, needs {check.threshold}"
