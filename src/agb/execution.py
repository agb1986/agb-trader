"""Paper MOC execution: the approved basket becomes market-on-close orders at the broker.

Execution is the only service that talks to the broker, and this module is what it does with
an approved basket, in three parts that the service runs each cycle.

Submission, in the quarter hour before the MOC cutoff of the basket's trade session. First the
broker's own data is checked the way the gate checks the basket: the account is active and
nothing is blocked or suspended, every name is tradable, and fractionable where the quantity
needs it. The halt latch is read before those checks, so an open cause halts the basket as
itself and a pause the broker carries as `suspend_trade` is never taken for a failed check.
A failed check halts the basket with a `gate_breach`, exactly as a failed gate check does,
and the checks are recorded once so a later cycle reads the verdict back; the broker is read
outside the transaction that records them. Then every quote the orders need is read in one
request on the named feed: a name without a quote sends nothing at all, and the error raises
the one `submission_failed` alert the service raises for a failed step and is tried again
next cycle. Then one order at a time: the halt latch is read again before each, so a cause
opened partway through stops every order not yet sent and cancels those already sent; the
order's row is written and committed, with the quoted mid, before the broker is called; and
the broker's answer is appended as an event. A `client_order_id` the broker has already seen
is looked up under that id and never sent twice. When every order has an answer the basket is
submitted.

Reconciliation, after the close. Each order is read back and every status change appended;
each fill the account reports is one `fill` row carrying the regulatory fees the shared
schedule charges on that fill alone; and the basket is filled once the broker is done with
every order. A basket that filled short raises a `partial_rebalance_short_fill` alert naming
what happened.

The daily snapshot, once per closed session: the broker's positions and the account flags,
so the halt latch can be compared with the broker's `suspend_trade`, and a snapshot with the
previous one plus the day's fills.

The broker latch, first thing each cycle. The operator's surfaces write the database half of
`/pause` and `/resume` themselves and record the command; execution carries each accepted
command to the broker in the order they arrived. A pause first cancels the sent orders of any
submitted basket whose session has not reached its cancel deadline, ten minutes before the
close, and halts that basket, then sets `suspend_trade`; a resume unsets it. Either way the
account is read back, the result recorded against the command, and a confirmation raised for
the bot to deliver. A broker that refuses leaves the command waiting for the next cycle.

Liquidation, straight after the latch. The bot opens the `liquidation` halt cause in the same
transaction as the operator's confirmation, so nothing new goes out; execution then cancels
what a submitted basket has in flight, as a pause does, and calls the broker's close-all,
which cancels every open order and closes every position at market. Its HTTP 207 answer is
recorded symbol by symbol, failures included, the moment it arrives, so a failure after that
is retried from the record and never sends a second close-all; then it is written up as the
confirmation's result and raised as an alert. The closing orders' fills are recorded like a
basket's, so the next snapshot accounts for them. `suspend_trade` is left
alone, since it would refuse the closing orders, and the `liquidation` cause stays open until
`/resume`. A confirmation whose cause has been cleared before execution reached it is
recorded as not carried out.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb

from agb import cost, jobs, nyse, schedule
from agb.alpaca import AlpacaError, Broker, Closed, Order, Quote
from agb.gate import Check
from agb.roles import EXECUTION

log = logging.getLogger(__name__)

ROLE = EXECUTION
ACTOR = ROLE
SURFACE = ROLE
# Orders go in during this window before the session's MOC cutoff, which the calendar puts
# fifteen minutes before the close: never earlier, never after. The scheduler owns the figure
# because the submission job becomes claimable at the same moment.
SUBMIT_WINDOW = schedule.SUBMIT_WINDOW
# How long execution holds the submission job. The lease runs out as the window closes, so a
# crash mid-submission is visible by the time the orders could no longer have gone in.
SUBMISSION_LEASE = schedule.SUBMIT_WINDOW
# How long after the close the broker is given before fills and positions are read back.
SETTLE = timedelta(minutes=5)
# How far before a basket's first order, or before the reconcile itself, the fills are read. The
# broker's clock is not this database's clock, and `created_at` is this database's, so a fill
# stamped a moment before the row that caused it would be dropped for good by an exact window.
# The reconcile's own clock comes into it because that is the session being read back, which is
# the window the fills belong to however long ago the rows were written. Re-reading a day costs
# nothing: a fill row is keyed by the broker's execution id, so one already recorded is skipped.
FILL_LOOKBACK = timedelta(days=1)
# The gate's levels for the broker checks.
ACCOUNT = "account"
ORDER = "order"
# The two order events a reconcile appends: the broker's own view of an order, or the fact
# that it has never seen it. Neither is a trading decision; both are only what was found.
RECONCILED = "reconciled"
UNSENT = "unsent"
# Broker order states from which nothing more will come. `pending_cancel` is not one: the
# broker has the cancel but may still fill the order before it lands.
DONE = frozenset({"filled", "canceled", "expired", "rejected", "done_for_day"})
PENDING_CANCEL = "pending_cancel"
# A basket left short raises one alert per cause, each its own kind: an alert is keyed on
# (kind, subject, period), so causes sharing a kind would let the first one raised hide the
# rest. Refused at submission, halted mid-submission with orders still live, halted by a
# pause with orders still live, and filled short at the close.
PARTIAL_REFUSED = "partial_rebalance_refused"
PARTIAL_HALTED = "partial_rebalance_halted"
PARTIAL_PAUSED = "partial_rebalance_paused"
PARTIAL_SHORT_FILL = "partial_rebalance_short_fill"
# The kind all four shared until they were split, still on the alerts raised before then.
PARTIAL_REBALANCE = "partial_rebalance"
# A pause cancels a submitted basket's orders only up to this long before the close.
CANCEL_BEFORE = timedelta(minutes=10)
PAUSE = "pause"
RESUME = "resume"
# The halt cause an expired submission lease opens. Only a human clears it.
LEASE_EXPIRED = "execution_lease_expired"
LATCH_FAILED = "latch_failed"
# A run of cycles that all ended on an error, and the run ending. One pair per run, not one
# alert per failed cycle: a broker that is down for an hour is one thing that happened.
CYCLE_FAILING = "cycle_failing"
CYCLE_RECOVERED = "cycle_recovered"
LIQUIDATION = "liquidation"
LIQUIDATION_FAILED = "liquidation_failed"


class ExecutionError(Exception):
    """This cycle could not finish; the next one starts where this one stopped."""


@dataclass(frozen=True)
class Basket:
    """A live basket as the state view shows it, waiting in `approved` or `submitted`."""

    id: int
    reference: str
    period: str
    session_date: date
    state: str
    seq: int


@dataclass(frozen=True)
class Planned:
    """One of the basket's orders as the pipeline planned it."""

    id: int
    client_order_id: str
    symbol: str
    side: str
    qty: Decimal
    cost_model_version: str
    modelled_cost_bps: Decimal


@dataclass(frozen=True)
class Sent:
    """An order row with the broker's latest word on it, from `agb.order_state`."""

    id: int
    basket_order_id: int
    client_order_id: str
    symbol: str
    side: str
    qty: Decimal
    kind: str | None
    status: str | None
    filled_qty: Decimal
    broker_order_id: str | None

    @property
    def answered(self) -> bool:
        """The broker has acknowledged, been found holding, or refused this order.

        An `unsent` event is the opposite: a reconcile asked and the broker had never seen
        the order. Once the operator clears the halt, that order is still to send.
        """
        return self.kind is not None and self.kind != UNSENT

    @property
    def live(self) -> bool:
        return self.broker_order_id is not None and self.kind != "rejected"

    @property
    def done(self) -> bool:
        return self.kind == "rejected" or self.status in DONE


@dataclass(frozen=True)
class Command:
    """An accepted pause or resume that execution has not yet carried to the broker."""

    id: int
    verb: str
    actor: str
    at: datetime
    args: dict


@dataclass(frozen=True)
class Outcome:
    """What one cycle did to one basket."""

    basket_id: int
    state: str
    sent: int = 0
    reason: str | None = None


_WAITING = """
    SELECT id, reference, period, session_date, state, seq FROM agb.basket_state
    WHERE state = %s AND kind = 'live' ORDER BY session_date, id
"""
_STATE = "SELECT state, seq FROM agb.basket_state WHERE id = %s"
_PLANNED = """
    SELECT id, client_order_id, symbol, side, qty, cost_model_version, modelled_cost_bps
    FROM agb.basket_order WHERE basket_id = %s ORDER BY id
"""
_SENT = """
    SELECT s.order_id, o.basket_order_id, s.client_order_id, s.symbol, s.side, s.qty,
           s.kind, s.status, s.filled_qty, s.broker_order_id
    FROM agb.order_state s JOIN agb.broker_order o ON o.id = s.order_id
    WHERE s.basket_id = %s ORDER BY s.order_id
"""
_HALTS = "SELECT cause FROM agb.halt_open ORDER BY at"
_CHECK_RUN = "SELECT id FROM agb.broker_check_run WHERE basket_id = %s"
_INSERT_CHECK_RUN = """
    INSERT INTO agb.broker_check_run (basket_id, checks, actor) VALUES (%s, %s, %s) RETURNING id
"""
_CHECKS = """
    SELECT level, name, symbol, measured, threshold, passed FROM agb.broker_check
    WHERE run_id = %s ORDER BY id
"""
_INSERT_CHECK = """
    INSERT INTO agb.broker_check (run_id, level, name, symbol, measured, threshold, passed)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
_MOVE = """
    INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface, reason)
    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
"""
_BREACH = """
    INSERT INTO agb.halt_event (cause, action, basket_id, actor, reason)
    VALUES ('gate_breach', 'set', %s, %s, %s)
"""
_INSERT_ORDER = """
    INSERT INTO agb.broker_order (basket_id, basket_order_id, client_order_id, symbol, side,
        qty, cost_model_version, modelled_cost_bps, nbbo_bid, nbbo_ask, nbbo_mid, nbbo_at, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
"""
_INSERT_EVENT = """
    INSERT INTO agb.order_event (order_id, kind, broker_order_id, status, filled_qty,
        filled_avg_price, detail, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
_INSERT_STATE = """
    INSERT INTO agb.broker_state (account_status, suspend_trade, no_shorting, trading_blocked,
        account_blocked, equity_usd, cash_usd, buying_power_usd, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
_INSERT_FILL = """
    INSERT INTO agb.fill (order_id, execution_id, qty, price, filled_at, sec_fee_usd,
        taf_fee_usd, cat_fee_usd, commission_usd, fee_model_version, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (execution_id) DO NOTHING
"""
_ORDER_BY_BROKER_ID = """
    SELECT o.id FROM agb.broker_order o
    WHERE EXISTS (SELECT 1 FROM agb.order_event e
                  WHERE e.order_id = o.id AND e.broker_order_id = %s)
"""
_LIQUIDATION_ORDER_BY_BROKER_ID = """
    SELECT id FROM agb.liquidation_order WHERE broker_order_id = %s
"""
_INSERT_LIQUIDATION_FILL = """
    INSERT INTO agb.liquidation_fill (order_id, execution_id, side, qty, price, filled_at,
        sec_fee_usd, taf_fee_usd, cat_fee_usd, commission_usd, fee_model_version, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (execution_id) DO NOTHING
"""
_EARLIEST_ORDER = "SELECT min(created_at) FROM agb.broker_order WHERE basket_id = %s"
_SNAPSHOT = "SELECT id, observed_at FROM agb.position_snapshot WHERE snapshot_date = %s"
_LAST_SNAPSHOT = """
    SELECT observed_at FROM agb.position_snapshot ORDER BY snapshot_date DESC LIMIT 1
"""
_INSERT_SNAPSHOT = """
    INSERT INTO agb.position_snapshot (snapshot_date, positions, actor)
    VALUES (%s, %s, %s) RETURNING id
"""
_INSERT_POSITION = """
    INSERT INTO agb.position (snapshot_id, symbol, qty, avg_entry_price, market_value_usd)
    VALUES (%s, %s, %s, %s, %s)
"""
_LATCH_PENDING = "SELECT id, verb, actor, at, args FROM agb.latch_pending"
_INSERT_LATCH = """
    INSERT INTO agb.broker_latch (command_id, suspend_trade, observed_suspend, detail, actor)
    VALUES (%s, %s, %s, %s, %s)
"""
_PAUSE_OPEN = "SELECT at, actor FROM agb.halt_open WHERE cause = 'pause'"
_LIQUIDATION_PENDING = "SELECT id, actor, at FROM agb.liquidation_pending"
_INSERT_RESULT = """
    INSERT INTO agb.liquidation (kind, follows, symbols, detail, actor)
    VALUES ('result', %s, %s, %s, %s)
"""
_INSERT_ANSWER = """
    INSERT INTO agb.liquidation_order (confirm_id, symbol, status, broker_order_id, message,
        actor)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
_ANSWER = """
    SELECT symbol, status, broker_order_id, message FROM agb.liquidation_order
    WHERE confirm_id = %s ORDER BY id
"""
_HALTED_SINCE = """
    SELECT b.reference, e.at, e.reason
    FROM agb.basket_event e JOIN agb.basket b ON b.id = e.basket_id
    WHERE e.to_state = 'halted' AND e.surface = %s AND e.at >= %s
    ORDER BY e.id
"""
_RAISE = """
    INSERT INTO agb.alert (kind, subject, period, body, raised_by)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (kind, subject, period) DO NOTHING
"""


def waiting(conn: psycopg.Connection, state: str) -> list[Basket]:
    """The live baskets in `state`, earliest session first."""
    return [Basket(*row) for row in conn.execute(_WAITING, (state,)).fetchall()]


def planned(conn: psycopg.Connection, basket_id: int) -> list[Planned]:
    return [Planned(*row) for row in conn.execute(_PLANNED, (basket_id,)).fetchall()]


def sent(conn: psycopg.Connection, basket_id: int) -> list[Sent]:
    return [Sent(*row) for row in conn.execute(_SENT, (basket_id,)).fetchall()]


def open_halts(conn: psycopg.Connection) -> list[str]:
    """The open halt causes, oldest first. Read before every order."""
    return [cause for (cause,) in conn.execute(_HALTS).fetchall()]


def submit_window(calendar: nyse.NyseCalendar, session: date) -> tuple[datetime, datetime]:
    """When orders for `session` go in: from `SUBMIT_WINDOW` before the MOC cutoff up to it."""
    cutoff = calendar.moc_cutoff(session)
    return cutoff - SUBMIT_WINDOW, cutoff


def record_state(conn: psycopg.Connection, broker: Broker) -> tuple[bool, bool]:
    """Read the account and its configuration and append what the broker said to
    `broker_state`. Returns (suspend_trade, anything blocked)."""
    account = broker.account()
    configuration = broker.configuration()
    conn.execute(
        _INSERT_STATE,
        (
            account.status or "unknown",
            configuration.suspend_trade,
            configuration.no_shorting,
            account.trading_blocked,
            account.account_blocked,
            account.equity,
            account.cash,
            account.buying_power,
            ACTOR,
        ),
    )
    blocked = account.trading_blocked or account.account_blocked
    return configuration.suspend_trade, blocked


def broker_checks(broker: Broker, orders: Sequence[Planned]) -> list[Check]:
    """The broker's data against what a submission needs, in the gate's shape."""
    account = broker.account()
    configuration = broker.configuration()
    checks = [
        Check(
            ACCOUNT,
            "account_status",
            None,
            account.status or "unknown",
            "ACTIVE",
            account.status == "ACTIVE",
        ),
        Check(
            ACCOUNT,
            "trading_blocked",
            None,
            str(account.trading_blocked).lower(),
            "false",
            not account.trading_blocked,
        ),
        Check(
            ACCOUNT,
            "account_blocked",
            None,
            str(account.account_blocked).lower(),
            "false",
            not account.account_blocked,
        ),
        Check(
            ACCOUNT,
            "suspend_trade",
            None,
            str(configuration.suspend_trade).lower(),
            "false",
            not configuration.suspend_trade,
        ),
    ]
    for order in orders:
        try:
            asset = broker.asset(order.symbol)
        except AlpacaError as exc:
            if not exc.refused:
                raise
            checks.append(Check(ORDER, "tradable", order.symbol, "unknown", "true", False))
            continue
        checks.append(
            Check(
                ORDER, "tradable", order.symbol, str(asset.tradable).lower(), "true", asset.tradable
            )
        )
        checks.append(
            Check(
                ORDER,
                "asset_status",
                order.symbol,
                asset.status or "unknown",
                "active",
                asset.status == "active",
            )
        )
        if order.qty != order.qty.to_integral_value():
            checks.append(
                Check(
                    ORDER,
                    "fractionable",
                    order.symbol,
                    str(asset.fractionable).lower(),
                    "true",
                    asset.fractionable,
                )
            )
    return checks


def describe(check: Check) -> str:
    name = f"{check.name} {check.symbol}" if check.symbol else check.name
    return f"{name} measured {check.measured}, needs {check.threshold}"


def _verdict(conn: psycopg.Connection, run_id: int) -> tuple[bool, str | None]:
    checks = [Check(*c) for c in conn.execute(_CHECKS, (run_id,)).fetchall()]
    failed = [c for c in checks if not c.passed]
    if failed:
        return False, "broker checks failed: " + "; ".join(describe(c) for c in failed)
    return True, None


def check(conn: psycopg.Connection, broker: Broker, basket: Basket) -> tuple[bool, str | None]:
    """Run the broker checks once for `basket` and record them; halt the basket on any
    failure, in the same transaction as the verdict. A basket already checked reads its
    verdict back. Returns (passed, reason).

    The broker is read before the transaction opens, so no database lock waits on the
    network; the transaction then looks for a verdict once more and records this one only
    when there is none."""
    recorded = conn.execute(_CHECK_RUN, (basket.id,)).fetchone()
    if recorded is not None:
        return _verdict(conn, recorded[0])
    checks = broker_checks(broker, planned(conn, basket.id))
    with conn.transaction():
        recorded = conn.execute(_CHECK_RUN, (basket.id,)).fetchone()
        if recorded is not None:
            return _verdict(conn, recorded[0])
        (run_id,) = conn.execute(_INSERT_CHECK_RUN, (basket.id, len(checks), ACTOR)).fetchone()
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_CHECK,
                [
                    (run_id, c.level, c.name, c.symbol, c.measured, c.threshold, c.passed)
                    for c in checks
                ],
            )
        failed = [c for c in checks if not c.passed]
        if not failed:
            return True, None
        reason = "broker checks failed: " + "; ".join(describe(c) for c in failed)
        _halt(conn, basket, reason, breach=True)
        return False, reason


def _halt(conn: psycopg.Connection, basket: Basket, reason: str, *, breach: bool) -> None:
    """Move an approved basket to halted, opening `gate_breach` when the fault is the
    basket's own rather than a latch someone else opened."""
    current = conn.execute(_STATE, (basket.id,)).fetchone()
    if current is None or current[0] != "approved":
        raise ExecutionError(f"basket {basket.id} is {current[0] if current else 'unknown'}")
    conn.execute(_MOVE, (basket.id, current[1] + 1, "approved", "halted", ACTOR, SURFACE, reason))
    if breach:
        conn.execute(_BREACH, (basket.id, ACTOR, reason))


def _event(
    conn: psycopg.Connection,
    order_id: int,
    kind: str,
    order: Order | None,
    *,
    status: str | None = None,
    detail: str | None = None,
) -> None:
    conn.execute(
        _INSERT_EVENT,
        (
            order_id,
            kind,
            order.id if order else None,
            status or (order.status if order else "unknown"),
            order.filled_qty if order else Decimal(0),
            order.filled_avg_price if order else None,
            detail,
            ACTOR,
        ),
    )


def raise_alert(conn: psycopg.Connection, kind: str, subject: str, period: str, body: str):
    conn.execute(_RAISE, (kind, subject, period, body, ACTOR))


def _quotes(broker: Broker, symbols: Sequence[str]) -> dict[str, Quote]:
    """Every symbol's quote in one request, or an error naming each symbol without one. It
    is read before the first order goes out, so a missing quote sends nothing."""
    wanted = list(dict.fromkeys(symbols))
    if not wanted:
        return {}
    quotes = broker.latest_quotes(wanted)
    missing = [s for s in wanted if s not in quotes]
    if missing:
        raise ExecutionError(f"no quote for {', '.join(missing)}; no order sent")
    return quotes


def _cancel_sent(conn: psycopg.Connection, broker: Broker, orders: Sequence[Sent]) -> list[Sent]:
    """Cancel every order the broker holds that is not done. Returns those that moved the
    book or may still move it, each with the broker's latest word on it: an order still live,
    one the broker is still cancelling, and one that filled in part or in full first."""
    remaining = []
    for order in orders:
        if not order.live or order.done:
            continue
        _event(conn, order.id, "cancel_requested", None, status=PENDING_CANCEL)
        try:
            broker.cancel(order.broker_order_id)
            latest = broker.order(order.broker_order_id)
        except AlpacaError as exc:
            _event(conn, order.id, "cancel_failed", None, status="unknown", detail=str(exc))
            remaining.append(order)
            continue
        answered = replace(order, status=latest.status, filled_qty=latest.filled_qty)
        if latest.status == "canceled":
            _event(conn, order.id, "cancelled", latest)
            if latest.filled_qty > 0:
                remaining.append(answered)
        elif latest.status in DONE:
            # Done before the cancel landed: nothing left to cancel, and if it filled the
            # book has moved.
            _event(conn, order.id, "status", latest)
            if latest.filled_qty > 0:
                remaining.append(answered)
        elif latest.status == PENDING_CANCEL:
            # The broker has the cancel but not yet a final word, and may fill the order
            # first. It stays open until a later read-back sees how it ended.
            _event(conn, order.id, "status", latest)
            remaining.append(answered)
        else:
            _event(conn, order.id, "cancel_failed", latest, detail=f"still {latest.status}")
            remaining.append(answered)
    return remaining


def _named(order: Sent) -> str:
    """An order `_cancel_sent` left behind, as a halt's reason and alert name it."""
    name = f"{order.side} {order.qty} {order.symbol}"
    if order.status in DONE or order.status == PENDING_CANCEL:
        return f"{name} ({order.status}, {order.filled_qty} filled)"
    return name


def _stop(
    conn: psycopg.Connection, broker: Broker, basket: Basket, causes: Sequence[str]
) -> Outcome:
    """A halt cause open at or during submission: cancel what was sent, halt the basket, and
    alert on anything that stayed live or filled before the cancel landed."""
    already = sent(conn, basket.id)
    remaining = _cancel_sent(conn, broker, already)
    live = [o for o in already if o.live]
    when = "mid-submission" if live else "before submission"
    reason = (
        f"halt open {when} ({', '.join(causes)}): {len(live)} of "
        f"{len(planned(conn, basket.id))} orders sent, {len(remaining)} still live or filled"
    )
    with conn.transaction():
        _halt(conn, basket, reason, breach=False)
    if remaining:
        names = ", ".join(_named(o) for o in remaining)
        raise_alert(
            conn,
            PARTIAL_HALTED,
            basket.reference,
            basket.period,
            f"basket {basket.reference} halted mid-submission; still live or filled at the "
            f"broker: {names}",
        )
    log.warning("basket %s: %s", basket.reference, reason)
    return Outcome(basket.id, "halted", len(live), reason)


def submit(conn: psycopg.Connection, broker: Broker, basket: Basket, now: datetime) -> Outcome:
    """Send the basket's orders, one at a time, and move it to submitted when each has an
    answer. `conn` is in autocommit mode: each order's row is committed before the broker is
    called, so a crash between the call and the answer leaves a row to look up under its
    client_order_id, and never a second order.

    The halt latch is read before the broker checks, so a pause the broker already carries
    as `suspend_trade` halts the basket as a pause rather than as a failed check. Every quote
    an unsent order needs is read in one request before the first order goes out; a symbol
    without one raises, and nothing is sent."""
    causes = open_halts(conn)
    if causes:
        return _stop(conn, broker, basket, causes)
    passed, reason = check(conn, broker, basket)
    if not passed:
        log.warning("basket %s: %s", basket.reference, reason)
        return Outcome(basket.id, "halted", 0, reason)
    answered = {s.basket_order_id: s for s in sent(conn, basket.id)}
    orders = planned(conn, basket.id)
    quotes = _quotes(broker, [o.symbol for o in orders if o.id not in answered])
    rejected: list[Sent] = []
    count = 0
    for order in orders:
        existing = answered.get(order.id)
        if existing is not None and existing.answered:
            if existing.kind == "rejected":
                rejected.append(existing)
            count += 1
            continue
        causes = open_halts(conn)
        if causes:
            return _stop(conn, broker, basket, causes)
        if existing is not None:
            # A row without an answer: the broker may or may not have the order.
            order_id = existing.id
            try:
                found = broker.order_by_client_id(order.client_order_id)
            except AlpacaError as exc:
                if not exc.not_found:
                    raise ExecutionError(f"{order.client_order_id}: {exc}") from exc
            else:
                _event(conn, order_id, "lookup", found)
                count += 1
                continue
        else:
            quote = quotes[order.symbol]
            (order_id,) = conn.execute(
                _INSERT_ORDER,
                (
                    basket.id,
                    order.id,
                    order.client_order_id,
                    order.symbol,
                    order.side,
                    order.qty,
                    order.cost_model_version,
                    order.modelled_cost_bps,
                    quote.bid,
                    quote.ask,
                    quote.mid,
                    quote.at,
                    ACTOR,
                ),
            ).fetchone()
        try:
            acked = broker.submit(order.symbol, order.qty, order.side, order.client_order_id)
        except AlpacaError as exc:
            if exc.duplicate:
                found = broker.order_by_client_id(order.client_order_id)
                _event(conn, order_id, "lookup", found, detail=str(exc))
            elif exc.refused:
                _event(conn, order_id, "rejected", None, status="rejected", detail=str(exc))
                rejected.append(
                    Sent(
                        order_id,
                        order.id,
                        order.client_order_id,
                        order.symbol,
                        order.side,
                        order.qty,
                        "rejected",
                        "rejected",
                        Decimal(0),
                        None,
                    )
                )
                log.warning("basket %s: %s refused: %s", basket.reference, order.symbol, exc)
            else:
                raise ExecutionError(f"{order.client_order_id}: {exc}") from exc
        else:
            _event(conn, order_id, "ack", acked)
        count += 1
    with conn.transaction():
        current = conn.execute(_STATE, (basket.id,)).fetchone()
        if current is None or current[0] != "approved":
            raise ExecutionError(f"basket {basket.id} is no longer approved")
        reason = f"{count} orders answered by the broker at {now.astimezone(UTC):%H:%M:%SZ}"
        (event_id,) = conn.execute(
            _MOVE, (basket.id, current[1] + 1, "approved", "submitted", ACTOR, SURFACE, reason)
        ).fetchone()
        jobs.fulfil(conn, schedule.SUBMISSION, basket.period, "basket_event", event_id, ACTOR)
    if rejected:
        names = ", ".join(f"{o.side} {o.qty} {o.symbol}" for o in rejected)
        raise_alert(
            conn,
            PARTIAL_REFUSED,
            basket.reference,
            basket.period,
            f"basket {basket.reference} submitted with {len(rejected)} of {count} orders "
            f"refused by the broker: {names}",
        )
    log.info("basket %s submitted: %s", basket.reference, reason)
    return Outcome(basket.id, "submitted", count, reason)


def _fees(side: str, qty: Decimal, price: Decimal) -> cost.Fees:
    """The schedule's charge on one fill alone. The broker writes a short sale's side as
    `sell_short`; the schedule charges it as a sale."""
    return cost.fees(cost.Side.SELL if side.startswith("sell") else cost.Side.BUY, qty * price, qty)


def ingest_fills(conn: psycopg.Connection, broker: Broker, after: datetime) -> int:
    """Record every fill the account reports after `after` on an order this database knows,
    one row each with the schedule's fees on it: a basket's order in `agb.fill`, a
    liquidation's closing order in `agb.liquidation_fill`. Returns how many rows were new."""
    added = 0
    known: dict[str, tuple[str, int] | None] = {}
    for fill in broker.fills(after):
        if fill.order_id not in known:
            known[fill.order_id] = _known_order(conn, fill.order_id)
        order = known[fill.order_id]
        if order is None:
            continue
        kind, order_id = order
        fees = _fees(fill.side, fill.qty, fill.price)
        charged = (fees.sec, fees.taf, fees.cat, fees.commission, cost.CURRENT.version, ACTOR)
        if kind == ORDER:
            done = conn.execute(
                _INSERT_FILL,
                (order_id, fill.id, fill.qty, fill.price, fill.at, *charged),
            )
        else:
            side = "sell" if fill.side.startswith("sell") else "buy"
            done = conn.execute(
                _INSERT_LIQUIDATION_FILL,
                (order_id, fill.id, side, fill.qty, fill.price, fill.at, *charged),
            )
        added += done.rowcount
    return added


def _known_order(conn: psycopg.Connection, broker_order_id: str) -> tuple[str, int] | None:
    """Which recorded order a broker order id is: a basket's or a liquidation's, or neither."""
    row = conn.execute(_ORDER_BY_BROKER_ID, (broker_order_id,)).fetchone()
    if row is not None:
        return ORDER, row[0]
    row = conn.execute(_LIQUIDATION_ORDER_BY_BROKER_ID, (broker_order_id,)).fetchone()
    if row is not None:
        return LIQUIDATION, row[0]
    return None


def reconcile(conn: psycopg.Connection, broker: Broker, basket: Basket, now: datetime) -> Outcome:
    """After the close: read each order back, append what changed, record the fills, and
    move the basket to filled once the broker is done with every order."""
    orders = sent(conn, basket.id)
    for order in orders:
        if not order.live or order.done:
            continue
        latest = broker.order(order.broker_order_id)
        if latest.status != order.status or latest.filled_qty != order.filled_qty:
            _event(conn, order.id, "status", latest)
    (earliest,) = conn.execute(_EARLIEST_ORDER, (basket.id,)).fetchone()
    if earliest is not None:
        ingest_fills(conn, broker, min(earliest, now) - FILL_LOOKBACK)
    orders = sent(conn, basket.id)
    if not all(o.done for o in orders):
        pending = [o.symbol for o in orders if not o.done]
        return Outcome(basket.id, "submitted", len(orders), f"waiting on {', '.join(pending)}")
    short = [o for o in orders if o.filled_qty < o.qty]
    with conn.transaction():
        current = conn.execute(_STATE, (basket.id,)).fetchone()
        if current is None or current[0] != "submitted":
            raise ExecutionError(f"basket {basket.id} is no longer submitted")
        reason = (
            f"{len(orders) - len(short)} of {len(orders)} orders filled in full at "
            f"{now.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}"
        )
        conn.execute(
            _MOVE, (basket.id, current[1] + 1, "submitted", "filled", ACTOR, SURFACE, reason)
        )
    if short:
        names = ", ".join(f"{o.side} {o.qty} {o.symbol} filled {o.filled_qty}" for o in short)
        raise_alert(
            conn,
            PARTIAL_SHORT_FILL,
            basket.reference,
            basket.period,
            f"basket {basket.reference} filled short on {len(short)} of {len(orders)} orders: "
            f"{names}",
        )
    log.info("basket %s filled: %s", basket.reference, reason)
    return Outcome(basket.id, "filled", len(orders), reason)


def snapshot(
    conn: psycopg.Connection, broker: Broker, session: date, calendar: nyse.NyseCalendar
) -> bool:
    """The daily snapshot for a closed `session`, once: the fills since the previous
    snapshot, the broker's positions and the account state. Returns False if it exists."""
    if conn.execute(_SNAPSHOT, (session,)).fetchone() is not None:
        return False
    previous = conn.execute(_LAST_SNAPSHOT).fetchone()
    since = previous[0] if previous else calendar.open_at(session)
    ingest_fills(conn, broker, since)
    positions = broker.positions()
    with conn.transaction():
        (snapshot_id,) = conn.execute(_INSERT_SNAPSHOT, (session, len(positions), ACTOR)).fetchone()
        with conn.cursor() as cur:
            cur.executemany(
                _INSERT_POSITION,
                [
                    (snapshot_id, p.symbol, p.qty, p.avg_entry_price, p.market_value)
                    for p in positions
                    if p.qty != 0
                ],
            )
    record_state(conn, broker)
    log.info("snapshot %s: %d positions", session, len(positions))
    return True


# -- the broker latch ----------------------------------------------------------------------


def pending_latch(conn: psycopg.Connection) -> list[Command]:
    """The accepted pauses and resumes not yet carried to the broker, oldest first."""
    return [Command(*row) for row in conn.execute(_LATCH_PENDING).fetchall()]


def cancel_deadline(calendar: nyse.NyseCalendar, session: date) -> datetime:
    """The last moment a pause cancels `session`'s orders: `CANCEL_BEFORE` before its close,
    15:50 ET on a full day and ten minutes before an early close."""
    return calendar.close_at(session) - CANCEL_BEFORE


def cancel_in_flight(
    conn: psycopg.Connection, broker: Broker, calendar: nyse.NyseCalendar, now: datetime
) -> list[str]:
    """A pause's cancel: every submitted basket whose session has not closed. Before the
    cancel deadline its sent orders are cancelled and it is halted, with a partial-rebalance
    alert naming any order that stayed live, is still being cancelled, or filled in part or in
    full before the cancel landed; after it, the orders are left to the close.
    Returns a line for each basket left to the close."""
    left = []
    for basket in waiting(conn, "submitted"):
        close = calendar.close_at(basket.session_date)
        if now >= close:
            continue
        orders = sent(conn, basket.id)
        live = [o for o in orders if o.live and not o.done]
        deadline = cancel_deadline(calendar, basket.session_date)
        if now >= deadline:
            left.append(
                f"basket {basket.reference}: past the "
                f"{deadline.astimezone(nyse.NEW_YORK):%H:%M} ET cancel deadline, "
                f"{len(live)} orders left to the close"
            )
            continue
        remaining = _cancel_sent(conn, broker, orders)
        # A cancelled order that filled in part was cancelled, and is still named below.
        cancelled = len(live) - sum(1 for o in remaining if o.status != "canceled")
        reason = f"paused mid-submission: {cancelled} of {len(live)} orders cancelled"
        if remaining:
            names = ", ".join(_named(o) for o in remaining)
            reason += f"; still live or filled at the broker: {names}"
        with conn.transaction():
            current = conn.execute(_STATE, (basket.id,)).fetchone()
            if current is None or current[0] != "submitted":
                raise ExecutionError(f"basket {basket.id} is no longer submitted")
            conn.execute(
                _MOVE, (basket.id, current[1] + 1, "submitted", "halted", ACTOR, SURFACE, reason)
            )
        if remaining:
            raise_alert(
                conn,
                PARTIAL_PAUSED,
                basket.reference,
                basket.period,
                f"basket {basket.reference} halted by a pause mid-submission; still live or "
                f"filled at the broker: {names}",
            )
        log.warning("basket %s: %s", basket.reference, reason)
    return left


def _at(at: datetime) -> str:
    return at.astimezone(nyse.NEW_YORK).strftime("%Y-%m-%d %H:%M ET")


def _confirmation(
    conn: psycopg.Connection, command: Command, observed: bool, left: Sequence[str]
) -> str:
    """The message the operator gets once execution has carried `command` to the broker."""
    wanted = command.verb == PAUSE
    broker_line = f"broker suspend_trade: {'on' if observed else 'off'}"
    if observed != wanted:
        broker_line += f", asked for {'on' if wanted else 'off'}"
    pause = conn.execute(_PAUSE_OPEN).fetchone()
    if pause is not None:
        pause_line = f"halt cause pause: open since {_at(pause[0])}, by {pause[1]}"
    else:
        pause_line = "halt cause pause: not open"
    if wanted:
        lines = [f"PAUSE by {command.actor} at {_at(command.at)}", pause_line, broker_line]
        halted = conn.execute(_HALTED_SINCE, (SURFACE, command.at)).fetchall()
        lines += [f"basket {ref} halted at {_at(at)}: {reason}" for ref, at, reason in halted]
        lines += list(left)
        if not halted and not left:
            lines.append("no submission in flight")
    else:
        cleared = command.args.get("cleared") or []
        lines = [
            f"RESUME by {command.actor} at {_at(command.at)}",
            f"cleared: {', '.join(cleared)}" if cleared else "cleared: nothing was open",
            broker_line,
            "a halted rebalance is not resubmitted",
        ]
    return "\n".join(lines)


def apply_latch(
    conn: psycopg.Connection, broker: Broker, calendar: nyse.NyseCalendar, now: datetime
) -> int:
    """Carry each waiting pause and resume to the broker, oldest first, and raise its
    confirmation. A broker error, or a basket that moves under the pause's cancel, raises a
    `latch_failed` alert once per command and stops, leaving that command and those after it
    for the next cycle. Returns how many were carried."""
    done = 0
    for command in pending_latch(conn):
        wanted = command.verb == PAUSE
        try:
            left = cancel_in_flight(conn, broker, calendar, now) if wanted else []
            broker.suspend(wanted)
            observed, _ = record_state(conn, broker)
        except (AlpacaError, ExecutionError) as exc:
            raise_alert(
                conn,
                LATCH_FAILED,
                f"command {command.id}",
                command.at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"{command.verb.upper()} by {command.actor} at {_at(command.at)} could not "
                f"set the broker's suspend_trade {'on' if wanted else 'off'}: {exc}\n"
                "execution tries again each cycle",
            )
            log.error("command %d (%s): %s", command.id, command.verb, exc)
            return done
        body = _confirmation(conn, command, observed, left)
        with conn.transaction():
            conn.execute(_INSERT_LATCH, (command.id, wanted, observed, body, ACTOR))
            raise_alert(
                conn,
                command.verb,
                f"command {command.id}",
                command.at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                body,
            )
        log.info(
            "command %d (%s) carried to the broker: suspend_trade %s",
            command.id,
            command.verb,
            observed,
        )
        done += 1
    return done


@dataclass(frozen=True)
class Confirmed:
    """An operator's liquidation confirmation that execution has not yet carried out."""

    id: int
    actor: str
    at: datetime


def pending_liquidation(conn: psycopg.Connection) -> list[Confirmed]:
    """The confirmed liquidations without a result, oldest first."""
    return [Confirmed(*row) for row in conn.execute(_LIQUIDATION_PENDING).fetchall()]


def _closed_line(closed: Closed) -> str:
    if closed.ok:
        return f"{closed.symbol}: closing, order {closed.order_id or 'with no id'}"
    return f"{closed.symbol}: FAILED, HTTP {closed.status}: {closed.message}"


def _liquidation_text(confirmed: Confirmed, closed: Sequence[Closed], left: Sequence[str]) -> str:
    """The message the operator gets once execution has had the broker's answer."""
    ok = sum(1 for c in closed if c.ok)
    failed = len(closed) - ok
    head = f"LIQUIDATION by {confirmed.actor} at {_at(confirmed.at)}"
    if not closed:
        summary = "the broker held no positions to close"
    elif failed:
        summary = f"{ok} of {len(closed)} positions closing, {failed} FAILED and still held"
    else:
        summary = f"all {len(closed)} positions closing"
    lines = [head, summary, "open orders cancelled by the broker first"]
    lines += [_closed_line(c) for c in closed]
    lines += list(left)
    lines.append("halt cause liquidation: stays open until /resume")
    return "\n".join(lines)


def apply_liquidation(
    conn: psycopg.Connection, broker: Broker, calendar: nyse.NyseCalendar, now: datetime
) -> int:
    """Carry each confirmed liquidation to the broker and record its answer symbol by symbol.
    The answer is recorded the moment the broker gives it, so a failure after that is retried
    from the record and never sends a second close-all. A broker error, or a basket that
    moves under the cancel, raises a `liquidation_failed` alert once per confirmation and
    stops, leaving it for the next cycle. Returns how many were carried."""
    done = 0
    for confirmed in pending_liquidation(conn):
        subject = f"liquidation {confirmed.id}"
        period = confirmed.at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        closed = answer(conn, confirmed.id)
        left: list[str] = []
        if not closed and LIQUIDATION not in open_halts(conn):
            body = (
                f"LIQUIDATION by {confirmed.actor} at {_at(confirmed.at)}: not carried out, "
                "the liquidation cause was cleared before execution reached it\n"
                "nothing was sent to the broker"
            )
            with conn.transaction():
                conn.execute(_INSERT_RESULT, (confirmed.id, Jsonb([]), body, ACTOR))
                raise_alert(conn, LIQUIDATION, subject, period, body)
            log.warning("liquidation %d: cause cleared, not carried out", confirmed.id)
            done += 1
            continue
        if not closed:
            try:
                left = cancel_in_flight(conn, broker, calendar, now)
                closed = broker.close_all()
            except (AlpacaError, ExecutionError) as exc:
                raise_alert(
                    conn,
                    LIQUIDATION_FAILED,
                    subject,
                    period,
                    f"LIQUIDATION by {confirmed.actor} at {_at(confirmed.at)} could not reach "
                    f"the broker: {exc}\nhalt cause liquidation: open. Execution tries again "
                    "each cycle; /resume clears the cause and stops it.",
                )
                log.error("liquidation %d: %s", confirmed.id, exc)
                return done
            record_answer(conn, confirmed.id, closed)
        halted = conn.execute(_HALTED_SINCE, (SURFACE, confirmed.at)).fetchall()
        left = [f"basket {ref} halted at {_at(at)}: {why}" for ref, at, why in halted] + left
        body = _liquidation_text(confirmed, closed, left)
        symbols = [
            {
                "symbol": c.symbol,
                "status": c.status,
                "order_id": c.order_id,
                "message": c.message,
            }
            for c in closed
        ]
        with conn.transaction():
            conn.execute(_INSERT_RESULT, (confirmed.id, Jsonb(symbols), body, ACTOR))
            raise_alert(conn, LIQUIDATION, subject, period, body)
        log.warning(
            "liquidation %d carried to the broker: %d of %d closing",
            confirmed.id,
            sum(1 for c in closed if c.ok),
            len(closed),
        )
        done += 1
    return done


def record_answer(conn: psycopg.Connection, confirm_id: int, closed: Sequence[Closed]) -> None:
    """Record the broker's close-all answer for a confirmation, one row per symbol, in one
    transaction. Its closing orders are what `ingest_fills` later records fills against."""
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            _INSERT_ANSWER,
            [(confirm_id, c.symbol, c.status, c.order_id, c.message, ACTOR) for c in closed],
        )


def answer(conn: psycopg.Connection, confirm_id: int) -> list[Closed]:
    """The close-all answer recorded for a confirmation, in the broker's order; empty if the
    broker has not answered it, or answered that there was nothing to close."""
    return [Closed(*row) for row in conn.execute(_ANSWER, (confirm_id,)).fetchall()]
