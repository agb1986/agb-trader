"""Crash recovery: the halt an expired submission lease opens, and the operator's reconcile.

Execution holds the period's `submission` job while it sends a basket's orders. If the
process dies mid-submission, the lease is never reclaimed and no one runs the submission
again: what reached the broker is not something to guess at. Instead the expiry opens the
`execution_lease_expired` halt cause (`halt_expired_submissions`), and nothing submits until
a human has reconciled and cleared it. The clear is also what frees the lease: until then the
job stays held by the process that died, so no cycle in between can pick the submission up.

`recover` is that reconcile, reached as `agb reconcile REFERENCE`. It walks every planned
order of the basket and asks the broker what it knows by `client_order_id`, reporting each
order as sent and filled, sent and still open, written but never reached the broker, or never
written at all. What it learns is appended as order events and one `reconcile_run` row; no
history row is updated, no order is sent, nothing is cancelled. The operator then decides
what to do; the system does not.

`execution.reconcile` is a different thing with a similar name: the post-close read-back of a
submitted basket's fills on a day nothing went wrong.
"""

import logging
from dataclasses import dataclass
from datetime import UTC
from decimal import Decimal

import psycopg

from agb import execution, jobs, schedule
from agb.alpaca import AlpacaError, Broker
from agb.execution import ACTOR, DONE, LEASE_EXPIRED, RECONCILED, UNSENT

log = logging.getLogger(__name__)

# What a walked order turned out to be. The first three are the ticket's three outcomes;
# `closed` is the broker holding an order it will not fill, and `unwritten` is a crash before
# the row was written, which is the one case where nothing was ever sent.
FILLED = "filled"
OPEN = "open"
CLOSED = "closed"
UNWRITTEN = "unwritten"

# How each outcome reads in the run's one-line summary, in the order they are reported.
PHRASES = (
    (FILLED, "filled"),
    (OPEN, "still open at the broker"),
    (CLOSED, "closed at the broker without a fill"),
    (UNSENT, "written but never reached the broker"),
    (UNWRITTEN, "never written"),
)


class RecoveryError(Exception):
    """The reconcile cannot run: leaving the operator where they were is the safe answer."""


_EXPIRED = """
    SELECT j.id, j.period, j.lease_until, j.claimed_by
    FROM agb.job j
    WHERE j.kind = %s AND j.claimed_by IS NOT NULL AND j.lease_until < now()
      AND NOT EXISTS (
          SELECT 1 FROM agb.job_event e
          WHERE e.job_id = j.id AND e.event IN ('completed', 'failed'))
    ORDER BY j.not_before, j.id
"""
_LIVE_BASKET = """
    SELECT id, reference FROM agb.basket_state
    WHERE period = %s AND kind = 'live' ORDER BY id DESC LIMIT 1
"""
_BASKET_BY_REFERENCE = """
    SELECT id, reference, state FROM agb.basket_state
    WHERE reference = %s AND kind = 'live' ORDER BY id DESC LIMIT 1
"""
_OPEN_HALT = """
    INSERT INTO agb.halt_event (cause, action, basket_id, actor, reason)
    VALUES (%s, 'set', %s, %s, %s)
"""
# Whether this expiry has already been halted. The scheduler's sweep records every expired
# lease as a job event, so the event cannot say; a `set` of the cause after the lease ran out
# can only be about this expiry, because nothing else sets it and an earlier one was cleared
# before the lease that has now expired was ever taken.
_HALTED = """
    SELECT EXISTS (
        SELECT 1 FROM agb.halt_event
        WHERE cause = %s AND action = 'set' AND at >= %s)
"""
# The walk needs only the row and whether one was ever written; what the broker says is
# asked of the broker, not read back from the view.
_WALK = """
    SELECT client_order_id, symbol, side, qty, order_id
    FROM agb.reconcile_state WHERE basket_id = %s ORDER BY basket_order_id
"""
_EVENT = """
    INSERT INTO agb.order_event (order_id, kind, broker_order_id, status, filled_qty,
        filled_avg_price, detail, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
_RUN = """
    INSERT INTO agb.reconcile_run (basket_id, orders, detail, actor)
    VALUES (%s, %s, %s, %s) RETURNING id
"""


@dataclass(frozen=True)
class Walked:
    """One planned order and what the broker turned out to know about it."""

    client_order_id: str
    symbol: str
    side: str
    qty: Decimal
    outcome: str
    detail: str


@dataclass(frozen=True)
class Report:
    basket_id: int
    reference: str
    walked: tuple[Walked, ...]
    detail: str


def summarise(walked: tuple[Walked, ...]) -> str:
    """The one line the run records and the operator reads: how many of each outcome."""
    counts = [
        (phrase, sum(1 for w in walked if w.outcome == outcome)) for outcome, phrase in PHRASES
    ]
    found = [f"{n} {phrase}" for phrase, n in counts if n]
    return f"{len(walked)} orders: " + (", ".join(found) if found else "nothing found")


def halt_expired_submissions(conn: psycopg.Connection, *, actor: str = ACTOR) -> str | None:
    """Carry an expired submission lease through its halt. Returns the reason it just opened.

    First sight of the expiry records it as a job event and opens `execution_lease_expired`.
    The lease is left exactly as it is: the job is not reclaimed and `claimed_by` is not
    cleared, so what happened stays visible in `agb.job_state` and no second attempt can take
    the job. Nothing submits while the cause is open, so there is only ever one expired
    submission lease to find; the earliest is taken if that is ever untrue.

    Once the operator has reconciled and cleared the cause by hand, the lease is given up:
    the clear is the human saying what happens next, and the period can be worked again. That
    is the only way an expired submission lease is ever let go of; nothing here clears the
    cause, and no cycle in between touches the job.
    """
    expired = conn.execute(_EXPIRED, (schedule.SUBMISSION,)).fetchall()
    if not expired:
        return None
    job_id, period, lease_until, holder = expired[0]
    if conn.execute(_HALTED, (LEASE_EXPIRED, lease_until)).fetchone()[0]:
        if LEASE_EXPIRED in execution.open_halts(conn):
            return None
        given_up = (
            f"the submission lease for {period} expired at "
            f"{lease_until.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}; released after a human "
            f"reconciled and cleared {LEASE_EXPIRED}"
        )
        if jobs.release_expired(conn, job_id, actor, given_up):
            log.warning("job %s released: %s", job_id, given_up)
        return None
    found = conn.execute(_LIVE_BASKET, (period,)).fetchone()
    basket_id, reference = found if found is not None else (None, period)
    reason = (
        f"the submission lease for {period} expired at "
        f"{lease_until.astimezone(UTC):%Y-%m-%dT%H:%M:%SZ} and is never retried; "
        f"reconcile basket {reference} by client_order_id, then clear this cause"
    )
    with conn.transaction():
        jobs.record_expiry(conn, job_id, lease_until, actor)
        conn.execute(_OPEN_HALT, (LEASE_EXPIRED, basket_id, actor, reason))
    log.error("%s held the lease; %s", holder, reason)
    return reason


def recover(conn: psycopg.Connection, broker: Broker, reference: str, actor: str = ACTOR) -> Report:
    """Walk the basket's orders against the broker and append what each one turned out to be.

    `conn` is in autocommit mode: each order's event is committed as it is learned, so an
    interrupted reconcile keeps what it found and can simply be run again.
    """
    found = conn.execute(_BASKET_BY_REFERENCE, (reference,)).fetchone()
    if found is None:
        raise RecoveryError(f"no live basket {reference}")
    basket_id, reference, state = found
    rows = conn.execute(_WALK, (basket_id,)).fetchall()
    if not rows:
        raise RecoveryError(f"basket {reference} is {state} and has no orders")
    walked: list[Walked] = []
    for client_order_id, symbol, side, qty, order_id in rows:
        if order_id is None:
            # Nothing was written, so there is nothing to append an event to and nothing can
            # have been sent: the client_order_id was never used.
            walked.append(
                Walked(client_order_id, symbol, side, qty, UNWRITTEN, "no order row was written")
            )
            continue
        try:
            order = broker.order_by_client_id(client_order_id)
        except AlpacaError as exc:
            if not exc.not_found:
                raise RecoveryError(f"{client_order_id}: {exc}") from exc
            conn.execute(
                _EVENT,
                (order_id, UNSENT, None, UNSENT, Decimal(0), None, str(exc), actor),
            )
            walked.append(
                Walked(
                    client_order_id,
                    symbol,
                    side,
                    qty,
                    UNSENT,
                    "the broker has never seen this client_order_id",
                )
            )
            continue
        conn.execute(
            _EVENT,
            (
                order_id,
                RECONCILED,
                order.id,
                order.status,
                order.filled_qty,
                order.filled_avg_price,
                f"reconciled by {actor}",
                actor,
            ),
        )
        if order.status == "filled":
            classified, detail = FILLED, f"filled {order.filled_qty} at {order.filled_avg_price}"
        elif order.status in DONE:
            classified, detail = CLOSED, f"{order.status} at the broker"
        else:
            classified = OPEN
            detail = f"{order.status} at the broker, {order.filled_qty} of {qty} filled"
        walked.append(
            Walked(client_order_id, symbol, side, qty, classified, f"{detail} ({order.id})")
        )
    detail = summarise(tuple(walked))
    conn.execute(_RUN, (basket_id, len(walked), detail, actor))
    log.info("basket %s reconciled: %s", reference, detail)
    return Report(basket_id, reference, tuple(walked), detail)
