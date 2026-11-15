"""Crash recovery: the halt an expired submission lease opens, and the operator's reconcile.

Execution is killed at the ticket's three points — before an order row is written, after the
write but before the broker call, and after the call but before the acknowledgement is
recorded. In each case the lease it held runs out, no later cycle reclaims the job, and
`execution_lease_expired` opens. The reconcile then walks the basket against the broker and
classifies every order, appending what it learns and updating nothing. Nothing submits while
the cause is open, and only the operator's `/resume` closes it, which is also what gives up
the dead process's lease.

The broker is `alpaca_fake.FakeBroker`, the baskets come from the execution tests, and the
chat comes from the pause/resume tests.
"""

import logging
from datetime import timedelta

import pytest
from market_fake import PERIOD
from test_execution import (
    CLOSE_AT,
    MID,
    NOW,
    Clock,
    approved,
    broker,
    events,
    halts,
    order_id,
    rows,
    state,
)
from test_pause_resume import Operator, open_causes

from agb import execution, recovery
from agb.alpaca import AlpacaError
from agb.services.execution import Execution

# The lease has already run out when it is taken, so the cycle after the crash finds an
# expired lease without the test waiting a quarter of an hour for one.
EXPIRED = timedelta(seconds=-1)
# Who the operator is when they reconcile, as the CLI names them.
OPERATOR = "cli:agb"
LOST = "POST /v2/orders: connection reset"


@pytest.fixture
def operator(db):
    """The configured chat, for the `/resume` that is the only way this cause closes."""
    return Operator(db)


def crash(db, fake, clock=None):
    """One cycle that dies mid-submission, holding a lease that has already run out."""
    Execution(fake, clock=clock or Clock(NOW), lease=EXPIRED)(db.connect("execution"))


def cycle(db, fake, clock=None):
    """A healthy cycle, with the service's own lease."""
    Execution(fake, clock=clock or Clock(NOW))(db.connect("execution"))


def job(db, period=PERIOD):
    return rows(
        db,
        "SELECT state, claimed_by, lease_expired FROM agb.job_state"
        " WHERE kind = 'submission' AND period = %s",
        period,
    )[0]


def job_events(db, period=PERIOD):
    return rows(
        db,
        "SELECT e.event, e.actor FROM agb.job_event e JOIN agb.job j ON j.id = e.job_id"
        " WHERE j.kind = 'submission' AND j.period = %s ORDER BY e.id",
        period,
    )


def planned(db, basket_id):
    """Every planned order by client_order_id, for a crash hook that stands in for the
    broker's own acceptance."""
    return {
        client_order_id: (symbol, side, qty)
        for client_order_id, symbol, side, qty in rows(
            db,
            "SELECT client_order_id, symbol, side, qty FROM agb.basket_order"
            " WHERE basket_id = %s ORDER BY id",
            basket_id,
        )
    }


def outcomes(report):
    return {w.client_order_id: w.outcome for w in report.walked}


def no_quotes(symbols):
    """Crash point 1: the quote the first order needs never comes, so no row is written."""
    raise AlpacaError("GET /v2/stocks/quotes/latest: connection reset")


def die_before_the_call(fake, client_order_id):
    """Crash point 2: the row is committed and the call never reaches the broker."""
    raise AlpacaError(LOST)


def accept_then_die(orders):
    """Crash point 3: the broker took the order and the answer was never recorded."""

    def hook(fake, client_order_id):
        symbol, side, qty = orders[client_order_id]
        fake.accept(symbol, qty, side, client_order_id)
        raise AlpacaError(LOST)

    return hook


def expect_halted(db, target):
    """The cause is open against the basket, naming the reconcile the operator must run."""
    [(cause, basket_id, actor, reason)] = halts(db)
    assert (cause, basket_id, actor) == (execution.LEASE_EXPIRED, target.id, "execution")
    assert f"the submission lease for {PERIOD} expired at " in reason
    assert f"reconcile basket {target.reference} by client_order_id" in reason
    # The job keeps the dead process's expired lease: it is never reclaimed.
    assert job(db) == ("lease_expired", "execution", True)
    assert job_events(db) == [("claimed", "execution"), ("lease_expired", "execution")]


# -- the three crash points ----------------------------------------------------------------


def test_a_crash_before_the_order_row_halts_with_every_order_unwritten(db, caplog):
    """Acceptance criteria 1 and 2, first crash point: the quote fails, so nothing is
    written, and the reconcile finds the broker was never asked for anything."""
    target = approved(db)
    fake = broker()
    fake.latest_quotes = no_quotes

    with caplog.at_level(logging.ERROR):
        crash(db, fake)
    assert (
        f"{PERIOD} submission_failed: AlpacaError: GET /v2/stocks/quotes/latest: connection reset"
        in caplog.text
    )
    # The lease had run out, so it is not released: it stays expired for the halt below.
    assert f"submission job for {PERIOD} not released" in caplog.text
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(0,)]

    # The next cycle, with a broker that answers, opens the cause rather than trying again.
    well = broker()
    cycle(db, well)
    expect_halted(db, target)
    assert well.submits() == []
    assert state(db, target.id)[0] == "approved"

    report = recovery.recover(db.connect("execution"), well, target.reference, OPERATOR)
    assert set(outcomes(report).values()) == {recovery.UNWRITTEN}
    assert report.detail == "20 orders: 20 never written"
    # Nothing was written, so there was nothing to ask the broker about.
    assert [c for c in well.calls if c[0] == "order_by_client_id"] == []
    assert rows(db, "SELECT count(*) FROM agb.order_event") == [(0,)]
    assert rows(db, "SELECT basket_id, orders, detail, actor FROM agb.reconcile_run") == [
        (target.id, 20, report.detail, OPERATOR)
    ]


def test_a_crash_between_the_write_and_the_call_reports_the_order_as_unsent(db, caplog):
    """Acceptance criteria 1 and 2, second crash point: one row is committed and the broker
    never saw it, which the reconcile learns from the 404 under the client_order_id."""
    target = approved(db)
    fake = broker(on_submit=die_before_the_call)

    with caplog.at_level(logging.ERROR):
        crash(db, fake)
    [(cid,)] = rows(db, "SELECT client_order_id FROM agb.broker_order")
    assert f"{PERIOD} submission_failed: ExecutionError: {cid}: {LOST}" in caplog.text
    assert fake.by_client == {}
    assert events(db, order_id(db, cid)) == []

    fake.on_submit = None
    cycle(db, fake)
    expect_halted(db, target)
    assert fake.submits() == [cid]

    report = recovery.recover(db.connect("execution"), fake, target.reference, OPERATOR)
    found = outcomes(report)
    assert found.pop(cid) == execution.UNSENT
    assert set(found.values()) == {recovery.UNWRITTEN}
    assert report.detail == "20 orders: 1 written but never reached the broker, 19 never written"
    assert events(db, order_id(db, cid)) == [(execution.UNSENT, execution.UNSENT, None)]
    # Only the order that was written could be asked about.
    assert [c for c in fake.calls if c[0] == "order_by_client_id"] == [("order_by_client_id", cid)]


def test_a_crash_after_the_call_reports_the_order_as_still_open_at_the_broker(db, caplog):
    """Acceptance criteria 1 and 2, third crash point: the broker holds the order and the
    acknowledgement was never recorded, so the row has no answer until the reconcile."""
    target = approved(db)
    fake = broker()
    fake.on_submit = accept_then_die(planned(db, target.id))

    with caplog.at_level(logging.ERROR):
        crash(db, fake)
    [(cid,)] = rows(db, "SELECT client_order_id FROM agb.broker_order")
    assert fake.submits() == [cid]
    assert list(fake.by_client) == [cid]
    assert events(db, order_id(db, cid)) == []

    fake.on_submit = None
    cycle(db, fake)
    expect_halted(db, target)

    report = recovery.recover(db.connect("execution"), fake, target.reference, OPERATOR)
    found = outcomes(report)
    assert found.pop(cid) == recovery.OPEN
    assert set(found.values()) == {recovery.UNWRITTEN}
    assert report.detail == "20 orders: 1 still open at the broker, 19 never written"
    [walked] = [w for w in report.walked if w.client_order_id == cid]
    assert walked.detail == f"accepted at the broker, 0 of {walked.qty} filled (ord-0001)"
    assert events(db, order_id(db, cid)) == [(execution.RECONCILED, "accepted", "ord-0001")]


def test_the_reconcile_reports_the_fill_a_crashed_submission_left_at_the_broker(db):
    """Acceptance criterion 2: an order the crash left at the broker filled in the closing
    auction, which is the outcome the operator most needs to know about."""
    target = approved(db)
    fake = broker()
    fake.on_submit = accept_then_die(planned(db, target.id))

    crash(db, fake)
    cycle(db, fake)
    [(cid,)] = rows(db, "SELECT client_order_id FROM agb.broker_order")
    fake.on_submit = None
    fake.close(CLOSE_AT)

    report = recovery.recover(db.connect("execution"), fake, target.reference, OPERATOR)
    [walked] = [w for w in report.walked if w.client_order_id == cid]
    assert walked.outcome == recovery.FILLED
    assert walked.detail == f"filled {walked.qty} at {MID} (ord-0001)"
    assert report.detail == "20 orders: 1 filled, 19 never written"
    assert events(db, order_id(db, cid)) == [(execution.RECONCILED, "filled", "ord-0001")]
    # The fill is reported, not acted on: the basket stays where the crash left it.
    assert state(db, target.id)[0] == "approved"
    assert rows(db, "SELECT count(*) FROM agb.fill") == [(0,)]


def test_the_reconcile_only_appends_and_updates_no_history_row(db):
    """Acceptance criterion 3: run twice, and the second run appends what it found again
    rather than correcting the first; the order rows themselves are untouched."""
    target = approved(db)
    fake = broker()
    fake.on_submit = accept_then_die(planned(db, target.id))
    crash(db, fake)
    cycle(db, fake)
    fake.on_submit = None
    [(cid,)] = rows(db, "SELECT client_order_id FROM agb.broker_order")
    before = rows(db, "SELECT * FROM agb.broker_order ORDER BY id")

    conn = db.connect("execution")
    recovery.recover(conn, fake, target.reference, OPERATOR)
    fake.close(CLOSE_AT)
    recovery.recover(conn, fake, target.reference, "cli:someone-else")

    assert rows(db, "SELECT * FROM agb.broker_order ORDER BY id") == before
    assert events(db, order_id(db, cid)) == [
        (execution.RECONCILED, "accepted", "ord-0001"),
        (execution.RECONCILED, "filled", "ord-0001"),
    ]
    assert rows(db, "SELECT actor, orders, detail FROM agb.reconcile_run ORDER BY id") == [
        (OPERATOR, 20, "20 orders: 1 still open at the broker, 19 never written"),
        ("cli:someone-else", 20, "20 orders: 1 filled, 19 never written"),
    ]


# -- what the open cause stops -------------------------------------------------------------


def test_nothing_submits_while_the_cause_is_open(db, caplog):
    """Acceptance criterion 4, first half: cycle after cycle, the basket stays approved, the
    broker is left alone, and the cause is opened once and not again."""
    target = approved(db)
    fake = broker(on_submit=die_before_the_call)
    crash(db, fake)
    fake.on_submit = None

    with caplog.at_level(logging.ERROR):
        for _ in range(3):
            cycle(db, fake)

    assert f"{execution.LEASE_EXPIRED} is open: no basket submits until it is" in caplog.text
    assert state(db, target.id)[0] == "approved"
    assert len(fake.submits()) == 1
    assert len(fake.orders) == 0
    expect_halted(db, target)
    assert rows(db, "SELECT action, count(*) FROM agb.halt_event GROUP BY 1") == [("set", 1)]


def test_only_the_operators_clear_closes_the_cause_and_releases_the_lease(db, operator):
    """Acceptance criterion 4, second half: nothing closes the cause by itself; the
    operator's `/resume` does, and that clear is what gives up the dead lease, after which
    the period can be worked again and nothing re-opens the cause."""
    target = approved(db)
    fake = broker()
    fake.on_submit = accept_then_die(planned(db, target.id))
    crash(db, fake)
    cycle(db, fake)
    fake.on_submit = None
    [(crashed,)] = rows(db, "SELECT client_order_id FROM agb.broker_order")

    # The operator reconciles first: the order the crash left at the broker is on the record.
    recovery.recover(db.connect("execution"), fake, target.reference, OPERATOR)
    assert open_causes(db) == [(execution.LEASE_EXPIRED, "execution")]

    assert "cleared: execution_lease_expired" in operator.says("/resume")
    assert open_causes(db) == []

    # The same cycle gives up the lease and works the period, skipping the order the broker
    # already has: an answered order is never sent twice.
    cycle(db, fake)
    assert job_events(db) == [
        ("claimed", "execution"),
        ("lease_expired", "execution"),
        ("released", "execution"),
        ("claimed", "execution"),
        ("completed", "execution"),
    ]
    assert state(db, target.id)[0] == "submitted"
    assert len(fake.orders) == 20
    assert fake.submits().count(crashed) == 1
    assert sorted(fake.by_client) == sorted(fake.submits())

    # And the cause stays closed: the expiry it was opened for has been dealt with.
    cycle(db, fake)
    assert open_causes(db) == []
    assert rows(db, "SELECT action, count(*) FROM agb.halt_event GROUP BY 1 ORDER BY 1") == [
        ("clear", 1),
        ("set", 1),
    ]
