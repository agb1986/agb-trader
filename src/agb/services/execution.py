"""The execution service: the only process that holds the broker's keys.

Each cycle it first carries any `/pause` or `/resume` waiting in the database to the broker
(`agb.execution.apply_latch`): the pause cancels what is in flight and sets `suspend_trade`,
the resume unsets it. A confirmed `/liquidate` follows: the broker's close-all, its answer
recorded symbol by symbol (`apply_liquidation`). Then it takes the approved live baskets
whose trade session is in its submission window and sends their orders
(`agb.execution.submit`), reads back the orders of submitted baskets once the session has
closed (`reconcile`), and after the close of each session snapshots the broker's positions
and account state once (`snapshot`). Between those it observes the account's flags every few
minutes, so the bot can compare the broker's
`suspend_trade` with the halt latch.

Without keys it beats and does nothing else, so the stack starts whole before the operator
has an Alpaca account. Errors reaching the broker end the cycle, not the service: the next
cycle starts from what the database says, which is why every row is written before the
call it records.
"""

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import psycopg

from agb import execution, jobs, nyse, recovery, schedule
from agb.alpaca import Alpaca, AlpacaError, Broker
from agb.execution import ROLE, ExecutionError
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.nyse import NyseCalendar, trading_date

log = logging.getLogger(__name__)

# How often the account's flags are observed when nothing else is happening.
OBSERVE_EVERY = timedelta(minutes=5)


class Execution:
    """One execution cycle, run by `serve`. The broker, the clock and the delays are
    injectable for tests."""

    def __init__(
        self,
        broker: Broker | None,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        settle: timedelta = execution.SETTLE,
        observe_every: timedelta = OBSERVE_EVERY,
        lease: timedelta = execution.SUBMISSION_LEASE,
    ) -> None:
        self.broker = broker
        self.clock = clock
        self.settle = settle
        self.observe_every = observe_every
        self.lease = lease
        self._calendar: NyseCalendar | None = None
        self._observed_at: datetime | None = None
        self._warned = False

    @classmethod
    def from_env(cls) -> "Execution":
        return cls(Alpaca.from_env() if Alpaca.configured() else None)

    def calendar(self, now: datetime) -> NyseCalendar:
        start, end = schedule.calendar_window(trading_date(now))
        if self._calendar is None or not self._calendar.covers(start, end):
            self._calendar = NyseCalendar(start, end + timedelta(days=90))
        return self._calendar

    def __call__(self, conn: psycopg.Connection) -> None:
        if self.broker is None:
            if not self._warned:
                log.warning("ALPACA_API_KEY and ALPACA_SECRET_KEY are not set: beating only")
                self._warned = True
            return
        try:
            self.cycle(conn)
        except (AlpacaError, ExecutionError, OSError) as exc:
            log.error("cycle stopped: %s", exc)

    def cycle(self, conn: psycopg.Connection) -> None:
        now = self.clock()
        calendar = self.calendar(now)
        if execution.apply_latch(conn, self.broker, calendar, now):
            self._observed_at = now
        if execution.apply_liquidation(conn, self.broker, calendar, now):
            execution.record_state(conn, self.broker)
            self._observed_at = now
        if self._observed_at is None or now - self._observed_at >= self.observe_every:
            execution.record_state(conn, self.broker)
            self._observed_at = now
        recovery.halt_expired_submissions(conn)
        if execution.LEASE_EXPIRED in execution.open_halts(conn):
            # A submission died with orders possibly at the broker. Nothing goes in until the
            # operator has reconciled by client_order_id and cleared the cause by hand. The
            # loops below this one only read and record, so they carry on.
            log.error(
                "%s is open: no basket submits until it is reconciled and cleared by hand",
                execution.LEASE_EXPIRED,
            )
        else:
            self.submit_due(conn, calendar, now)
        for basket in execution.waiting(conn, "submitted"):
            if now < calendar.close_at(basket.session_date) + self.settle:
                continue
            execution.reconcile(conn, self.broker, basket, now)
        session = self.closed_session(calendar, now)
        if session is not None:
            execution.snapshot(conn, self.broker, session, calendar)

    def submit_due(self, conn: psycopg.Connection, calendar: NyseCalendar, now: datetime) -> None:
        """Send the orders of every approved basket whose submission window is open.

        The period's submission job is held while its basket goes in, and completed when the
        submission is over however it ended. A worker that finds the job held by someone
        else, or held on a lease that has run out, leaves the basket alone: an expired lease
        is for a human, not for another attempt.
        """
        for basket in execution.waiting(conn, "approved"):
            start, end = execution.submit_window(calendar, basket.session_date)
            if now < start:
                continue
            if now >= end:
                log.warning(
                    "basket %s: its submission window closed at %s; leaving it approved",
                    basket.reference,
                    end.astimezone(nyse.NEW_YORK).strftime("%Y-%m-%d %H:%M ET"),
                )
                continue
            claim = jobs.hold(conn, schedule.SUBMISSION, basket.period, execution.ROLE, self.lease)
            if claim is None:
                log.error(
                    "basket %s: no submission job to hold for %s; leaving it approved",
                    basket.reference,
                    basket.period,
                )
                continue
            execution.submit(conn, self.broker, basket, now)
            jobs.complete(conn, claim)

    def closed_session(self, calendar: NyseCalendar, now: datetime) -> date | None:
        """The latest session whose close is at least `settle` behind `now`."""
        today = trading_date(now)
        for session in reversed(calendar.sessions(today - timedelta(days=14), today)):
            if now >= calendar.close_at(session) + self.settle:
                return session
        return None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    serve(ROLE, interval_from_env(), stop_on_signals(), Execution.from_env())


if __name__ == "__main__":
    main()
