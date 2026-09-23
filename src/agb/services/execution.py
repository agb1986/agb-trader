"""The execution service: the only process that holds the broker's keys.

Each cycle it takes the approved live baskets whose trade session is in its submission
window and sends their orders (`agb.execution.submit`), reads back the orders of submitted
baskets once the session has closed (`reconcile`), and after the close of each session
snapshots the broker's positions and account state once (`snapshot`). Between those it
observes the account's flags every few minutes, so the bot can compare the broker's
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

from agb import execution, nyse, schedule
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
    ) -> None:
        self.broker = broker
        self.clock = clock
        self.settle = settle
        self.observe_every = observe_every
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
        if self._observed_at is None or now - self._observed_at >= self.observe_every:
            execution.record_state(conn, self.broker)
            self._observed_at = now
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
            execution.submit(conn, self.broker, basket, now)
        for basket in execution.waiting(conn, "submitted"):
            if now < calendar.close_at(basket.session_date) + self.settle:
                continue
            execution.reconcile(conn, self.broker, basket, now)
        session = self.closed_session(calendar, now)
        if session is not None:
            execution.snapshot(conn, self.broker, session, calendar)

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
