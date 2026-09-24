"""The scheduler service. It owns the calendar: each cycle it beats its heartbeat, writes the
jobs and expected events due from today's session through the lookahead, records any lease
that has expired, and marks missed every rebalance still waiting for the human at its
session's MOC cutoff."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import psycopg

from agb import approval, jobs, schedule
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.nyse import NyseCalendar, trading_date

log = logging.getLogger(__name__)

# Planning is idempotent, so replanning often is safe; this only bounds the work per cycle.
REPLAN_EVERY = timedelta(minutes=5)


class Scheduler:
    """One scheduler cycle, run by `serve`. The clock is injectable for tests."""

    def __init__(
        self,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        *,
        approval_required: bool = True,
        replan_every: timedelta = REPLAN_EVERY,
    ) -> None:
        self.clock = clock
        self.approval_required = approval_required
        self.replan_every = replan_every
        self._calendar: NyseCalendar | None = None
        self._planned_at: datetime | None = None

    def calendar(self, now: datetime) -> NyseCalendar:
        start, end = schedule.calendar_window(trading_date(now))
        if self._calendar is None or not self._calendar.covers(start, end):
            self._calendar = NyseCalendar(start, end + timedelta(days=90))
        return self._calendar

    def __call__(self, conn: psycopg.Connection) -> None:
        now = self.clock()
        if self._planned_at is None or now - self._planned_at >= self.replan_every:
            planned = schedule.plan(
                self.calendar(now), trading_date(now), approval_required=self.approval_required
            )
            created = schedule.write(conn, planned)
            if created:
                log.info("scheduled %d new jobs", created)
            self._planned_at = now
        expired = jobs.record_expiries(conn, schedule.ACTOR)
        if expired:
            log.warning("recorded %d expired job leases", expired)
        for reference in approval.sweep(conn, now, self.calendar(now)):
            log.warning("rebalance %s: no answer by the cutoff, marked missed", reference)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    serve("scheduler", interval_from_env(), stop_on_signals(), Scheduler())


if __name__ == "__main__":
    main()
