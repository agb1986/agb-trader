"""The execution service: the only process that holds the broker's keys.

Each cycle it first carries any `/pause` or `/resume` waiting in the database to the broker
(`agb.execution.apply_latch`): the pause cancels what is in flight and sets `suspend_trade`,
the resume unsets it. A confirmed `/liquidate` follows: the broker's close-all, its answer
recorded symbol by symbol (`apply_liquidation`). Then it takes the approved live baskets
whose trade session is in its submission window and sends their orders
(`agb.execution.submit`), reads back the orders of submitted baskets once the session has
closed (`reconcile`), raising one alert for a basket still waiting on the broker an hour
after the close (`agb.execution.RECONCILE_DEADLINE`), and after the close of each session
snapshots the broker's positions and account state once (`snapshot`). Between those it
observes the account's flags every few minutes, so the bot can compare the broker's
`suspend_trade` with the halt latch.

Without keys it beats and does nothing else, so the stack starts whole before the operator
has an Alpaca account. With them it will not start, and stops, unless ALPACA_BASE_URL is the
trading endpoint of the mode in force (`agb.alpaca.TRADING_APIS`): the paper host in paper
mode, the live host in live mode. The check runs before the first heartbeat and again every
cycle, so a mode that changes under a running service ends it, and Compose restarts it into
the same refusal until the URL and the mode agree.

Errors reaching the broker end the cycle, not the service: the next cycle starts from what
the database says, which is why every row is written before the call it records. Each
basket's submission, each reconcile and the snapshot are their own step: one that fails is
logged and alerted on at once, and the steps after it still run. An error from the database
is the exception, and ends the process as it does in every service.
"""

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta

import psycopg

from agb import execution, jobs, nyse, recovery, schedule
from agb.alpaca import Alpaca, AlpacaError, Broker, check_mode
from agb.execution import ROLE, ExecutionError
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.nyse import NyseCalendar, trading_date

log = logging.getLogger(__name__)

_MODE = "SELECT mode FROM agb.current_mode"

# How often the account's flags are observed when nothing else is happening.
OBSERVE_EVERY = timedelta(minutes=5)
# How long an observation waits after one that failed, doubling to the cap. Without this a
# failed observation leaves the five-minute reading overdue, so the next cycle tries again a
# cycle later and every cycle after that: a broker that refuses this one read turns the
# five-minute observation into a thirty-second retry loop, and the rest of the cycle -- the
# submits, the reconciles, the snapshot -- never runs, because the failure ends the cycle at
# the same point every time.
OBSERVE_BACKOFF = timedelta(seconds=30)
OBSERVE_BACKOFF_CAP = timedelta(minutes=10)
# How many cycles in a row must end on an error before the operator is told. At the default
# thirty-second cycle, ten of them is five minutes with no reading of the account, which is
# the staleness window the health checks use. Below that it is noise: a single failed cycle is
# an ordinary event, and the next one carries on from what the database says.
FAILURES_BEFORE_ALERT = 10
# A step of the cycle that failed on its own, alerted on the cycle it fails: one basket's
# submission, one basket's reconcile, or the snapshot of one session. The alert's subject and
# period name the basket or the session, so a step that fails every cycle is raised once.
SUBMISSION_FAILED = "submission_failed"
RECONCILE_FAILED = "reconcile_failed"
SNAPSHOT_FAILED = "snapshot_failed"
# An approved basket whose submission window closed before it went in.
WINDOW_MISSED = "submission_window_missed"


def _stamp(at: datetime) -> str:
    """An instant as the alert table's `period`: one alert per run of failures, keyed by when
    the run began."""
    return at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class Execution:
    """One execution cycle, run by `serve`. The broker, the clock and the delays are
    injectable for tests."""

    def __init__(
        self,
        broker: Broker | None,
        *,
        base_url: str | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        settle: timedelta = execution.SETTLE,
        observe_every: timedelta = OBSERVE_EVERY,
        observe_backoff: timedelta = OBSERVE_BACKOFF,
        observe_backoff_cap: timedelta = OBSERVE_BACKOFF_CAP,
        failures_before_alert: int = FAILURES_BEFORE_ALERT,
        lease: timedelta = execution.SUBMISSION_LEASE,
    ) -> None:
        self.broker = broker
        # The broker's trading endpoint, checked against the trading mode; None for a broker
        # that is not Alpaca's, such as a test's fake.
        self.base_url = base_url
        self.clock = clock
        self.settle = settle
        self.observe_every = observe_every
        self.observe_backoff = observe_backoff
        self.observe_backoff_cap = observe_backoff_cap
        self.failures_before_alert = failures_before_alert
        self.lease = lease
        self._calendar: NyseCalendar | None = None
        self._observed_at: datetime | None = None
        self._warned = False
        # The observation's backoff: how long the last failed one asks us to wait, and when
        # it was tried. Both None whenever the last observation succeeded.
        self._observe_waiting: timedelta | None = None
        self._observe_tried_at: datetime | None = None
        # The run of failed cycles: how many, when it started, and whether it has been
        # alerted on. All reset by a cycle that finishes.
        self._failures = 0
        self._failing_since: datetime | None = None
        self._alerted = False
        # The baskets already told about as having missed their window, so the warning is
        # logged once per basket rather than once per cycle for as long as it stays approved.
        self._missed: set[int] = set()

    @classmethod
    def from_env(cls) -> "Execution":
        if not Alpaca.configured():
            return cls(None)
        broker = Alpaca.from_env()
        return cls(broker, base_url=broker.trading)

    def check_mode(self, conn: psycopg.Connection) -> None:
        """Refuse, with a `BaseUrlError` that ends the process, a trading endpoint that is not
        the one for the trading mode in force."""
        if self.base_url is None:
            return
        row = conn.execute(_MODE).fetchone()
        check_mode(self.base_url, row[0] if row else None)

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
        self.check_mode(conn)
        try:
            self.cycle(conn)
        except (AlpacaError, ExecutionError, OSError) as exc:
            log.error("cycle stopped: %s", exc)
            self.failed(conn, exc)
        else:
            self.recovered(conn)

    def failed(self, conn: psycopg.Connection, exc: Exception) -> None:
        """Count this failed cycle, and alert once when the run of them crosses the
        threshold. One alert per run: the run's own start is the alert's period, so the next
        outage raises its own and this one is never raised twice."""
        now = self.clock()
        if self._failing_since is None:
            self._failing_since = now
        self._failures += 1
        if self._alerted or self._failures < self.failures_before_alert:
            return
        execution.raise_alert(
            conn,
            execution.CYCLE_FAILING,
            execution.ROLE,
            _stamp(self._failing_since),
            f"{self._failures} execution cycles in a row have ended on an error since "
            f"{_stamp(self._failing_since)}. The account has not been read in that time, so "
            f"no basket goes in and nothing is reconciled until it recovers.\n"
            f"the last error: {exc}",
        )
        self._alerted = True
        log.error("raised %s after %d failed cycles", execution.CYCLE_FAILING, self._failures)

    def recovered(self, conn: psycopg.Connection) -> None:
        """A cycle finished. Clear the run, and say so if the operator was told about it."""
        if self._alerted and self._failing_since is not None:
            execution.raise_alert(
                conn,
                execution.CYCLE_RECOVERED,
                execution.ROLE,
                _stamp(self._failing_since),
                f"execution cycles are completing again after {self._failures} failed in a "
                f"row from {_stamp(self._failing_since)}.",
            )
            log.info("cycles recovered after %d failures", self._failures)
        self._failures = 0
        self._failing_since = None
        self._alerted = False

    def cycle(self, conn: psycopg.Connection) -> None:
        now = self.clock()
        calendar = self.calendar(now)
        if execution.apply_latch(conn, self.broker, calendar, now):
            self.observed(now)
        if execution.apply_liquidation(conn, self.broker, calendar, now):
            execution.record_state(conn, self.broker)
            self.observed(now)
        if self.observe_due(now):
            try:
                execution.record_state(conn, self.broker)
            except (AlpacaError, OSError):
                self.observe_failed(now)
                raise
            self.observed(now)
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
            try:
                deadline = execution.reconcile_deadline(calendar, basket.session_date)
                outcome = execution.reconcile(conn, self.broker, basket, now, deadline)
                if outcome.state == "submitted":
                    log.info("basket %s: %s", basket.reference, outcome.reason)
            except psycopg.Error:
                raise
            except Exception as exc:
                self.step_failed(conn, RECONCILE_FAILED, basket.reference, basket.period, exc)
        session = self.closed_session(calendar, now)
        if session is not None:
            try:
                execution.snapshot(conn, self.broker, session, calendar)
            except psycopg.Error:
                raise
            except Exception as exc:
                self.step_failed(conn, SNAPSHOT_FAILED, ROLE, session.isoformat(), exc)

    def step_failed(
        self, conn: psycopg.Connection, kind: str, subject: str, period: str, exc: Exception
    ) -> None:
        """One step of the cycle failed and the rest carry on. Log it and alert at once: a
        failed basket is something the operator acts on before the close, not after a run
        of failed cycles. The alert is unique by (kind, subject, period), so a step that
        fails every cycle is raised once."""
        error = f"{type(exc).__name__}: {exc}"
        log.error("%s %s: %s", subject, kind, error)
        execution.raise_alert(
            conn,
            kind,
            subject,
            period,
            f"{subject}: {kind.replace('_', ' ')} at {_stamp(self.clock())}. The cycle carried "
            f"on with the other steps; this one is tried again next cycle.\nthe error: {error}",
        )

    def observe_due(self, now: datetime) -> bool:
        """Whether to read the account's flags this cycle.

        After a failure the wait is the backoff, counted from the attempt that failed, rather
        than the five minutes counted from the last reading: that reading is already overdue,
        so without the backoff every cycle would try again.
        """
        if self._observe_waiting is not None and self._observe_tried_at is not None:
            return now - self._observe_tried_at >= self._observe_waiting
        return self._observed_at is None or now - self._observed_at >= self.observe_every

    def observed(self, now: datetime) -> None:
        """The account was read, however it was read. The backoff goes with it."""
        self._observed_at = now
        self._observe_waiting = None
        self._observe_tried_at = None

    def observe_failed(self, now: datetime) -> None:
        waiting = self.observe_backoff
        if self._observe_waiting is not None:
            waiting = min(self.observe_backoff_cap, self._observe_waiting * 2)
        self._observe_waiting = waiting
        self._observe_tried_at = now
        log.warning("the account could not be observed; trying again in %s", waiting)

    def submit_due(self, conn: psycopg.Connection, calendar: NyseCalendar, now: datetime) -> None:
        """Send the orders of every approved basket whose submission window is open.

        The period's submission job is held while its basket goes in. It is completed when the
        submission returns, and released with the error on the record when it raises, so the
        next cycle may hold it again: submission is idempotent by client_order_id, so going
        again sends only what the broker has not seen. A worker that finds the job held by
        someone else, or held on a lease that has run out, leaves the basket alone: an
        expired lease is for a human, not for another attempt.

        A basket whose submission fails is alerted on and the next basket still goes in. A
        basket still approved when its window closes is warned about and alerted on once.
        """
        for basket in execution.waiting(conn, "approved"):
            start, end = execution.submit_window(calendar, basket.session_date)
            if now < start:
                continue
            if now >= end:
                self.window_missed(conn, basket, end)
                continue
            claim = jobs.hold(conn, schedule.SUBMISSION, basket.period, execution.ROLE, self.lease)
            if claim is None:
                log.error(
                    "basket %s: no submission job to hold for %s; leaving it approved",
                    basket.reference,
                    basket.period,
                )
                continue
            outcome = "interrupted before the submission returned"
            try:
                execution.submit(conn, self.broker, basket, self.clock, end)
                outcome = None
            except psycopg.Error:
                raise
            except Exception as exc:
                outcome = f"{type(exc).__name__}: {exc}"
                self.step_failed(conn, SUBMISSION_FAILED, basket.reference, basket.period, exc)
            finally:
                self.end_submission(conn, claim, outcome)

    def end_submission(self, conn: psycopg.Connection, claim: jobs.Claim, error: str | None):
        """Complete the submission job if the submission returned, or release it with the
        error that stopped it. A lease lost while releasing is logged and left alone: the
        error that brought us here is the one to report, and an expired submission lease is
        halted on by the next cycle."""
        if error is None:
            jobs.complete(conn, claim)
            return
        try:
            jobs.release(conn, claim, error)
        except jobs.LeaseLost as exc:
            log.error("submission job for %s not released: %s", claim.period, exc)

    def window_missed(self, conn: psycopg.Connection, basket: execution.Basket, end: datetime):
        """A basket still approved after its window closed: warn and alert once for it. The
        scheduler's sweep marks it missed at the session's close, under the same alert."""
        if basket.id in self._missed:
            return
        closed = end.astimezone(nyse.NEW_YORK).strftime("%Y-%m-%d %H:%M ET")
        log.warning(
            "basket %s: its submission window closed at %s; not trying it again",
            basket.reference,
            closed,
        )
        execution.raise_alert(
            conn,
            WINDOW_MISSED,
            basket.reference,
            basket.period,
            f"basket {basket.reference} was still approved when its submission window closed "
            f"at {closed}, so none of its orders went in for {basket.session_date}. It is "
            f"not tried again, and the scheduler marks it missed at the close.",
        )
        self._missed.add(basket.id)

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
    service = Execution.from_env()
    # Refuse before the first heartbeat, so a service that will not start never looks alive.
    with psycopg.connect(application_name=ROLE, autocommit=True) as conn:
        service.check_mode(conn)
    serve(ROLE, interval_from_env(), stop_on_signals(), service)


if __name__ == "__main__":
    main()
