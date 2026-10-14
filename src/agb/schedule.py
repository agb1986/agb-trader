"""What the scheduler schedules, and when, against NYSE sessions in America/New_York.

Planning is pure: `plan` turns a calendar and a date into jobs and the expected events that
go with them. `write` records a plan, and is idempotent: (kind, period) is unique on both
`job` and `expected_event`, so writing the same plan again adds nothing.

The work:

- **data_pull**, one per session D, period `YYYY-MM-DD`. Claimable from D's close plus
  PULL_DELAY, to give the vendor time to publish, and expected done by the next open.
- **rebalance**, one per month. The signal comes from the month's final session L and fills at
  the next session R's close, so R always falls in the following month. The period is R's
  month, `YYYY-MM`, which is also the basket's human reference ("the 2026-10 rebalance" trades
  on 2026-10-01 from the 2026-09-30 signal). Claimable from L's close plus PULL_DELAY.
  Expected with it:
    - a **proposal** by R's open, fulfilled when the rebalance job completes;
    - an **approval_request** shortly after, which the bot fulfils when it sends one;
    - a **submission** by R's MOC cutoff, which execution fulfils. It is also the row the
      overdue view looks ahead for: no submission within the horizon means no rebalance.
- **submission**, one per rebalance period, claimable SUBMIT_WINDOW before R's MOC cutoff.
  Execution holds it while it sends the basket's orders, so a crash mid-submission leaves an
  expired lease behind; that lease is never reclaimed, and the halt it opens is what stops
  the system from guessing what reached the broker. It carries no expected event of its own:
  the submission expected above belongs to the rebalance, and execution still fulfils it.
- **backup**, one per calendar day D, weekends and holidays included, period `YYYY-MM-DD`.
  Claimable from 03:00 New York time on D and expected done BACKUP_GRACE later. Planning
  starts from tomorrow, so the scheduler never writes a backup that is already late.
"""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

import psycopg

from agb.nyse import NEW_YORK, NyseCalendar

DATA_PULL = "data_pull"
REBALANCE = "rebalance"
PROPOSAL = "proposal"
APPROVAL_REQUEST = "approval_request"
SUBMISSION = "submission"
BACKUP = "backup"

# Orders go in during this window before the session's MOC cutoff. It is also when the
# submission job becomes claimable, and how long execution's lease on it runs: the lease
# expires as the window closes, which is the moment a crash should be visible.
SUBMIT_WINDOW = timedelta(minutes=15)

# Poll intervals and lease durations are tuned against measured runs; these are first guesses.
PULL_DELAY = timedelta(hours=2)
APPROVAL_REQUEST_GRACE = timedelta(minutes=30)
# Well clear of the evening pull, and outside the hour daylight saving skips or repeats.
BACKUP_AT = time(3, 0)
BACKUP_GRACE = timedelta(hours=2)
# Longer than the widest gap between two rebalances, so the next one is always written well
# before the overdue view's horizon (40 days by default) runs out.
LOOKAHEAD = timedelta(days=45)

ACTOR = "scheduler"


@dataclass(frozen=True)
class Expected:
    kind: str
    period: str
    due_at: datetime
    # True when the job it is planned with fulfils it by completing.
    by_job: bool


@dataclass(frozen=True)
class Planned:
    kind: str
    period: str
    session_date: date
    not_before: datetime
    expected: tuple[Expected, ...]


def calendar_window(today: date) -> tuple[date, date]:
    """The sessions `plan` may ask about for `today`: the month before, to past the lookahead."""
    return today - timedelta(days=40), today + LOOKAHEAD + timedelta(days=40)


def plan(calendar: NyseCalendar, today: date, *, approval_required: bool = True) -> list[Planned]:
    """Everything due from `today` through the lookahead, in the order it becomes claimable.

    `approval_required` is true at every ladder level that asks the human first, which
    includes L0, where live trading starts.
    """
    until = today + LOOKAHEAD
    planned: list[Planned] = []
    for session in calendar.sessions(today, until):
        period = session.isoformat()
        planned.append(
            Planned(
                DATA_PULL,
                period,
                session,
                calendar.close_at(session) + PULL_DELAY,
                (
                    Expected(
                        DATA_PULL, period, calendar.open_at(calendar.next_session(session)), True
                    ),
                ),
            )
        )

    year, month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    while True:
        signal = calendar.last_session_of_month(year, month)
        trade = calendar.next_session(signal)
        if trade > until:
            break
        if trade >= today:
            period = f"{trade:%Y-%m}"
            proposal_due = calendar.open_at(trade)
            expected = [Expected(PROPOSAL, period, proposal_due, True)]
            if approval_required:
                expected.append(
                    Expected(APPROVAL_REQUEST, period, proposal_due + APPROVAL_REQUEST_GRACE, False)
                )
            cutoff = calendar.moc_cutoff(trade)
            expected.append(Expected(SUBMISSION, period, cutoff, False))
            planned.append(
                Planned(
                    REBALANCE,
                    period,
                    trade,
                    calendar.close_at(signal) + PULL_DELAY,
                    tuple(expected),
                )
            )
            # The submission expected event above stays on the rebalance, where it has always
            # been: execution fulfils it by kind and period, not by completing this job. The
            # job is here for its lease.
            planned.append(Planned(SUBMISSION, period, trade, cutoff - SUBMIT_WINDOW, ()))
        year, month = (year, month + 1) if month < 12 else (year + 1, 1)

    day = today + timedelta(days=1)
    while day <= until:
        period = day.isoformat()
        start = datetime.combine(day, BACKUP_AT, NEW_YORK).astimezone(UTC)
        planned.append(
            Planned(
                BACKUP, period, day, start, (Expected(BACKUP, period, start + BACKUP_GRACE, True),)
            )
        )
        day += timedelta(days=1)

    planned.sort(key=lambda p: (p.not_before, p.kind))
    return planned


_JOB = """
    INSERT INTO agb.job (kind, period, session_date, not_before, created_by)
    SELECT %(kind)s, %(period)s, %(session_date)s, %(not_before)s, %(actor)s
    WHERE NOT EXISTS (SELECT 1 FROM agb.job WHERE kind = %(kind)s AND period = %(period)s)
    ON CONFLICT (kind, period) DO NOTHING
    RETURNING id
"""

_EXPECTED = """
    INSERT INTO agb.expected_event (kind, period, due_at, job_id, created_by)
    SELECT %(kind)s, %(period)s, %(due_at)s, %(job_id)s, %(actor)s
    WHERE NOT EXISTS (
        SELECT 1 FROM agb.expected_event WHERE kind = %(kind)s AND period = %(period)s)
    ON CONFLICT (kind, period) DO NOTHING
"""


def write(conn: psycopg.Connection, planned: list[Planned], actor: str = ACTOR) -> int:
    """Record each planned job and its expected events. Returns how many jobs were new.

    The existence check before each insert keeps a steady state from spending identity
    values; the unique constraint is still what makes a race harmless.
    """
    created = 0
    for p in planned:
        with conn.transaction():
            params = {
                "kind": p.kind,
                "period": p.period,
                "session_date": p.session_date,
                "not_before": p.not_before,
                "actor": actor,
            }
            row = conn.execute(_JOB, params).fetchone()
            if row is not None:
                created += 1
                conn.execute(
                    "INSERT INTO agb.job_event (job_id, event, actor) VALUES (%s, 'created', %s)",
                    (row[0], actor),
                )
                job_id = row[0]
            else:
                job_id = conn.execute(
                    "SELECT id FROM agb.job WHERE kind = %s AND period = %s", (p.kind, p.period)
                ).fetchone()[0]
            for e in p.expected:
                conn.execute(
                    _EXPECTED,
                    {
                        "kind": e.kind,
                        "period": e.period,
                        "due_at": e.due_at,
                        "job_id": job_id if e.by_job else None,
                        "actor": actor,
                    },
                )
    return created
