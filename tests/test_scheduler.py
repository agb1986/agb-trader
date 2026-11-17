"""The scheduler service's cycle: how often it replans, when it reuses its calendar, and what it
logs. What the plan contains is covered by test_schedule, and the approval countdown it runs by
test_approval_loop."""

import logging
from datetime import UTC, datetime, timedelta

import pytest

from agb import jobs, schedule
from agb.services import scheduler as service
from agb.services.scheduler import REPLAN_EVERY, Scheduler

NOW = datetime(2026, 10, 1, 15, 0, tzinfo=UTC)
MINUTE = timedelta(minutes=1)


class Clock:
    def __init__(self, now=NOW):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, by):
        self.now += by


@pytest.fixture
def plans(monkeypatch):
    """Every call to `schedule.plan`, as the days it was asked to plan from."""
    asked = []
    real = schedule.plan

    def spy(calendar, today, **kwargs):
        asked.append((today, kwargs))
        return real(calendar, today, **kwargs)

    monkeypatch.setattr(service.schedule, "plan", spy)
    return asked


def jobs_written(db):
    return db.connect("cli").execute("SELECT count(*) FROM agb.job").fetchone()[0]


def test_the_default_clock_is_the_real_one_and_replanning_is_every_five_minutes():
    scheduler = Scheduler()
    assert abs(scheduler.clock() - datetime.now(UTC)) < timedelta(seconds=5)
    assert scheduler.replan_every == REPLAN_EVERY == timedelta(minutes=5)


def test_the_first_cycle_plans_and_writes_the_jobs(db, plans):
    clock = Clock()
    Scheduler(clock)(db.connect("scheduler"))
    assert len(plans) == 1
    assert jobs_written(db) > 0


def test_a_cycle_inside_the_interval_does_not_plan_again(db, plans):
    clock = Clock()
    scheduler = Scheduler(clock)
    conn = db.connect("scheduler")
    scheduler(conn)
    clock.advance(REPLAN_EVERY - MINUTE)
    scheduler(conn)
    assert len(plans) == 1


def test_a_cycle_at_the_interval_plans_again(db, plans):
    clock = Clock()
    scheduler = Scheduler(clock)
    conn = db.connect("scheduler")
    scheduler(conn)
    clock.advance(REPLAN_EVERY)
    scheduler(conn)
    assert len(plans) == 2


def test_the_interval_can_be_set_per_scheduler(db, plans):
    clock = Clock()
    scheduler = Scheduler(clock, replan_every=timedelta(0))
    conn = db.connect("scheduler")
    scheduler(conn)
    scheduler(conn)
    assert len(plans) == 2


def test_replanning_is_idempotent_and_logs_only_new_jobs(db, plans, caplog):
    clock = Clock()
    scheduler = Scheduler(clock, replan_every=timedelta(0))
    conn = db.connect("scheduler")
    with caplog.at_level(logging.INFO, logger=service.log.name):
        scheduler(conn)
        first = jobs_written(db)
        scheduled = [r.getMessage() for r in caplog.records if "scheduled" in r.getMessage()]
        caplog.clear()
        scheduler(conn)
    assert len(scheduled) == 1 and scheduled[0] == f"scheduled {first} new jobs"
    assert jobs_written(db) == first
    assert not [r for r in caplog.records if "scheduled" in r.getMessage()]


def test_planning_asks_for_approval_until_the_ladder_reaches_l2(db, plans):
    Scheduler(Clock())(db.connect("scheduler"))
    [(today, kwargs)] = plans
    assert today == NOW.date()
    assert kwargs == {"approval_required": True}


def test_the_calendar_is_built_once_and_reused_while_it_covers_the_window():
    scheduler = Scheduler()
    first = scheduler.calendar(NOW)
    assert scheduler.calendar(NOW + timedelta(days=3)) is first


def test_the_calendar_is_rebuilt_when_time_moves_past_its_window():
    scheduler = Scheduler()
    first = scheduler.calendar(NOW)
    later = scheduler.calendar(NOW + timedelta(days=400))
    assert later is not first
    start, end = schedule.calendar_window((NOW + timedelta(days=400)).date())
    assert later.covers(start, end)


def test_an_expired_lease_is_recorded_once_and_logged_as_a_warning(db, caplog):
    scheduler_conn = db.connect("scheduler")
    (job_id,) = scheduler_conn.execute(
        "INSERT INTO agb.job (kind, period, session_date, not_before, created_by)"
        " VALUES ('submission', '2026-10', current_date, now() - interval '1 hour', 'scheduler')"
        " RETURNING id"
    ).fetchone()
    scheduler_conn.execute(
        "INSERT INTO agb.job_event (job_id, event, actor) VALUES (%s, 'created', 'scheduler')",
        (job_id,),
    )
    # A lease that is over as soon as it is taken, so no clock has to be waited on.
    jobs.claim(db.connect("execution"), ["submission"], "execution", timedelta(seconds=-1))

    scheduler = Scheduler(Clock(), replan_every=timedelta(days=1))
    with caplog.at_level(logging.WARNING, logger=service.log.name):
        scheduler(scheduler_conn)
        scheduler(scheduler_conn)
    expired = [r.getMessage() for r in caplog.records if "expired job leases" in r.getMessage()]
    assert expired == ["recorded 1 expired job leases"]
