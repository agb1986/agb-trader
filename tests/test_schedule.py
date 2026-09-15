"""The scheduler: jobs on the right NYSE sessions, idempotent scheduling, and the service loop."""

import threading
from datetime import UTC, date, datetime, timedelta

import pytest
from psycopg import errors

from agb import schedule
from agb.nyse import NyseCalendar
from agb.services.scheduler import Scheduler


@pytest.fixture(scope="module")
def cal():
    return NyseCalendar(date(2023, 1, 1), date(2027, 12, 31))


def by_key(planned):
    return {(p.kind, p.period): p for p in planned}


def expected_of(planned_job):
    return {e.kind: e for e in planned_job.expected}


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def test_a_data_pull_for_each_session_across_a_holiday_and_an_early_close(cal):
    planned = by_key(schedule.plan(cal, date(2026, 11, 23)))
    pulls = sorted(period for kind, period in planned if kind == schedule.DATA_PULL)

    assert pulls[:5] == ["2026-11-23", "2026-11-24", "2026-11-25", "2026-11-27", "2026-11-30"]
    assert ("data_pull", "2026-11-26") not in planned  # Thanksgiving

    before_holiday = planned[("data_pull", "2026-11-25")]
    assert before_holiday.not_before == utc(2026, 11, 25, 23, 0)  # 16:00 ET close + 2h
    # Due by the next session's open, which skips Thanksgiving.
    assert expected_of(before_holiday)["data_pull"].due_at == utc(2026, 11, 27, 14, 30)

    early_close = planned[("data_pull", "2026-11-27")]
    assert early_close.not_before == utc(2026, 11, 27, 20, 0)  # 13:00 ET close + 2h


def test_no_pulls_on_christmas_or_good_friday(cal):
    periods = {p.period for p in schedule.plan(cal, date(2026, 12, 20))}
    assert "2026-12-24" in periods and "2026-12-25" not in periods
    periods = {p.period for p in schedule.plan(cal, date(2026, 3, 30))}
    assert "2026-04-02" in periods and "2026-04-03" not in periods


def test_the_rebalance_trades_on_the_session_after_the_month_end(cal):
    planned = by_key(schedule.plan(cal, date(2026, 9, 15)))

    rebalance = planned[("rebalance", "2026-10")]
    assert rebalance.session_date == date(2026, 10, 1)
    assert rebalance.not_before == utc(2026, 9, 30, 22, 0)  # 30 Sep close + 2h
    expected = expected_of(rebalance)
    assert expected["proposal"].due_at == utc(2026, 10, 1, 13, 30)
    assert expected["proposal"].by_job
    assert expected["approval_request"].due_at == utc(2026, 10, 1, 14, 0)
    assert expected["submission"].due_at == utc(2026, 10, 1, 19, 45)  # 15:45 EDT
    assert not expected["submission"].by_job


def test_a_month_end_on_a_holiday(cal):
    # 31 May 2027 is Memorial Day: the signal comes from Friday the 28th.
    rebalance = by_key(schedule.plan(cal, date(2027, 5, 20)))[("rebalance", "2027-06")]
    assert rebalance.session_date == date(2027, 6, 1)
    assert rebalance.not_before == utc(2027, 5, 28, 22, 0)


def test_a_month_end_on_an_early_close(cal):
    rebalance = by_key(schedule.plan(cal, date(2025, 11, 20)))[("rebalance", "2025-12")]
    assert rebalance.session_date == date(2025, 12, 1)
    assert rebalance.not_before == utc(2025, 11, 28, 20, 0)  # 13:00 ET close + 2h


def test_a_rebalance_on_an_early_close_moves_the_cutoff(cal):
    rebalance = by_key(schedule.plan(cal, date(2023, 6, 20)))[("rebalance", "2023-07")]
    assert rebalance.session_date == date(2023, 7, 3)
    assert expected_of(rebalance)["submission"].due_at == utc(2023, 7, 3, 16, 45)  # 12:45 EDT


def test_a_rebalance_after_new_year_and_across_daylight_saving(cal):
    january = by_key(schedule.plan(cal, date(2026, 12, 20)))[("rebalance", "2027-01")]
    assert january.session_date == date(2027, 1, 4)  # 1 Jan is a Friday holiday
    november = by_key(schedule.plan(cal, date(2026, 10, 20)))[("rebalance", "2026-11")]
    assert november.session_date == date(2026, 11, 2)
    assert expected_of(november)["submission"].due_at == utc(2026, 11, 2, 20, 45)  # 15:45 EST


def test_the_rebalance_is_planned_until_its_session_passes(cal):
    # On the rebalance session itself the job is still in the plan; the day after, it is not.
    assert ("rebalance", "2026-10") in by_key(schedule.plan(cal, date(2026, 10, 1)))
    assert ("rebalance", "2026-10") not in by_key(schedule.plan(cal, date(2026, 10, 2)))


def test_the_lookahead_always_holds_the_next_rebalance(cal):
    # Every day whose calendar window lies inside the test calendar.
    day = date(2023, 2, 15)
    while day < date(2027, 10, 1):
        planned = schedule.plan(cal, day)
        submissions = [
            e.due_at for p in planned for e in p.expected if e.kind == schedule.SUBMISSION
        ]
        assert submissions, day
        assert min(submissions) - datetime(day.year, day.month, day.day, tzinfo=UTC) < timedelta(
            days=40
        ), day
        day += timedelta(days=1)


def test_approval_request_only_when_approval_is_required(cal):
    rebalance = by_key(schedule.plan(cal, date(2026, 9, 15), approval_required=False))[
        ("rebalance", "2026-10")
    ]
    assert set(expected_of(rebalance)) == {"proposal", "submission"}


def counts(conn):
    return conn.execute("""
        SELECT (SELECT count(*) FROM agb.job),
               (SELECT count(*) FROM agb.job_event),
               (SELECT count(*) FROM agb.expected_event)
    """).fetchone()


def test_scheduling_the_same_plan_twice_is_a_no_op(db, cal):
    scheduler = db.connect("scheduler")
    planned = schedule.plan(cal, date(2026, 9, 15))

    assert schedule.write(scheduler, planned) == len(planned)
    first = counts(scheduler)
    assert schedule.write(scheduler, planned) == 0

    assert counts(scheduler) == first
    assert first[0] == first[1] == len(planned)  # one 'created' event per job
    [(n,)] = scheduler.execute(
        "SELECT count(*) FROM agb.job WHERE kind = 'rebalance' AND period = '2026-10'"
    ).fetchall()
    assert n == 1


def test_a_duplicate_kind_and_period_fails_on_the_constraint(db):
    scheduler = db.connect("scheduler")
    insert = (
        "INSERT INTO agb.job (kind, period, session_date, not_before, created_by) "
        "VALUES ('rebalance', '2026-10', '2026-10-01', now(), 'scheduler')"
    )
    scheduler.execute(insert)
    with pytest.raises(errors.UniqueViolation):
        scheduler.execute(insert)
    assert scheduler.execute("SELECT count(*) FROM agb.job").fetchone()[0] == 1


def test_expected_events_link_to_the_job_that_fulfils_them(db, cal):
    scheduler = db.connect("scheduler")
    schedule.write(scheduler, schedule.plan(cal, date(2026, 9, 15)))
    rows = scheduler.execute("""
        SELECT e.kind, j.kind FROM agb.expected_event e LEFT JOIN agb.job j ON j.id = e.job_id
        WHERE e.period = '2026-10' ORDER BY e.kind
    """).fetchall()
    assert rows == [
        ("approval_request", None),
        ("proposal", "rebalance"),
        ("submission", None),
    ]


def test_only_the_scheduler_creates_jobs(db):
    for role in ("pipeline", "execution", "bot", "ui", "cli"):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect(role).execute(
                "INSERT INTO agb.job (kind, period, session_date, not_before, created_by) "
                "VALUES ('rebalance', '2026-10', '2026-10-01', now(), 'x')"
            )
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect(role).execute(
                "INSERT INTO agb.expected_event (kind, period, due_at, created_by) "
                "VALUES ('submission', '2026-10', now(), 'x')"
            )


def test_the_service_beats_its_heartbeat_and_schedules(db, monkeypatch):
    for key, value in db.pg_env("scheduler").items():
        monkeypatch.setenv(key, value)
    now = utc(2026, 9, 28, 14, 0)
    scheduler = Scheduler(lambda: now, replan_every=timedelta(0))
    stop = threading.Event()
    cycles = []

    def cycle(conn):
        scheduler(conn)
        cycles.append(counts(conn))
        if len(cycles) == 2:
            stop.set()

    from agb import heartbeat

    heartbeat.serve("scheduler", 0.01, stop, cycle)

    cli = db.connect("cli")
    assert cli.execute("SELECT service FROM agb.heartbeat").fetchall() == [("scheduler",)]
    assert cycles[0] == cycles[1]  # the second cycle replanned and added nothing
    assert cli.execute(
        "SELECT session_date FROM agb.job WHERE kind = 'rebalance' AND period = '2026-10'"
    ).fetchone() == (date(2026, 10, 1),)
    # Nothing is overdue at the moment the scheduler ran, and the next rebalance is in view.
    assert cli.execute("SELECT * FROM agb.overdue_at(%s)", (now,)).fetchall() == []
