"""The overdue view: past-due expected events, and no rebalance within the horizon."""

from datetime import UTC, date, datetime, timedelta

import pytest
from psycopg import errors

from agb import jobs, schedule
from agb.nyse import NyseCalendar
from agb.roles import OWNER, SERVICE_ROLES


def expect(db, kind, period, due, job_id=None):
    return (
        db.connect("scheduler")
        .execute(
            "INSERT INTO agb.expected_event (kind, period, due_at, job_id, created_by) "
            f"VALUES (%s, %s, {due}, %s, 'scheduler') RETURNING id",
            (kind, period, job_id),
        )
        .fetchone()[0]
    )


def overdue(conn):
    return conn.execute("SELECT kind, period, reason FROM agb.overdue").fetchall()


@pytest.fixture
def rebalance_ahead(db):
    """A submission due within the horizon, so only past-due items show."""
    expect(db, "submission", "2026-10", "now() + interval '10 days'")


def test_a_past_due_expected_event_is_overdue_until_fulfilled(db, rebalance_ahead):
    cli = db.connect("cli")
    expect(db, "approval_request", "2026-10", "now() - interval '1 minute'")
    expect(db, "data_pull", "2026-09-30", "now() + interval '1 hour'")
    assert overdue(cli) == [("approval_request", "2026-10", "past_due")]

    assert jobs.fulfil(db.connect("bot"), "approval_request", "2026-10", "alert_event", 7, "bot")
    assert not jobs.fulfil(
        db.connect("bot"), "approval_request", "2026-10", "alert_event", 8, "bot"
    )
    assert overdue(cli) == []
    assert cli.execute(
        "SELECT ref_table, ref_id, actor FROM agb.expected_event_completion"
    ).fetchall() == [("alert_event", 7, "bot")]


def test_completing_a_job_fulfils_its_expected_event(db, rebalance_ahead):
    scheduler = db.connect("scheduler")
    (job_id,) = scheduler.execute(
        "INSERT INTO agb.job (kind, period, session_date, not_before, created_by) "
        "VALUES ('rebalance', '2026-10', current_date, now(), 'scheduler') RETURNING id"
    ).fetchone()
    expect(db, "proposal", "2026-10", "now() + interval '1 second'", job_id)
    pipeline = db.connect("pipeline")
    claim = jobs.claim(pipeline, ["rebalance"], "pipeline", timedelta(minutes=5))
    event_id = jobs.complete(pipeline, claim)

    cli = db.connect("cli")
    assert cli.execute(
        "SELECT ref_table, ref_id, actor FROM agb.expected_event_completion"
    ).fetchall() == [("job_event", event_id, "pipeline")]
    assert cli.execute("SELECT * FROM agb.overdue_at(now() + interval '1 day')").fetchall() == []


def test_a_failed_job_leaves_its_expected_event_overdue(db, rebalance_ahead):
    scheduler = db.connect("scheduler")
    (job_id,) = scheduler.execute(
        "INSERT INTO agb.job (kind, period, session_date, not_before, created_by) "
        "VALUES ('data_pull', '2026-09-30', current_date, now(), 'scheduler') RETURNING id"
    ).fetchone()
    expect(db, "data_pull", "2026-09-30", "now() - interval '1 second'", job_id)
    pipeline = db.connect("pipeline")
    jobs.fail(pipeline, jobs.claim(pipeline, ["data_pull"], "pipeline", timedelta(1)), "503")
    assert overdue(db.connect("cli")) == [("data_pull", "2026-09-30", "past_due")]


def test_no_rebalance_within_horizon_when_nothing_is_scheduled(db):
    assert overdue(db.connect("cli")) == [("submission", None, "no_rebalance_within_horizon")]


def test_no_rebalance_within_horizon_follows_the_configured_horizon(db):
    expect(db, "submission", "2026-10", "now() + interval '20 days'")
    cli = db.connect("cli")
    assert overdue(cli) == []

    cli.execute("SET agb.rebalance_horizon = '10 days'")
    assert overdue(cli) == [("submission", None, "no_rebalance_within_horizon")]
    cli.execute("RESET agb.rebalance_horizon")
    assert overdue(cli) == []


def test_a_past_submission_does_not_count_as_a_rebalance_ahead(db):
    expect(db, "submission", "2026-09", "now() - interval '1 day'")
    assert set(overdue(db.connect("cli"))) == {
        ("submission", "2026-09", "past_due"),
        ("submission", None, "no_rebalance_within_horizon"),
    }


def test_a_dead_scheduler_shows_as_no_rebalance_within_horizon(db):
    """Schedule once, then look at the overdue view as time passes with no scheduler running."""
    cal = NyseCalendar(date(2026, 8, 1), date(2027, 3, 31))
    schedule.write(db.connect("scheduler"), schedule.plan(cal, date(2026, 9, 15)))
    cli = db.connect("cli")

    def reasons(at):
        rows = cli.execute("SELECT reason, period FROM agb.overdue_at(%s)", (at,)).fetchall()
        return {reason for reason, _ in rows}

    assert reasons(datetime(2026, 9, 15, 12, tzinfo=UTC)) == set()
    # The plan reaches 45 days ahead, so by late November the next rebalance, 2027-01, is
    # beyond what was written.
    assert "no_rebalance_within_horizon" in reasons(datetime(2026, 11, 28, tzinfo=UTC))


def test_every_role_can_read_the_overdue_view(db):
    for role in SERVICE_ROLES:
        db.connect(role).execute("SELECT * FROM agb.overdue").fetchall()


def test_completion_is_appended_never_rewritten(db):
    expected_id = expect(db, "submission", "2026-10", "now()")
    jobs.fulfil(db.connect("execution"), "submission", "2026-10", "order_event", 1, "execution")
    owner = db.connect(OWNER)
    for statement in (
        "UPDATE agb.expected_event SET due_at = now() + interval '1 day'",
        "DELETE FROM agb.expected_event_completion",
    ):
        with pytest.raises(errors.RaiseException, match="append-only"):
            owner.execute(statement)
    with pytest.raises(errors.UniqueViolation):
        db.connect("execution").execute(
            "INSERT INTO agb.expected_event_completion "
            "(expected_event_id, ref_table, ref_id, actor) VALUES (%s, 'x', 2, 'execution')",
            (expected_id,),
        )
