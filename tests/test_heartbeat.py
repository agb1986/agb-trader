import threading
from datetime import UTC, datetime

import pytest
from psycopg import errors

from agb import heartbeat
from agb.roles import OWNER


def rows(conn):
    return conn.execute(
        "SELECT service, started_at, beat_at, version FROM agb.heartbeat ORDER BY service"
    ).fetchall()


def test_beat_upserts_the_services_own_row(db):
    scheduler = db.connect("scheduler")
    started = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)

    heartbeat.beat(scheduler, started, "v1")
    [(_, _, first_beat, _)] = rows(scheduler)
    heartbeat.beat(scheduler, started, "v2")

    [(service, started_at, beat_at, version)] = rows(scheduler)
    assert (service, started_at, version) == ("scheduler", started, "v2")
    assert beat_at >= first_beat


def test_progress_moves_the_beat_and_nothing_else(db):
    scheduler = db.connect("scheduler")
    started = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    heartbeat.beat(scheduler, started, "v1")
    [(_, _, first_beat, _)] = rows(scheduler)

    heartbeat.progress(scheduler)

    [(service, started_at, beat_at, version)] = rows(scheduler)
    assert (service, started_at, version) == ("scheduler", started, "v1")
    assert beat_at >= first_beat


def test_progress_before_the_first_beat_reports_nothing(db):
    """`serve` beats before it runs a cycle, so there is always a row by the time work can
    report progress. Without one there is nothing to move and no row to invent."""
    moved = db.connect("scheduler").execute(heartbeat._TOUCH)

    assert moved.rowcount == 0
    assert rows(db.connect("cli")) == []


def test_progress_cannot_move_another_services_beat(db):
    heartbeat.beat(db.connect("scheduler"), datetime.now(UTC), "v1")
    before = rows(db.connect("cli"))

    heartbeat.progress(db.connect("pipeline"))

    assert rows(db.connect("cli")) == before


def test_service_cannot_report_for_another(db):
    with pytest.raises(errors.InsufficientPrivilege, match="row-level security"):
        db.connect("pipeline").execute(
            "INSERT INTO agb.heartbeat (service, started_at) VALUES ('scheduler', now())"
        )


def test_service_cannot_touch_another_services_row(db):
    heartbeat.beat(db.connect("scheduler"), datetime.now(UTC), "v1")
    before = rows(db.connect("cli"))

    moved = db.connect("pipeline").execute(
        "UPDATE agb.heartbeat SET beat_at = now() + interval '1 day' WHERE service = 'scheduler'"
    )

    assert moved.rowcount == 0
    assert rows(db.connect("cli")) == before


def test_cli_cannot_beat(db):
    with pytest.raises(errors.InsufficientPrivilege):
        heartbeat.beat(db.connect("cli"), datetime.now(UTC), None)


@pytest.mark.parametrize("role", ["scheduler", "cli"])
@pytest.mark.parametrize("statement", ["DELETE FROM agb.heartbeat", "TRUNCATE agb.heartbeat"])
def test_services_cannot_remove_heartbeats(db, role, statement):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute(statement)


@pytest.mark.parametrize("statement", ["DELETE FROM agb.heartbeat", "TRUNCATE agb.heartbeat"])
def test_owner_cannot_remove_heartbeats(db, statement):
    heartbeat.beat(db.connect("scheduler"), datetime.now(UTC), None)
    with pytest.raises(errors.RaiseException, match="append-only"):
        db.connect(OWNER).execute(statement)


def test_serve_beats_then_runs_each_cycle(db, monkeypatch):
    for key, value in db.pg_env("scheduler").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("AGB_VERSION", "test-version")
    stop = threading.Event()
    cycles = []

    def cycle(conn):
        cycles.append(rows(conn))
        if len(cycles) == 2:
            stop.set()

    heartbeat.serve("scheduler", 0.01, stop, cycle)

    assert len(cycles) == 2
    [(service, _, _, version)] = cycles[0]
    assert (service, version) == ("scheduler", "test-version")
    assert cycles[1][0][2] >= cycles[0][0][2]


def test_serve_refuses_to_run_as_the_wrong_role(db, monkeypatch):
    for key, value in db.pg_env("pipeline").items():
        monkeypatch.setenv(key, value)
    with pytest.raises(RuntimeError, match="must connect as the 'scheduler' role"):
        heartbeat.serve("scheduler", 0.01, threading.Event())


def test_interval_comes_from_the_environment(monkeypatch):
    monkeypatch.delenv("AGB_HEARTBEAT_SECONDS", raising=False)
    assert heartbeat.interval_from_env() == heartbeat.DEFAULT_INTERVAL_SECONDS
    monkeypatch.setenv("AGB_HEARTBEAT_SECONDS", "1.5")
    assert heartbeat.interval_from_env() == 1.5
