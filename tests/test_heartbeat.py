import os
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta

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


def set_age(conn, age: str) -> None:
    """Push the connecting service's own beat back by `age`, as a service that stopped beating
    that long ago would have left it."""
    conn.execute(
        "UPDATE agb.heartbeat SET beat_at = now() - %s::interval WHERE service = current_user",
        [age],
    )


def test_healthcheck_passes_on_a_fresh_beat(db, monkeypatch, capsys):
    for key, value in db.pg_env("scheduler").items():
        monkeypatch.setenv(key, value)
    heartbeat.beat(db.connect("scheduler"), datetime.now(UTC), "v1")

    assert heartbeat.healthcheck(heartbeat.STALE_AFTER) == 0
    assert "last beat" in capsys.readouterr().out


def test_healthcheck_fails_on_a_stale_beat(db, monkeypatch, capsys):
    for key, value in db.pg_env("scheduler").items():
        monkeypatch.setenv(key, value)
    scheduler = db.connect("scheduler")
    heartbeat.beat(scheduler, datetime.now(UTC), "v1")
    set_age(scheduler, "6 minutes")

    assert heartbeat.healthcheck(heartbeat.STALE_AFTER) == 1
    assert "stale" in capsys.readouterr().err


def test_healthcheck_judges_only_its_own_service(db, monkeypatch, capsys):
    """A fresh beat from another service does not make a silent one healthy."""
    for key, value in db.pg_env("pipeline").items():
        monkeypatch.setenv(key, value)
    heartbeat.beat(db.connect("scheduler"), datetime.now(UTC), "v1")

    assert heartbeat.healthcheck(heartbeat.STALE_AFTER) == 1
    assert "no heartbeat yet" in capsys.readouterr().err


def test_healthcheck_fails_when_the_database_is_unreachable(db, monkeypatch, capsys):
    for key, value in db.pg_env("scheduler").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("PGPASSWORD", "wrong")

    assert heartbeat.healthcheck(heartbeat.STALE_AFTER) == 1
    assert "password authentication failed" in capsys.readouterr().err


def test_stale_after_comes_from_the_environment(monkeypatch):
    monkeypatch.delenv("AGB_STALE_AFTER_SECONDS", raising=False)
    assert heartbeat.stale_after_from_env() == heartbeat.STALE_AFTER
    monkeypatch.setenv("AGB_STALE_AFTER_SECONDS", "90")
    assert heartbeat.stale_after_from_env() == timedelta(seconds=90)


@pytest.fixture
def serve_ui(db, monkeypatch):
    """Start the UI's real handler on a free loopback port, as tests/test_ui.py serves it, with
    the names AGB_UI_ALLOWED_HOSTS gives when it starts, and return its base URL. The port is
    not 8080, so a request must say which name it is addressed to."""
    from agb.services import ui as service

    monkeypatch.delenv("AGB_UI_HOST", raising=False)
    monkeypatch.delenv("AGB_UI_ALLOWED_HOSTS", raising=False)
    monkeypatch.setattr(service, "PORT", 0)
    servers = []

    def start(*, own_name: bool = False) -> str:
        """`own_name` makes the server answer `127.0.0.1:<its port>` alone, the URL's own name,
        as tests/test_ui.py's server does."""
        httpd = service.server()
        if own_name:
            httpd.allowed_hosts = frozenset({f"127.0.0.1:{httpd.server_address[1]}"})
        servers.append(httpd)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{httpd.server_address[1]}"

    yield start
    for httpd in servers:
        httpd.shutdown()
        httpd.server_close()


@pytest.fixture
def ui_beating(db, monkeypatch):
    for key, value in db.pg_env("ui").items():
        monkeypatch.setenv(key, value)
    heartbeat.beat(db.connect("ui"), datetime.now(UTC), "v1")


def test_the_ui_check_asks_the_server_as_well_as_the_heartbeat(ui_beating, capsys, serve_ui):
    url = serve_ui()

    assert heartbeat.healthcheck(heartbeat.STALE_AFTER, f"{url}/", "127.0.0.1:8080") == 0
    assert heartbeat.healthcheck(heartbeat.STALE_AFTER, f"{url}/nowhere", "127.0.0.1:8080") == 1
    assert "answered 404" in capsys.readouterr().err


def test_the_ui_check_fails_on_a_name_the_ui_refuses(ui_beating, capsys, serve_ui):
    """The URL's own name, `127.0.0.1:<port>`, is not one the UI answers."""
    url = serve_ui()

    assert heartbeat.healthcheck(heartbeat.STALE_AFTER, f"{url}/") == 1
    assert "answered 421" in capsys.readouterr().err


def test_the_ui_check_follows_a_replaced_list_of_names(ui_beating, monkeypatch, serve_ui):
    """AGB_UI_ALLOWED_HOSTS replaces the UI's default names, and the check sends one of them."""
    monkeypatch.setenv("AGB_UI_ALLOWED_HOSTS", " , LocalHost:9090,127.0.0.1:9090")
    url = serve_ui()

    assert heartbeat.probe_host_from_env() == "localhost:9090"
    assert heartbeat.healthcheck(heartbeat.STALE_AFTER, f"{url}/", "127.0.0.1:8080") == 1
    assert (
        heartbeat.healthcheck(heartbeat.STALE_AFTER, f"{url}/", heartbeat.probe_host_from_env())
        == 0
    )


def test_without_a_list_the_ui_check_passes_on_the_url_s_own_name(ui_beating, serve_ui):
    url = serve_ui(own_name=True)

    assert heartbeat.probe_host_from_env() is None
    assert heartbeat.healthcheck(heartbeat.STALE_AFTER, f"{url}/", None) == 0


@pytest.mark.parametrize("configured", [None, "", " , "])
def test_without_a_list_the_check_sends_the_url_s_own_name(monkeypatch, configured):
    if configured is None:
        monkeypatch.delenv("AGB_UI_ALLOWED_HOSTS", raising=False)
    else:
        monkeypatch.setenv("AGB_UI_ALLOWED_HOSTS", configured)
    assert heartbeat.probe_host_from_env() is None


def test_the_ui_check_fails_when_nothing_answers(ui_beating, capsys):
    assert heartbeat.healthcheck(heartbeat.STALE_AFTER, "http://127.0.0.1:1/") == 1
    assert "http://127.0.0.1:1/" in capsys.readouterr().err


def test_the_check_runs_as_a_module_with_the_ui_s_url(db, serve_ui):
    """The command line compose.yaml gives the UI, run the way Docker runs it. The test server
    is not on 8080, so the list stands in for the container's, where the URL's own name works."""
    url = serve_ui()
    env = {**os.environ, **db.pg_env("ui"), "AGB_UI_ALLOWED_HOSTS": "127.0.0.1:8080"}
    heartbeat.beat(db.connect("ui"), datetime.now(UTC), "v1")

    checked = subprocess.run(
        [sys.executable, "-m", "agb.heartbeat", f"{url}/"],
        env=env, capture_output=True, text=True, timeout=30,
    )  # fmt: skip

    assert checked.returncode == 0, checked.stderr
    assert checked.stdout.startswith("last beat")
