from datetime import UTC, datetime, timedelta

import pytest

from agb import cli, heartbeat


@pytest.mark.parametrize(
    ("age", "text"),
    [
        (timedelta(seconds=-2), "0s"),
        (timedelta(seconds=7), "7s"),
        (timedelta(minutes=3, seconds=4), "3m 4s"),
        (timedelta(hours=2, minutes=5, seconds=59), "2h 5m"),
        (timedelta(days=3, hours=4, minutes=1), "3d 4h"),
    ],
)
def test_format_age(age, text):
    assert cli.format_age(age) == text


def test_format_status_lists_every_known_service_then_any_others():
    beat_at = datetime(2026, 9, 15, 12, 0, 5, tzinfo=UTC)
    out = cli.format_status(
        [
            ("scheduler", beat_at, timedelta(seconds=12), "abc123"),
            ("zeta", beat_at, timedelta(hours=1), None),
        ]
    ).splitlines()

    assert out[0].split() == ["SERVICE", "LAST", "BEAT", "(UTC)", "AGE", "VERSION"]
    assert [line.split()[0] for line in out[1:]] == [
        "scheduler",
        "pipeline",
        "execution",
        "bot",
        "ui",
        "backup",
        "zeta",
    ]
    assert out[1].split() == ["scheduler", "2026-09-15", "12:00:05", "12s", "abc123"]
    assert out[2].split() == ["pipeline", "never", "-", "-"]
    assert out[7].split() == ["zeta", "2026-09-15", "12:00:05", "1h", "0m", "-"]


def test_status_reads_heartbeats_as_the_cli_role(db):
    heartbeat.beat(db.connect("scheduler"), datetime.now(UTC), "abc123")

    lines = cli.status(db.connect("cli")).splitlines()

    scheduler = lines[1].split()
    assert scheduler[0] == "scheduler"
    assert scheduler[-1] == "abc123"
    assert lines[2].split()[:2] == ["pipeline", "never"]


def test_main_status(db, monkeypatch, capsys):
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)

    assert cli.main(["status"]) == 0
    assert "SERVICE" in capsys.readouterr().out


def test_main_reports_connection_errors(db, monkeypatch, capsys):
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("PGPASSWORD", "wrong")

    assert cli.main(["status"]) == 1
    assert "password authentication failed" in capsys.readouterr().err
