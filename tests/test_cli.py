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


def test_main_reconcile_refuses_without_the_brokers_keys(db, monkeypatch, capsys):
    for key, value in db.pg_env("execution").items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)

    assert cli.main(["reconcile", "2026-10"]) == 1
    assert "refused: ALPACA_API_KEY and ALPACA_SECRET_KEY are not set" in capsys.readouterr().err


def test_main_reconcile_reports_an_unknown_basket_before_it_asks_the_broker(
    db, monkeypatch, capsys
):
    """The basket is looked up first, so a mistyped reference costs no call and says so."""
    for key, value in db.pg_env("execution").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("ALPACA_API_KEY", "key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "secret")

    assert cli.main(["reconcile", "2026-09"]) == 1
    assert "agb: no live basket 2026-09" in capsys.readouterr().err


# `agb runtimes`: criterion (c) of T32, poll intervals and leases set from measured run times.

_JOB = """
    INSERT INTO agb.job (kind, period, session_date, not_before, created_by)
    VALUES (%s, %s, %s, %s, 'scheduler')
    RETURNING id
"""
_AT = """
    INSERT INTO agb.job_event (job_id, event, actor, at, lease_until)
    VALUES (%s, %s, 'pipeline', %s, %s)
"""
RUN_AT = datetime(2026, 10, 1, 14, 0, tzinfo=UTC)
# A lease a claim takes out, long enough that these runs finish inside it.
LEASE = timedelta(hours=1)
# Only these events carry the lease they were taken out under; the table insists on it.
LEASED = ("claimed", "reclaimed", "lease_expired")


def job_run(conn, kind, period, *events):
    """A job whose events happened at the given offsets from `RUN_AT`, so a run has a length.

    The events are written with an explicit `at` as the superuser, which no service may do: a
    real run's lengths come from the clock, and a test's have to be chosen. A lease runs from
    the claim that took it out, so an expiry is recorded against that claim's lease and a
    reclaim takes out a new one.
    """
    job_id = conn.execute(_JOB, (kind, period, RUN_AT.date(), RUN_AT)).fetchone()[0]
    taken_out = RUN_AT
    for event, offset in events:
        at = RUN_AT + offset
        if event in ("claimed", "reclaimed"):
            taken_out = at
        lease = taken_out + LEASE if event in LEASED else None
        conn.execute(_AT, (job_id, event, at, lease))
    return job_id


def test_format_runtimes_says_so_when_there_is_nothing_measured_yet():
    text = cli.format_runtimes([])

    assert text.splitlines()[0].split() == ["KIND", "RUNS", "FASTEST", "MEDIAN", "SLOWEST"]
    assert "no completed job has a claim and a completion to measure between" in text


def test_format_runtimes_puts_each_kind_on_its_own_line():
    rows = [
        ("data_pull", 42, timedelta(seconds=31), timedelta(minutes=1), timedelta(minutes=4)),
        (
            "rebalance",
            3,
            timedelta(minutes=12),
            timedelta(minutes=25),
            timedelta(hours=1, minutes=8),
        ),
    ]

    kinds, pull, rebalance = cli.format_runtimes(rows).splitlines()

    assert pull.split() == ["data_pull", "42", "31s", "1m", "0s", "4m", "0s"]
    assert rebalance.split() == ["rebalance", "3", "12m", "0s", "25m", "0s", "1h", "8m"]
    assert kinds.startswith("KIND")


def test_runtimes_measures_from_the_claim_to_the_completion(db):
    conn = db.connect("postgres")
    job_run(
        conn,
        "data_pull",
        "2026-10-01",
        ("created", timedelta(0)),
        ("claimed", timedelta(minutes=1)),
        ("completed", timedelta(minutes=3)),
    )
    job_run(
        conn,
        "data_pull",
        "2026-10-02",
        ("claimed", timedelta(0)),
        ("completed", timedelta(minutes=8)),
    )

    [line] = cli.runtimes(db.connect("cli")).splitlines()[1:]

    # Two runs of two and eight minutes: the median of an even count is one of the readings,
    # not an average of them, so a reported runtime is always one that really happened.
    assert line.split() == ["data_pull", "2", "2m", "0s", "2m", "0s", "8m", "0s"]


def test_runtimes_ignores_a_job_that_has_not_finished_or_failed(db):
    conn = db.connect("postgres")
    job_run(conn, "rebalance", "2026-10", ("claimed", timedelta(0)))
    job_run(
        conn,
        "rebalance",
        "2026-11",
        ("claimed", timedelta(0)),
        ("failed", timedelta(minutes=5)),
    )
    job_run(
        conn,
        "backup",
        "2026-10-01",
        ("claimed", timedelta(0)),
        ("completed", timedelta(seconds=45)),
    )

    text = cli.runtimes(db.connect("cli"))

    # A rebalance still running has no length yet, and one that failed is not how long the
    # work takes; only the backup is measured.
    assert [line.split()[0] for line in text.splitlines()[1:]] == ["backup"]
    assert text.splitlines()[1].split() == ["backup", "1", "45s", "45s", "45s"]


def test_runtimes_measures_a_reclaimed_job_from_the_reclaim(db):
    conn = db.connect("postgres")
    job_run(
        conn,
        "data_pull",
        "2026-10-05",
        ("claimed", timedelta(0)),
        ("lease_expired", timedelta(minutes=30)),
        ("reclaimed", timedelta(minutes=31)),
        ("completed", timedelta(minutes=33)),
    )

    [line] = cli.runtimes(db.connect("cli")).splitlines()[1:]

    # Two minutes, not thirty-three: the half hour the first holder spent dead is not work.
    assert line.split() == ["data_pull", "1", "2m", "0s", "2m", "0s", "2m", "0s"]
