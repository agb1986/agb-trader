"""The backup service: a dated dump for each night's job, retention, a missed or failed backup in
the overdue view, and a real dump restored into a scratch database."""

import gzip
import subprocess
import threading
import time
from datetime import UTC, date, datetime, timedelta

import pytest
from conftest import SUPERUSER, create_database, drop_database
from psycopg import errors, sql

from agb import heartbeat, jobs, schedule
from agb.schedule import Expected, Planned
from agb.services.backup import Backup, prune

PERIOD = "2026-09-14"
DUMP = f"agb-{PERIOD}.sql.gz"
MINUTE = timedelta(minutes=1)
COMPLETE = b"SELECT 1;\n-- PostgreSQL database dump complete\n"


def fake(script):
    """A stand-in for pg_dump. Backup appends pg_dump's options, which `sh -c` ignores."""
    return ("sh", "-c", script, "pg_dump")


FAKE_DUMP = fake(f"printf '{COMPLETE.decode()}'")


def plan_backup(db, period=PERIOD, *, start=None):
    """The scheduler's backup job for `period`, claimable from `start`. Returns when it is due."""
    start = start or datetime.now(UTC) - timedelta(hours=1)
    due = start + schedule.BACKUP_GRACE
    job = Planned(
        schedule.BACKUP,
        period,
        date.fromisoformat(period),
        start,
        (Expected(schedule.BACKUP, period, due, True),),
    )
    schedule.write(db.connect("scheduler"), [job])
    return due


def late_backups(db, at):
    return (
        db.connect("cli")
        .execute("SELECT period FROM agb.overdue_at(%s) WHERE kind = 'backup'", (at,))
        .fetchall()
    )


def backup_state(db):
    return (
        db.connect("cli")
        .execute("SELECT state FROM agb.job_state WHERE kind = 'backup'")
        .fetchone()[0]
    )


def seed_dumps(directory, *periods):
    for period in periods:
        (directory / f"agb-{period}.sql.gz").write_bytes(gzip.compress(COMPLETE))
    return names(directory)


def names(directory):
    return sorted(p.name for p in directory.iterdir())


def test_a_dump_is_written_and_fulfils_the_night(db, tmp_path):
    seed_dumps(tmp_path, "2026-09-11", "2026-09-12", "2026-09-13")
    (tmp_path / "agb-2026-09-13.sql.gz.partial").write_bytes(b"")  # left by a crash
    due = plan_backup(db)

    Backup(tmp_path, keep=2, pg_dump=FAKE_DUMP)(db.connect("backup"))

    assert names(tmp_path) == ["agb-2026-09-13.sql.gz", DUMP]
    with gzip.open(tmp_path / DUMP) as dump:
        assert dump.read() == COMPLETE
    assert backup_state(db) == "completed"
    assert late_backups(db, due + MINUTE) == []


def test_a_backup_that_never_runs_is_overdue(db):
    due = plan_backup(db)
    assert late_backups(db, due - MINUTE) == []
    assert late_backups(db, due + MINUTE) == [(PERIOD,)]


@pytest.mark.parametrize(
    ("command", "detail"),
    [
        (fake("echo 'SELECT 1;'; echo 'out of disk' >&2; exit 3"), "exited 3: out of disk"),
        (fake("echo 'SELECT 1;'"), "no trailer"),
        (("/nonexistent/pg_dump",), "cannot run"),
    ],
    ids=["exits-non-zero", "truncated", "missing"],
)
def test_a_failed_dump_is_overdue_and_prunes_nothing(db, tmp_path, command, detail):
    before = seed_dumps(tmp_path, "2026-09-11", "2026-09-12", "2026-09-13")
    due = plan_backup(db)

    Backup(tmp_path, keep=1, pg_dump=command)(db.connect("backup"))

    assert names(tmp_path) == before  # no partial dump, and nothing pruned
    assert backup_state(db) == "failed"
    [(failure,)] = (
        db.connect("cli")
        .execute("SELECT detail FROM agb.job_event WHERE event = 'failed'")
        .fetchall()
    )
    assert detail in failure
    assert late_backups(db, due + MINUTE) == [(PERIOD,)]


def test_nothing_is_dumped_before_the_job_is_due(db, tmp_path):
    plan_backup(db, start=datetime.now(UTC) + timedelta(hours=1))
    Backup(tmp_path, pg_dump=("/nonexistent/pg_dump",))(db.connect("backup"))
    assert names(tmp_path) == []


def test_a_backup_whose_lease_expired_is_redone(db, tmp_path):
    plan_backup(db)
    conn = db.connect("backup")
    assert jobs.claim(conn, [schedule.BACKUP], "backup", timedelta(milliseconds=1))
    time.sleep(0.05)

    Backup(tmp_path, pg_dump=FAKE_DUMP)(conn)

    assert names(tmp_path) == [DUMP]
    assert backup_state(db) == "completed"


def test_prune_keeps_the_newest_dumps_and_clears_partials(tmp_path):
    seed_dumps(tmp_path, "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04")
    for other in ("agb-2026-09-05.sql.gz.partial", "notes.txt", "agb-latest.sql.gz"):
        (tmp_path / other).write_bytes(b"")

    removed = prune(tmp_path, 2)

    assert sorted(p.name for p in removed) == [
        "agb-2026-09-01.sql.gz",
        "agb-2026-09-02.sql.gz",
        "agb-2026-09-05.sql.gz.partial",
    ]
    assert names(tmp_path) == [
        "agb-2026-09-03.sql.gz",
        "agb-2026-09-04.sql.gz",
        "agb-latest.sql.gz",
        "notes.txt",
    ]


def test_the_service_beats_its_heartbeat_and_backs_up(db, tmp_path, monkeypatch):
    for key, value in db.pg_env("backup").items():
        monkeypatch.setenv(key, value)
    plan_backup(db)
    stop = threading.Event()
    run = Backup(tmp_path, pg_dump=FAKE_DUMP)

    def cycle(conn):
        run(conn)
        stop.set()

    heartbeat.serve("backup", 0.01, stop, cycle)

    services = db.connect("cli").execute("SELECT service FROM agb.heartbeat").fetchall()
    assert services == [("backup",)]
    assert names(tmp_path) == [DUMP]


def test_backup_cannot_report_for_another_service(db):
    with pytest.raises(errors.InsufficientPrivilege, match="row-level security"):
        db.connect("backup").execute(
            "INSERT INTO agb.heartbeat (service, started_at) VALUES ('scheduler', now())"
        )


def test_row_level_security_never_hides_a_row_from_backup(db):
    # pg_dump runs as backup with row-level security on (see migrations/README.md), so a policy
    # that hid rows from it would silently drop them from every backup.
    with db.cluster.connect(db.name, SUPERUSER) as conn:
        tables = conn.execute("""
            SELECT c.relname,
                   EXISTS (SELECT 1 FROM pg_policies p
                           WHERE p.schemaname = 'agb' AND p.tablename = c.relname
                             AND p.permissive = 'PERMISSIVE' AND p.cmd IN ('SELECT', 'ALL')
                             AND p.roles && ARRAY['public', 'backup']::name[]
                             AND p.qual = 'true'),
                   EXISTS (SELECT 1 FROM pg_policies p
                           WHERE p.schemaname = 'agb' AND p.tablename = c.relname
                             AND p.permissive = 'RESTRICTIVE' AND p.cmd IN ('SELECT', 'ALL')
                             AND p.roles && ARRAY['public', 'backup']::name[]
                             AND p.qual <> 'true')
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'agb' AND c.relkind IN ('r', 'p') AND c.relrowsecurity
            ORDER BY 1
        """).fetchall()
    assert ("heartbeat", True, False) in tables  # the check is not vacuous
    assert [t for t in tables if t[1:] != (True, False)] == []


def row_counts(cluster, dbname):
    with cluster.connect(dbname, SUPERUSER) as conn:
        tables = conn.execute("""
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
        """).fetchall()
        count = sql.SQL("SELECT count(*) FROM {}.{}")
        return {
            f"{schema}.{table}": conn.execute(
                count.format(sql.Identifier(schema), sql.Identifier(table))
            ).fetchone()[0]
            for schema, table in tables
        }


def test_a_real_dump_restores_into_a_scratch_database(db, tmp_path, monkeypatch):
    cluster = db.cluster
    for key, value in db.pg_env("backup").items():
        monkeypatch.setenv(key, value)
    now = datetime.now(UTC)
    heartbeat.beat(db.connect("scheduler"), now, "v1")
    heartbeat.beat(db.connect("backup"), now, "v1")
    later = now + timedelta(days=1)
    pull = Planned(
        schedule.DATA_PULL,
        "2026-09-15",
        date(2026, 9, 15),
        later,
        (Expected(schedule.DATA_PULL, "2026-09-15", later + timedelta(hours=12), True),),
    )
    schedule.write(db.connect("scheduler"), [pull])
    plan_backup(db)
    # pg_dump inside the test's Postgres container, connecting as backup with its password over
    # TCP, as the service does. The PG* values come from this process's environment.
    pg_dump = (
        "docker", "exec",
        "--env", "PGHOST=localhost", "--env", "PGPORT=5432",
        "--env", "PGUSER", "--env", "PGPASSWORD", "--env", "PGDATABASE",
        cluster.container, "pg_dump",
    )  # fmt: skip

    Backup(tmp_path, pg_dump=pg_dump)(db.connect("backup"))
    assert backup_state(db) == "completed"

    scratch = create_database(cluster, "template0")
    try:
        load = subprocess.run(
            [
                "docker", "exec", "--interactive", cluster.container,
                "psql", "--username=postgres", f"--dbname={scratch}",
                "--no-psqlrc", "--set=ON_ERROR_STOP=1", "--quiet",
            ],
            input=gzip.decompress((tmp_path / DUMP).read_bytes()),
            capture_output=True,
        )  # fmt: skip
        assert load.returncode == 0, load.stderr.decode()[-4000:]
        restored = row_counts(cluster, scratch)
    finally:
        drop_database(cluster, scratch)

    live = row_counts(cluster, db.name)
    # Completing the backup logged its event and fulfilled its expectation after the dump.
    live["agb.job_event"] -= 1
    live["agb.expected_event_completion"] -= 1
    assert restored == live
    assert live["agb.heartbeat"] == 2
    assert live["agb.job"] == 2
