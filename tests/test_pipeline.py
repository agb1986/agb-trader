"""The pipeline service's data pull: a landed session completes its job and records its
snapshot, a failed or missed pull is overdue, a re-run changes nothing, and the backfill."""

import threading
import time
import urllib.error
from datetime import UTC, date, datetime, timedelta

import pytest
from psycopg import errors
from sharadar_fake import sample, sep_row

from agb import heartbeat, jobs, lake, schedule
from agb.schedule import Expected, Planned
from agb.services.pipeline import RETRY, Pipeline, record

PERIOD = "2026-09-14"
SESSION = date(2026, 9, 14)
KEY = "k3y-not-a-real-one"
MINUTE = timedelta(minutes=1)


def plan_pull(db, period=PERIOD, *, start=None):
    """The scheduler's data_pull job for `period`, claimable from `start`. Returns when its
    expected event is due."""
    start = start or datetime.now(UTC) - timedelta(hours=1)
    due = start + timedelta(hours=15)
    job = Planned(
        schedule.DATA_PULL,
        period,
        date.fromisoformat(period),
        start,
        (Expected(schedule.DATA_PULL, period, due, True),),
    )
    schedule.write(db.connect("scheduler"), [job])
    return due


def late_pulls(db, at):
    return (
        db.connect("cli")
        .execute("SELECT period FROM agb.overdue_at(%s) WHERE kind = 'data_pull'", (at,))
        .fetchall()
    )


def pull_state(db):
    return (
        db.connect("cli")
        .execute("SELECT state FROM agb.job_state WHERE kind = 'data_pull'")
        .fetchone()[0]
    )


def snapshots(db):
    return (
        db.connect("cli")
        .execute("""
            SELECT snapshot_date, dataset, partition, sha256, row_count, job_id, actor
            FROM agb.lake_snapshot ORDER BY id
        """)
        .fetchall()
    )


def landed(root):
    return sorted(str(p.parent.relative_to(root)) for p in root.rglob(lake.FILE))


def mtimes(root):
    return {p: p.stat().st_mtime_ns for p in root.rglob("*") if p.is_file()}


def test_a_pull_lands_the_session_records_it_and_fulfils_it(db, tmp_path):
    due = plan_pull(db)

    Pipeline(tmp_path, KEY, opener=sample())(db.connect("pipeline"))

    assert pull_state(db) == "completed"
    assert late_pulls(db, due + MINUTE) == []
    [(job_id,)] = db.connect("pipeline").execute("SELECT id FROM agb.job").fetchall()
    rows = snapshots(db)
    assert [(r[1], r[2], r[4]) for r in rows] == [
        ("sep", "sep/date=2026-09-14", 2),
        ("sf1", "sf1/lastupdated=2026-09-08", 1),
        ("sf1", "sf1/lastupdated=2026-09-14", 1),
        ("tickers", "tickers/snapshot=2026-09-14", 6),
    ]
    for snapshot_date, _, partition, sha256, _, job, actor in rows:
        assert (snapshot_date, job, actor) == (SESSION, job_id, "pipeline")
        assert sha256 == lake.file_hash(tmp_path / partition / lake.FILE)


def test_rerunning_a_landed_pull_changes_nothing(db, tmp_path):
    vendor = sample()
    plan_pull(db)
    conn = db.connect("pipeline")
    Pipeline(tmp_path, KEY, opener=vendor)(conn)
    before, files = snapshots(db), mtimes(tmp_path)

    partitions = lake.pull(tmp_path, lake.Sharadar(KEY, vendor), SESSION)

    assert record(conn, SESSION, partitions, None) == 0
    assert snapshots(db) == before
    assert mtimes(tmp_path) == files


def test_a_revised_partition_is_recorded_beside_the_first(db, tmp_path):
    vendor = sample()
    plan_pull(db)
    conn = db.connect("pipeline")
    Pipeline(tmp_path, KEY, opener=vendor)(conn)
    vendor.rows["stocks"][-1] = sep_row("MSFT", "2026-09-14", "512.4", lastupdated="2026-09-15")

    assert (
        record(conn, SESSION, lake.pull(tmp_path, lake.Sharadar(KEY, vendor), SESSION), None) == 1
    )

    sep = [r for r in snapshots(db) if r[1] == "sep"]
    assert len({r[3] for r in sep}) == 2
    assert sep[-1][3] == lake.file_hash(tmp_path / "sep/date=2026-09-14" / lake.FILE)


def fetch_fails(vendor):
    vendor.fail = lambda url: urllib.error.URLError(f"timed out fetching {url}")
    return KEY


def no_key(vendor):
    return ""


@pytest.mark.parametrize(
    ("setup", "detail"),
    [
        (fetch_fails, "timed out fetching"),
        (no_key, "SHARADAR_API_KEY is not set"),
    ],
    ids=["fetch-fails", "no-key"],
)
def test_a_failed_pull_lands_nothing_and_is_overdue(db, tmp_path, setup, detail):
    vendor = sample()
    key = setup(vendor)
    due = plan_pull(db)

    Pipeline(tmp_path, key, opener=vendor)(db.connect("pipeline"))

    assert pull_state(db) == "failed"
    [(failure,)] = (
        db.connect("cli")
        .execute("SELECT detail FROM agb.job_event WHERE event = 'failed'")
        .fetchall()
    )
    assert detail in failure
    assert KEY not in failure
    assert landed(tmp_path) == []
    assert snapshots(db) == []
    assert late_pulls(db, due - MINUTE) == []
    assert late_pulls(db, due + MINUTE) == [(PERIOD,)]


def test_a_pull_that_never_runs_is_overdue(db):
    due = plan_pull(db)
    assert late_pulls(db, due - MINUTE) == []
    assert late_pulls(db, due + MINUTE) == [(PERIOD,)]


def test_a_session_not_yet_published_is_kept_and_tried_again(db, tmp_path):
    vendor = sample()
    published = vendor.rows["stocks"][-2:]
    del vendor.rows["stocks"][-2:]
    due = plan_pull(db)
    conn = db.connect("pipeline")

    Pipeline(tmp_path, KEY, opener=vendor)(conn)

    assert pull_state(db) == "claimed"
    [(left,)] = db.connect("cli").execute("SELECT lease_until - now() FROM agb.job").fetchall()
    assert timedelta(0) < left <= RETRY
    assert landed(tmp_path) == []
    assert db.connect("cli").execute(
        "SELECT count(*) FROM agb.job_event WHERE event = 'failed'"
    ).fetchone() == (0,)
    assert late_pulls(db, due + MINUTE) == [(PERIOD,)]  # until it lands
    vendor.rows["stocks"] += published
    Pipeline(tmp_path, KEY, opener=vendor)(conn)
    assert len(vendor.urls) == 1  # not before the retry is due


def test_a_session_not_yet_published_lands_once_the_vendor_has_it(db, tmp_path):
    vendor = sample()
    published = vendor.rows["stocks"][-2:]
    del vendor.rows["stocks"][-2:]
    due = plan_pull(db)
    conn = db.connect("pipeline")
    run = Pipeline(tmp_path, KEY, opener=vendor, retry=timedelta(milliseconds=1))

    run(conn)
    time.sleep(0.05)
    run(conn)
    assert len(vendor.urls) == 2
    assert pull_state(db) == "lease_expired"
    vendor.rows["stocks"] += published
    time.sleep(0.05)
    run(conn)

    assert pull_state(db) == "completed"
    assert "sep/date=2026-09-14" in landed(tmp_path)
    assert late_pulls(db, due + MINUTE) == []


def test_nothing_is_pulled_before_the_job_is_due(db, tmp_path):
    vendor = sample()
    plan_pull(db, start=datetime.now(UTC) + timedelta(hours=1))
    Pipeline(tmp_path, KEY, opener=vendor)(db.connect("pipeline"))
    assert vendor.urls == []
    assert landed(tmp_path) == []


def test_a_pull_whose_lease_expired_is_redone(db, tmp_path):
    plan_pull(db)
    conn = db.connect("pipeline")
    assert jobs.claim(conn, [schedule.DATA_PULL], "pipeline", timedelta(milliseconds=1))
    time.sleep(0.05)

    Pipeline(tmp_path, KEY, opener=sample())(conn)

    assert pull_state(db) == "completed"
    assert "sep/date=2026-09-14" in landed(tmp_path)


def test_the_service_beats_its_heartbeat_and_pulls(db, tmp_path, monkeypatch):
    for key, value in db.pg_env("pipeline").items():
        monkeypatch.setenv(key, value)
    plan_pull(db)
    stop = threading.Event()
    run = Pipeline(tmp_path, KEY, opener=sample())

    def cycle(conn):
        run(conn)
        stop.set()

    heartbeat.serve("pipeline", 0.01, stop, cycle)

    services = db.connect("cli").execute("SELECT service FROM agb.heartbeat").fetchall()
    assert services == [("pipeline",)]
    assert pull_state(db) == "completed"


def test_the_backfill_lands_history_from_1998_and_records_it(db, tmp_path):
    conn = db.connect("pipeline")
    pipeline = Pipeline(tmp_path, KEY, opener=sample())

    pipeline.backfill(conn, snapshot=SESSION)

    assert landed(tmp_path) == [
        "sep/date=1998-01-02",
        "sep/date=2008-09-12",
        "sep/date=2026-09-11",
        "sep/date=2026-09-14",
        "sf1/lastupdated=2019-03-01",
        "sf1/lastupdated=2025-11-03",
        "sf1/lastupdated=2026-09-08",
        "sf1/lastupdated=2026-09-14",
        "tickers/snapshot=2026-09-14",
    ]
    assert not (tmp_path / ".backfill").exists()
    rows = snapshots(db)
    assert sorted(r[2] for r in rows) == landed(tmp_path)
    assert {(r[0], r[5]) for r in rows} == {(SESSION, None)}
    history = lake.connect(tmp_path).sql(
        "SELECT DISTINCT ticker FROM sep WHERE date < '2009-01-01' ORDER BY ticker"
    )
    assert history.fetchall() == [("AAPL",), ("LEHMQ",)]
    delisted = lake.connect(tmp_path).sql(
        "SELECT DISTINCT ticker FROM tickers WHERE isdelisted = 'Y'"
    )
    assert delisted.fetchall() == [("LEHMQ",)]

    files = mtimes(tmp_path)
    pipeline.backfill(conn, snapshot=SESSION)
    assert snapshots(db) == rows
    assert mtimes(tmp_path) == files


def test_snapshots_are_append_only(db, tmp_path):
    plan_pull(db)
    Pipeline(tmp_path, KEY, opener=sample())(db.connect("pipeline"))
    for statement in (
        "UPDATE agb.lake_snapshot SET row_count = 0",
        "DELETE FROM agb.lake_snapshot",
    ):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect("pipeline").execute(statement)
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("scheduler").execute(
            "INSERT INTO agb.lake_snapshot (snapshot_date, dataset, partition, sha256, row_count,"
            " actor) VALUES (current_date, 'sep', 'sep/date=2026-09-14', repeat('0', 64), 0,"
            " 'scheduler')"
        )
