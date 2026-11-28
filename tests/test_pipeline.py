"""The pipeline service's data pull: a landed session completes its job and records its
snapshot, a failed or missed pull is overdue, a re-run changes nothing, and the backfill. Then
what any job does when it breaks: it fails once, naming its cause, with an alert, and the
service carries on; a job whose lease was lost is left alone."""

import errno
import logging
import re
import threading
import time
import urllib.error
from datetime import UTC, date, datetime, timedelta

import duckdb
import psycopg
import pyarrow as pa
import pytest
import registry_fake
from account_fake import read_book
from market_fake import SIGNAL, TRADE, Market
from panel_fake import FakePanel
from psycopg import errors
from sharadar_fake import sample, sep_row, status
from test_approval_loop import NEXT_MORNING, cli_main
from test_basket import CAPITAL, EVENING, LIMITS, job_state, plan_rebalance
from test_basket import rows as cli_rows
from test_basket import run as rebalance
from test_blend import fake_panel, market
from test_gate import LATE
from test_jobs import age

from agb import analyst, approval, edgar, heartbeat, jobs, lake, schedule
from agb.errors import NotConfigured
from agb.lake import KeyNotSet, LakeError
from agb.panel_store import PanelStore
from agb.schedule import Expected, Planned
from agb.services import pipeline as pipeline_module
from agb.services.pipeline import (
    CONFIGURATION,
    JOB_FAILED,
    JOB_HELD,
    PERMANENT,
    RERUN_FAILED,
    RETRY,
    TRANSIENT,
    TRANSIENT_RETRIES,
    Pipeline,
    classify,
    record,
)
from agb.services.scheduler import Scheduler

PERIOD = "2026-09-14"
SESSION = date(2026, 9, 14)
KEY = "k3y-not-a-real-one"
MINUTE = timedelta(minutes=1)
# The day after PERIOD, while its pull is still worth doing: the job expires when the next
# session's pull is due, at 2026-09-15's close plus schedule.PULL_DELAY.
DAY_AFTER = datetime(2026, 9, 15, 12, tzinfo=UTC)
PULL_EXPIRES = datetime(2026, 9, 15, 22, tzinfo=UTC)


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


def no_wait(seconds):
    """A sleep for the lake client's retries that does not wait."""


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


def test_ingestion_continues_while_paused(db, tmp_path):
    """A pause stops orders, not data: the pull lands with the pause cause open."""
    db.connect("bot").execute(
        "INSERT INTO agb.halt_event (cause, action, actor, reason)"
        " VALUES ('pause', 'set', 'telegram:77', '/pause')"
    )
    plan_pull(db)

    Pipeline(tmp_path, KEY, opener=sample())(db.connect("pipeline"))

    assert db.connect("cli").execute("SELECT cause FROM agb.halt_open").fetchall() == [("pause",)]
    assert pull_state(db) == "completed"
    assert "sep/date=2026-09-14" in landed(tmp_path)


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

    # Past the pull's deadline, so a held job (no key) fails as well.
    run = Pipeline(tmp_path, key, opener=vendor, sleep=no_wait, clock=lambda: PULL_EXPIRES)
    run(db.connect("pipeline"))

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


def test_a_vendor_error_then_an_answer_completes_the_pull(db, tmp_path):
    vendor = sample()
    vendor.errors = [status(503)]
    waits = []
    due = plan_pull(db)

    Pipeline(tmp_path, KEY, opener=vendor, sleep=waits.append)(db.connect("pipeline"))

    assert waits == [lake.BACKOFF]
    assert pull_state(db) == "completed"
    assert "sep/date=2026-09-14" in landed(tmp_path)
    assert late_pulls(db, due + MINUTE) == []
    assert alerts(db) == []


def test_a_vendor_error_past_every_retry_fails_the_pull_with_an_alert(db, tmp_path):
    vendor = sample()
    vendor.fail = lambda url: status(503)
    plan_pull(db)

    Pipeline(tmp_path, KEY, opener=vendor, sleep=no_wait)(db.connect("pipeline"))

    assert pull_state(db) == "failed"
    assert len(vendor.urls) == lake.ATTEMPTS
    [(kind, subject, period, body, raised_by)] = alerts(db)
    assert (kind, subject, period, raised_by) == (JOB_FAILED, "data_pull", PERIOD, "pipeline")
    assert f"failed after {lake.ATTEMPTS} attempts" in body
    assert KEY not in body
    assert landed(tmp_path) == []


def test_a_deferred_pull_records_the_missed_sessions_it_landed(db, tmp_path):
    # The lake has the 10th; the vendor has the 11th, and only AAPL of the 14th so far.
    vendor = sample()
    vendor.rows["stocks"] = [r for r in vendor.rows["stocks"] if r["date"] != "2026-09-14"]
    vendor.rows["stocks"] += [
        sep_row("AAPL", "2026-09-10", "320"),
        sep_row("MSFT", "2026-09-10", "500"),
        sep_row("AAPL", "2026-09-14", "328.21"),
    ]
    lake.pull(tmp_path, lake.Sharadar(KEY, vendor), date(2026, 9, 10))
    due = plan_pull(db)

    Pipeline(tmp_path, KEY, opener=vendor, clock=lambda: DAY_AFTER)(db.connect("pipeline"))

    assert pull_state(db) == "claimed"
    assert [(r[0], r[2], r[4]) for r in snapshots(db)] == [(SESSION, "sep/date=2026-09-11", 2)]
    assert "sep/date=2026-09-14" not in landed(tmp_path)
    assert alerts(db) == []
    assert late_pulls(db, due + MINUTE) == [(PERIOD,)]  # until the 14th lands


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

    Pipeline(tmp_path, KEY, opener=vendor, clock=lambda: DAY_AFTER)(conn)

    assert pull_state(db) == "claimed"
    [(left,)] = db.connect("cli").execute("SELECT lease_until - now() FROM agb.job").fetchall()
    assert timedelta(0) < left <= RETRY
    assert landed(tmp_path) == []
    assert db.connect("cli").execute(
        "SELECT count(*) FROM agb.job_event WHERE event = 'failed'"
    ).fetchone() == (0,)
    assert late_pulls(db, due + MINUTE) == [(PERIOD,)]  # until it lands
    vendor.rows["stocks"] += published
    Pipeline(tmp_path, KEY, opener=vendor, clock=lambda: DAY_AFTER)(conn)
    assert len(vendor.urls) == 1  # not before the retry is due


def test_a_session_not_yet_published_lands_once_the_vendor_has_it(db, tmp_path):
    vendor = sample()
    published = vendor.rows["stocks"][-2:]
    del vendor.rows["stocks"][-2:]
    due = plan_pull(db)
    conn = db.connect("pipeline")
    run = Pipeline(
        tmp_path, KEY, opener=vendor, retry=timedelta(milliseconds=1), clock=lambda: DAY_AFTER
    )

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


def failed_details(db):
    return [
        detail
        for (detail,) in cli_rows(db, "SELECT detail FROM agb.job_event WHERE event = 'failed'")
    ]


def job_id(db):
    [(found,)] = cli_rows(db, "SELECT id FROM agb.job")
    return found


def test_a_pull_past_its_deadline_and_not_yet_published_fails_once_with_an_alert(db, tmp_path):
    vendor = sample()
    del vendor.rows["stocks"][-2:]
    plan_pull(db)
    conn = db.connect("pipeline")

    Pipeline(tmp_path, KEY, opener=vendor, clock=lambda: DAY_AFTER)(conn)
    assert pull_state(db) == "claimed"  # held, while its data can still be used
    age(db, job_id(db), RETRY)
    Pipeline(tmp_path, KEY, opener=vendor, clock=lambda: PULL_EXPIRES)(conn)

    assert pull_state(db) == "failed"
    [detail] = failed_details(db)
    assert detail.startswith(
        "expired: its work could be used only until 2026-09-15 22:00 UTC; not yet published: "
    )
    [(kind, subject, period, body, raised_by)] = alerts(db)
    assert (kind, subject, period, raised_by) == (JOB_FAILED, "data_pull", PERIOD, "pipeline")
    assert detail in body

    # Never renewed again: the next cycle takes nothing and asks the vendor nothing.
    asked = len(vendor.urls)
    Pipeline(tmp_path, KEY, opener=vendor, clock=lambda: PULL_EXPIRES)(conn)
    assert len(vendor.urls) == asked
    assert len(failed_details(db)) == 1
    assert len(alerts(db)) == 1


def raises_first(monkeypatch, failures):
    """Make lake.pull raise each of `failures` in turn, then pull as it would."""
    real = lake.pull

    def pull(*args, **kwargs):
        if failures:
            raise failures.pop(0)
        return real(*args, **kwargs)

    monkeypatch.setattr(lake, "pull", pull)


def disk_error():
    return OSError(errno.EIO, "Input/output error")


def test_the_retries_for_a_transient_error_are_bounded():
    assert TRANSIENT_RETRIES == 3


def test_a_transient_error_releases_the_pull_and_a_later_cycle_lands_it(
    db, tmp_path, monkeypatch, caplog
):
    raises_first(monkeypatch, [disk_error()])
    due = plan_pull(db)
    run = Pipeline(tmp_path, KEY, opener=sample(), clock=lambda: DAY_AFTER)
    conn = db.connect("pipeline")

    with caplog.at_level(logging.WARNING, logger=pipeline_module.__name__):
        run(conn)

    assert pull_state(db) == "pending"
    assert cli_rows(db, "SELECT event, detail FROM agb.job_event WHERE event = 'released'") == [
        ("released", "transient: OSError: [Errno 5] Input/output error")
    ]
    assert "released to be tried again (1 of 3)" in caplog.text
    run(conn)

    assert pull_state(db) == "completed"
    assert "sep/date=2026-09-14" in landed(tmp_path)
    assert late_pulls(db, due + MINUTE) == []
    assert alerts(db) == []


def test_a_transient_error_past_every_retry_fails_the_pull_with_an_alert(db, tmp_path, monkeypatch):
    raises_first(monkeypatch, [disk_error() for _ in range(TRANSIENT_RETRIES + 1)])
    plan_pull(db)
    run = Pipeline(tmp_path, KEY, opener=sample(), clock=lambda: DAY_AFTER)
    conn = db.connect("pipeline")

    for _ in range(TRANSIENT_RETRIES):
        run(conn)
        assert pull_state(db) == "pending"
    run(conn)

    assert pull_state(db) == "failed"
    assert failed_details(db) == [
        "transient, still after 3 retries: OSError: [Errno 5] Input/output error"
    ]
    [(kind, subject, period, _, _)] = alerts(db)
    assert (kind, subject, period) == (JOB_FAILED, "data_pull", PERIOD)
    run(conn)
    assert len(alerts(db)) == 1


def test_an_os_error_that_trying_again_does_not_mend_fails_at_once(db, tmp_path, monkeypatch):
    raises_first(monkeypatch, [PermissionError(errno.EACCES, "Permission denied")])
    plan_pull(db)

    Pipeline(tmp_path, KEY, opener=sample(), clock=lambda: DAY_AFTER)(db.connect("pipeline"))

    assert pull_state(db) == "failed"
    assert failed_details(db) == ["permanent: PermissionError: [Errno 13] Permission denied"]


def test_a_pull_with_no_key_is_held_with_one_alert_until_its_deadline(db, tmp_path):
    plan_pull(db)
    conn = db.connect("pipeline")

    for _ in range(2):
        Pipeline(tmp_path, "", opener=sample(), clock=lambda: DAY_AFTER)(conn)
        assert pull_state(db) == "claimed"
        age(db, job_id(db), RETRY)
    [(kind, subject, period, body, _)] = alerts(db)
    assert (kind, subject, period) == (JOB_HELD, "data_pull", PERIOD)
    assert "KeyNotSet: SHARADAR_API_KEY is not set" in body
    assert failed_details(db) == []

    Pipeline(tmp_path, "", opener=sample(), clock=lambda: PULL_EXPIRES)(conn)

    assert pull_state(db) == "failed"
    [detail] = failed_details(db)
    assert detail.startswith("expired: ")
    assert "held for missing configuration: KeyNotSet: SHARADAR_API_KEY is not set" in detail
    assert [a[0] for a in alerts(db)] == [JOB_HELD, JOB_FAILED]


@pytest.mark.parametrize(
    ("exc", "failure"),
    [
        (OSError(errno.EIO, "Input/output error"), TRANSIENT),
        (ConnectionResetError(), TRANSIENT),
        (TimeoutError(), TRANSIENT),
        (psycopg.OperationalError("could not obtain lock"), TRANSIENT),
        (duckdb.IOException("could not set lock on file"), TRANSIENT),
        (KeyNotSet("SHARADAR_API_KEY is not set"), CONFIGURATION),
        (NotConfigured("EDGAR_USER_AGENT is not set"), CONFIGURATION),
        (FileNotFoundError(errno.ENOENT, "No such file"), PERMANENT),
        (PermissionError(errno.EACCES, "Permission denied"), PERMANENT),
        (LakeError("GET SEP failed after 3 attempts"), PERMANENT),
        (errors.CheckViolation("basket_cutoff"), PERMANENT),
        (pa.ArrowInvalid("bad cast"), PERMANENT),
        (ValueError("bad data"), PERMANENT),
    ],
    ids=lambda v: type(v).__name__ if isinstance(v, BaseException) else v,
)
def test_each_error_is_classified_by_its_type(exc, failure):
    assert classify(exc) == failure


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


def test_the_backfill_lands_the_benchmark_and_records_it(db, tmp_path):
    vendor = sample()
    vendor.rows["funds"] += [
        sep_row("SPY", day, "500") for day in ("1997-12-31", "2026-09-11", "2026-09-14")
    ]
    conn = db.connect("pipeline")

    Pipeline(tmp_path, KEY, opener=vendor).backfill(conn, snapshot=SESSION)

    sfp = ["sfp/date=2026-09-11", "sfp/date=2026-09-14"]
    assert [p for p in landed(tmp_path) if p.startswith("sfp/")] == sfp
    assert [r[2] for r in snapshots(db) if r[2].startswith("sfp/")] == sfp
    assert lake.connect(tmp_path).sql("SELECT DISTINCT ticker FROM sfp").fetchall() == [("SPY",)]


def test_a_backfill_stopped_between_a_write_and_its_record_lands_again(db, tmp_path, monkeypatch):
    # The ledger fails on the third partition, its bytes written but not yet recorded: that
    # partition does not land, every file that did is recorded, the lake reads, and the backfill
    # run again lands and records the rest.
    conn = db.connect("pipeline")
    pipeline = Pipeline(tmp_path, KEY, opener=sample())
    real, calls = pipeline_module.record, 0

    def fails_on_the_third(*args):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise psycopg.OperationalError("the server closed the connection")
        return real(*args)

    monkeypatch.setattr(pipeline_module, "record", fails_on_the_third)
    with pytest.raises(psycopg.OperationalError):
        pipeline.backfill(conn, snapshot=SESSION)

    assert landed(tmp_path) == ["sep/date=1998-01-02", "sep/date=2008-09-12"]
    assert sorted(r[2] for r in snapshots(db)) == landed(tmp_path)
    recorded_rows = sum(r[4] for r in snapshots(db))
    assert lake.connect(tmp_path).sql("SELECT count(*) FROM sep").fetchone() == (recorded_rows,)
    assert not (tmp_path / ".backfill").exists()

    pipeline.backfill(conn, snapshot=SESSION)

    assert len(landed(tmp_path)) == 9
    assert sorted(r[2] for r in snapshots(db)) == landed(tmp_path)


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


class Breaks(FakePanel):
    """A panel that breaks partway through its pass the way `breaking` does, after writing its
    run and its first names as the real one does."""

    def __init__(self, config, breaking, **kwargs):
        super().__init__(config, **kwargs)
        self.breaking = breaking

    def __call__(self, conn, shortlist, sectors, as_of, job_id=None):
        super().__call__(conn, shortlist[:3], sectors, as_of, job_id)
        self.breaking(conn)


def corrupt_body(tmp_path):
    """Reading back a body the store holds that has gone bad on disk."""
    store = PanelStore(tmp_path / "panel")
    digest = store.put(b'{"text": "the Analyst\'s extract"}')
    store.path(digest).write_bytes(b"not zstd")
    return lambda conn: store.get(digest)


def database_error(tmp_path):
    return lambda conn: conn.execute("SELECT 1 / 0")


def unreadable_parquet(tmp_path):
    return lambda conn: pa.array(["N01"]).cast(pa.int64())


def alerts(db):
    return cli_rows(db, "SELECT kind, subject, period, body, raised_by FROM agb.alert")


@pytest.mark.parametrize(
    ("breaking", "cause"),
    [
        (corrupt_body, "permanent: StoreError: body .* does not decompress"),
        (database_error, "permanent: DivisionByZero: division by zero"),
        (unreadable_parquet, "permanent: ArrowInvalid: "),
    ],
    ids=["corrupt-body", "database", "arrow"],
)
def test_a_job_that_breaks_fails_once_with_an_alert_and_the_service_carries_on(
    db, tmp_path, breaking, cause
):
    plan_rebalance(db)
    root = market(tmp_path).land()
    panel = Breaks(fake_panel(db, 0.2).config_id, breaking(tmp_path), shortlist_n=25)

    rebalance(db, root, runner=panel)

    assert job_state(db) == "failed"
    [(detail,)] = cli_rows(db, "SELECT detail FROM agb.job_event WHERE event = 'failed'")
    assert re.match(cause, detail)
    [(kind, subject, period, body, raised_by)] = alerts(db)
    assert (kind, subject, period, raised_by) == (JOB_FAILED, "rebalance", "2026-10", "pipeline")
    assert "will not be tried again" in body and detail in body
    assert cli_rows(db, "SELECT count(*) FROM agb.basket") == [(0,)]

    # The next cycle runs: the failed job is not taken again, so nothing is asked of the
    # panel and the alert is not raised a second time.
    rebalance(db, root, runner=panel)

    assert len(panel.calls) == 1
    assert job_state(db) == "failed"
    assert len(alerts(db)) == 1


def test_a_dropped_database_connection_still_stops_the_service(db, tmp_path):
    plan_rebalance(db)
    root = market(tmp_path).land()

    def drop(conn):
        # The connection goes away under the pass, as it does when Postgres restarts.
        conn.close()
        conn.execute("SELECT 1")

    panel = Breaks(fake_panel(db, 0.2).config_id, drop, shortlist_n=25)

    with pytest.raises(psycopg.OperationalError):
        rebalance(db, root, runner=panel)

    assert alerts(db) == []


def test_a_lost_lease_ends_the_job_without_failing_it(db, tmp_path, caplog):
    plan_rebalance(db)
    root = market(tmp_path).land()
    lost = []

    def lose_lease(conn):
        # The lease runs out mid-pass and is given up, as recovery does for a stuck job, so
        # the job is no longer this worker's to complete.
        [(job_id,)] = cli_rows(db, "SELECT id FROM agb.job WHERE kind = 'rebalance'")
        age(db, job_id, timedelta(hours=2))
        assert jobs.release_expired(conn, job_id, "pipeline", "lease ran out")
        lost.append(job_id)

    panel = Breaks(fake_panel(db, 0.2).config_id, lose_lease, shortlist_n=25)

    with caplog.at_level(logging.ERROR, logger="agb.services.pipeline"):
        rebalance(db, root, runner=panel)

    assert len(lost) == 1
    [record] = [r for r in caplog.records if "lease lost" in r.getMessage()]
    assert record.exc_info is None
    assert "rebalance 2026-10" in record.getMessage()
    assert job_state(db) != "failed"
    assert cli_rows(db, "SELECT count(*) FROM agb.job_event WHERE event = 'failed'") == [(0,)]
    assert cli_rows(db, "SELECT count(*) FROM agb.job_event WHERE event = 'completed'") == [(0,)]
    assert alerts(db) == []


def test_a_rebalance_renews_the_lease_between_its_steps(db, tmp_path):
    plan_rebalance(db)
    root = market(tmp_path).land()
    seen = []

    def pass_time(conn):
        # Most of a lease goes by inside the panel; the rebalance must have the rest of it.
        [(job_id,)] = cli_rows(db, "SELECT id FROM agb.job WHERE kind = 'rebalance'")
        age(db, job_id, timedelta(minutes=50))
        seen.append(job_id)

    panel = Breaks(fake_panel(db, 0.2).config_id, pass_time, shortlist_n=25)
    rebalance(db, root, runner=panel, lease=timedelta(hours=1))

    assert len(seen) == 1
    [(lease_until,)] = cli_rows(db, "SELECT lease_until FROM agb.job WHERE id = %s", seen[0])
    assert lease_until is None or lease_until > datetime.now(UTC) - timedelta(minutes=1)


class Panel:
    """What `panel.Runner` is built with, kept for the test to drive."""

    def __init__(self, config, client, model, store, beat):
        self.beat = beat


def real_panel(db, monkeypatch):
    """A pipeline whose `stage2` builds its panel as in production, from a configuration the
    current prompts agree with, with the network and the model left out."""
    fake_panel(db, 0.2)
    monkeypatch.setattr(edgar.Edgar, "from_env", staticmethod(lambda: None))
    monkeypatch.setattr(analyst, "AnthropicModel", lambda: None)
    monkeypatch.setattr(pipeline_module.panel, "Runner", Panel)
    return Pipeline()


def test_the_panel_run_renews_the_lease_past_its_duration(db, monkeypatch):
    plan_rebalance(db)
    conn, rival = db.connect("pipeline"), db.connect("pipeline")
    service = real_panel(db, monkeypatch)
    claim = jobs.claim(conn, [schedule.REBALANCE], service.worker, service.lease)
    beat = service.stage2(conn, claim).beat

    # Three quarters of the lease at a time, three times over: longer than the lease in all.
    for _ in range(3):
        age(db, claim.job_id, service.lease * 3 // 4)
        beat()

    assert (
        jobs.claim(
            rival, [schedule.REBALANCE], "pipeline-other", service.lease, reclaim_expired=True
        )
        is None
    )
    jobs.complete(conn, claim)
    assert job_state(db) == "completed"


def test_the_panel_run_stops_when_its_lease_is_lost(db, monkeypatch):
    plan_rebalance(db)
    conn, rival = db.connect("pipeline"), db.connect("pipeline")
    service = real_panel(db, monkeypatch)
    claim = jobs.claim(conn, [schedule.REBALANCE], service.worker, service.lease)
    beat = service.stage2(conn, claim).beat
    age(db, claim.job_id, service.lease * 2)
    taken = jobs.claim(
        rival, [schedule.REBALANCE], "pipeline-other", service.lease, reclaim_expired=True
    )

    with pytest.raises(jobs.LeaseLost):
        beat()
    with pytest.raises(jobs.LeaseLost):
        jobs.complete(conn, claim)
    assert cli_rows(db, "SELECT claimed_by FROM agb.job WHERE id = %s", claim.job_id) == [
        (taken.worker,)
    ]


def test_each_pipeline_claims_under_a_name_of_its_own(db, tmp_path):
    plan_pull(db)
    first, second = Pipeline(tmp_path, KEY), Pipeline(tmp_path, KEY)
    assert first.worker != second.worker
    assert first.worker.startswith("pipeline-")
    assert Pipeline(tmp_path, KEY, worker="pipeline-fixed").worker == "pipeline-fixed"

    Pipeline(tmp_path, KEY, opener=sample(), worker="pipeline-fixed")(db.connect("pipeline"))

    assert cli_rows(
        db, "SELECT actor FROM agb.job_event WHERE event IN ('claimed', 'completed') ORDER BY id"
    ) == [("pipeline-fixed",), ("pipeline-fixed",)]


# The basket cutoff in the database (issue #155): a gate that would approve a basket at or
# after its cutoff is refused with a CheckViolation, which the pipeline handles like any other
# database error. The tests pin the database's clock through agb.test_clock.


def run_at(db, root, at, **kwargs):
    """One pipeline cycle, on a connection whose database clock reads `at`, with the vendor
    faked for any pull it takes."""
    conn = db.connect("pipeline")
    conn.execute("SELECT set_config('agb.test_clock', %s, false)", (at.isoformat(),))
    kwargs = {
        "limits": LIMITS,
        "clock": lambda: EVENING,
        "capital": CAPITAL,
        "opener": sample(),
        **kwargs,
    }
    Pipeline(root, KEY, ranker=registry_fake.ranker(db), **kwargs)(conn)


def test_a_rebalance_gated_past_its_cutoff_fails_once_with_an_alert(db, tmp_path):
    plan_rebalance(db)
    root = market(tmp_path).land()
    read_book(db, SIGNAL)

    run_at(db, root, LATE)

    assert job_state(db) == "failed"
    [(detail,)] = cli_rows(db, "SELECT detail FROM agb.job_event WHERE event = 'failed'")
    assert detail.startswith(
        "permanent: CheckViolation: basket_cutoff: pipeline may not approve basket"
    )
    [(kind, subject, period, body, _)] = alerts(db)
    assert (kind, subject, period) == (JOB_FAILED, "rebalance", "2026-10")
    assert detail in body
    # The gate's whole run was rolled back with the approval it would have written.
    [(basket_id, state)] = cli_rows(db, "SELECT id, state FROM agb.basket_state")
    assert state == "proposed"
    assert cli_rows(db, "SELECT count(*) FROM agb.gate_run") == [(0,)]

    run_at(db, root, LATE)

    assert job_state(db) == "failed"
    assert len(alerts(db)) == 1


def failed_reruns(db):
    return cli_rows(
        db,
        "SELECT subject, period, body, raised_by FROM agb.alert WHERE kind = %s",
        RERUN_FAILED,
    )


def test_a_rerun_gated_past_its_cutoff_alerts_once_and_the_cycle_carries_on(
    db, tmp_path, monkeypatch, capsys, caplog
):
    plan_rebalance(db)
    approval.set_paper_approval(db.connect("cli"), True, "operator", "rehearse the loop")
    root = Market(tmp_path).names(25).land()
    rebalance(db, root)
    Scheduler(lambda: LATE)(db.connect("scheduler"))
    code, _, _ = cli_main(db, monkeypatch, capsys, "rerun", "2026-10", "--actor", "alex")
    assert code == 0
    # Paper mode runs unattended again, so the gate would approve the rerun itself.
    approval.set_paper_approval(db.connect("cli"), False, "operator", "done rehearsing")
    claimed = "SELECT count(*) FROM agb.job_event WHERE event = 'claimed'"
    [(before,)] = cli_rows(db, claimed)
    rerun_cutoff = datetime(2026, 10, 2, 19, 45, tzinfo=UTC)
    read_book(db, TRADE)

    with caplog.at_level(logging.ERROR, logger="agb.services.pipeline"):
        run_at(db, root, rerun_cutoff, clock=lambda: NEXT_MORNING)

    [record] = [r for r in caplog.records if r.getMessage().startswith("rerun 2026-10r1")]
    assert "CheckViolation: basket_cutoff: pipeline may not approve" in record.getMessage()
    [(subject, period, body, raised_by)] = failed_reruns(db)
    assert (subject, period, raised_by) == ("2026-10r1", "2026-10-02", "pipeline")
    assert "asked for by alex" in body and "CheckViolation: basket_cutoff" in body
    assert cli_rows(db, "SELECT reference FROM agb.rerun_pending") == [("2026-10r1",)]
    [(rerun, state)] = cli_rows(
        db,
        "SELECT b.id, s.state FROM agb.basket b JOIN agb.basket_state s ON s.id = b.id"
        " WHERE b.reference = '2026-10r1'",
    )
    assert state == "proposed"
    # The cycle went on to take the next job (the scheduler planned the month's data pulls).
    assert cli_rows(db, claimed) == [(before + 1,)]

    # Tried again the next cycle, still refused, and the alert is not raised a second time.
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="agb.services.pipeline"):
        run_at(db, root, rerun_cutoff, clock=lambda: NEXT_MORNING)
    assert [r for r in caplog.records if r.getMessage().startswith("rerun 2026-10r1")]
    assert len(failed_reruns(db)) == 1

    # The sweep retires the ungated rerun at its cutoff, with its own alert, and the pipeline
    # stops trying it: the gate would refuse a basket no longer proposed every cycle.
    Scheduler(lambda: rerun_cutoff)(db.connect("scheduler"))
    assert cli_rows(db, "SELECT state FROM agb.basket_state WHERE id = %s", rerun) == [("missed",)]
    raised = len(alerts(db))
    caplog.clear()
    with caplog.at_level(logging.ERROR, logger="agb.services.pipeline"):
        run_at(db, root, rerun_cutoff, clock=lambda: NEXT_MORNING)
    assert caplog.records == []
    assert len(alerts(db)) == raised
    assert cli_rows(db, "SELECT count(*) FROM agb.gate_run WHERE basket_id = %s", rerun) == [(0,)]
