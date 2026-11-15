"""The pipeline service's data pull: a landed session completes its job and records its
snapshot, a failed or missed pull is overdue, a re-run changes nothing, and the backfill. Then
what any job does when it breaks: it fails once, naming its cause, with an alert, and the
service carries on; a job whose lease was lost is left alone."""

import logging
import re
import threading
import time
import urllib.error
from datetime import UTC, date, datetime, timedelta

import psycopg
import pyarrow as pa
import pytest
import registry_fake
from market_fake import Market
from panel_fake import FakePanel
from psycopg import errors
from sharadar_fake import sample, sep_row, status
from test_approval_loop import NEXT_MORNING, cli_main
from test_basket import CAPITAL, EVENING, LIMITS, job_state, plan_rebalance
from test_basket import rows as cli_rows
from test_basket import run as rebalance
from test_blend import fake_panel, market
from test_gate import LATE

from agb import approval, heartbeat, jobs, lake, schedule
from agb.panel_store import PanelStore
from agb.schedule import Expected, Planned
from agb.services.pipeline import JOB_FAILED, RERUN_FAILED, RETRY, Pipeline, record
from agb.services.scheduler import Scheduler

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

    Pipeline(tmp_path, key, opener=vendor, sleep=no_wait)(db.connect("pipeline"))

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

    Pipeline(tmp_path, KEY, opener=vendor)(db.connect("pipeline"))

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
        (corrupt_body, "StoreError: body .* does not decompress"),
        (database_error, "DivisionByZero: division by zero"),
        (unreadable_parquet, "ArrowInvalid: "),
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

    def lose_lease(conn):
        # The lease runs out mid-pass and is given up, as recovery does for a stuck job, so
        # the job is no longer this worker's to complete.
        time.sleep(0.05)
        [(job_id,)] = cli_rows(db, "SELECT id FROM agb.job WHERE kind = 'rebalance'")
        assert jobs.release_expired(conn, job_id, "pipeline", "lease ran out")

    panel = Breaks(fake_panel(db, 0.2).config_id, lose_lease, shortlist_n=25)

    with caplog.at_level(logging.ERROR, logger="agb.services.pipeline"):
        rebalance(db, root, runner=panel, lease=timedelta(0))

    [record] = [r for r in caplog.records if "lease lost" in r.getMessage()]
    assert record.exc_info is None
    assert "rebalance 2026-10" in record.getMessage()
    assert job_state(db) != "failed"
    assert cli_rows(db, "SELECT count(*) FROM agb.job_event WHERE event = 'failed'") == [(0,)]
    assert alerts(db) == []


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

    run_at(db, root, LATE)

    assert job_state(db) == "failed"
    [(detail,)] = cli_rows(db, "SELECT detail FROM agb.job_event WHERE event = 'failed'")
    assert detail.startswith("CheckViolation: basket_cutoff: pipeline may not approve basket")
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
