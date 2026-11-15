"""The panel store's nightly integrity check: acceptance criterion (d) of T32.

What the check is for is what the database cannot do: every panel body -- a filing document,
its text, a model request or response, a scrubbed extract -- lives in the store, and the
database holds only the hash it is named by. These tests build a run whose rows cite real
bodies, then take bodies away, rewrite them and break their compression, and assert that the
check finds each one, that the pipeline's nightly job records what it found, and that the bot
turns a body it could not read back into one alert.
"""

import shutil
from datetime import UTC, date, datetime, timedelta

import pytest
from psycopg import errors
from test_bot import CHAT, FakeTelegram, alerts

from agb import health, jobs, panel, panel_audit, schedule
from agb.panel_store import PanelStore
from agb.schedule import Expected, Planned
from agb.services.bot import ALERT_HEADER, Bot
from agb.services.pipeline import Pipeline, sample_from_env

PERIOD = "2026-09-14"
SIGNAL = date(2026, 9, 14)
NOW = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)
# Each name in a run cites five bodies: the filing's document and its text, the Judge's
# request and response, and the scrubbed extract.
PER_NAME = 5

_FILING = """
    INSERT INTO agb.panel_filing (retrieval_id, document, form, accession, filed, report_date,
        section, document_sha256, text_sha256, chars, sent_chars)
    VALUES (%s, 'D1', '10-K', '0000000001-26-000001', %s, %s, 'Item 1A', %s, %s, 1000, 1000)
"""
_CALL = """
    INSERT INTO agb.panel_model_call (run_id, retrieval_id, role, model, response_model,
        request_sha256, response_sha256, cached, input_tokens, output_tokens,
        cache_read_tokens, cache_creation_tokens)
    VALUES (%s, %s, 'judge', 'm', 'm', %s, %s, %s, 1, 1, 0, 0)
    RETURNING id
"""
_EXTRACT = """
    INSERT INTO agb.panel_extract (retrieval_id, model_call_id, outcome, scrubber_version,
        extract_sha256)
    VALUES (%s, %s, 'extracted', 'v1', %s)
"""


@pytest.fixture
def store(tmp_path):
    return PanelStore(tmp_path / "panel")


def put(store, *parts):
    """A body in the store, by its hash, whose content is unique to `parts`."""
    return store.put_text("/".join(parts))


def panel_bodies(db, store, *symbols, request=None):
    """A panel run citing five bodies for each name, every one of them in `store`.

    `request` is the request body every call cites instead of its own, which is what a cache
    hit looks like: two calls, one body. Returns the distinct hashes the run cites.
    """
    config_id = panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)
    conn = db.connect("pipeline")
    (run_id,) = conn.execute(
        "INSERT INTO agb.panel_run (panel_config_id, as_of, actor)"
        " VALUES (%s, %s, 'pipeline') RETURNING id",
        (config_id, SIGNAL),
    ).fetchone()
    cited = []
    for symbol in symbols:
        (retrieval_id,) = conn.execute(
            "INSERT INTO agb.panel_retrieval (run_id, symbol, cik, company, outcome)"
            " VALUES (%s, %s, '0000000001', %s, 'retrieved') RETURNING id",
            (run_id, symbol, f"{symbol} Inc"),
        ).fetchone()
        document, text = put(store, symbol, "document"), put(store, symbol, "text")
        conn.execute(_FILING, (retrieval_id, SIGNAL, SIGNAL, document, text))
        asked = request or put(store, symbol, "request")
        answered = put(store, symbol, "response")
        (call_id,) = conn.execute(
            _CALL, (run_id, retrieval_id, asked, answered, request is not None)
        ).fetchone()
        extract = put(store, symbol, "extract")
        conn.execute(_EXTRACT, (retrieval_id, call_id, extract))
        cited += [document, text, asked, answered, extract]
    return sorted(set(cited))


def plan_audit(db, period=PERIOD, *, start=None):
    """The scheduler's panel_audit job for `period`, claimable from `start`."""
    start = start or datetime.now(UTC) - timedelta(hours=1)
    job = Planned(
        schedule.PANEL_AUDIT,
        period,
        date.fromisoformat(period),
        start,
        (Expected(schedule.PANEL_AUDIT, period, start + schedule.PANEL_AUDIT_GRACE, True),),
    )
    schedule.write(db.connect("scheduler"), [job])


def audits(db):
    return (
        db.connect("cli")
        .execute(
            "SELECT period, bodies, checked, sound, missing, corrupt, actor"
            " FROM agb.panel_audit ORDER BY id"
        )
        .fetchall()
    )


def path(store, digest):
    return store.path(digest)


def lose(store, digest):
    """The disk losing a body: the file is gone, the database still refers to it."""
    path(store, digest).unlink()


def rewrite(store, digest, other):
    """The disk rewriting a body: a file that is there, holding another body's bytes."""
    path(store, digest).write_bytes(path(store, other).read_bytes())


def garble(store, digest):
    """A body whose compression itself is broken, so it cannot even be decompressed."""
    path(store, digest).write_bytes(b"not a zstd frame")


# The check.


def test_a_sound_store_reads_back_sound(db, store):
    cited = panel_bodies(db, store, "AAA", "BBB")
    assert len(cited) == 2 * PER_NAME

    audit = panel_audit.check(db.connect("pipeline"), store)

    assert (audit.bodies, audit.checked, audit.sound) == (len(cited), len(cited), len(cited))
    assert (audit.missing, audit.corrupt, audit.unsound) == ((), (), ())


def test_a_body_two_rows_cite_is_one_body(db, store):
    shared = put(store, "shared", "request")
    cited = panel_bodies(db, store, "AAA", "BBB", request=shared)
    assert len(cited) == 2 * PER_NAME - 1

    audit = panel_audit.check(db.connect("pipeline"), store)

    # Nine bodies, not ten: the cached call cites the request the other call already cited,
    # and a body cited twice is not twice as likely to be drawn.
    assert (audit.bodies, audit.checked, audit.sound) == (9, 9, 9)


def test_a_missing_body_a_rewritten_one_and_an_unreadable_one_are_all_found(db, store):
    cited = panel_bodies(db, store, "AAA", "BBB")
    gone, rewritten, garbled, *rest = cited
    lose(store, gone)
    rewrite(store, rewritten, rest[0])
    garble(store, garbled)

    audit = panel_audit.check(db.connect("pipeline"), store)

    assert audit.missing == (gone,)
    assert sorted(audit.corrupt) == sorted([rewritten, garbled])
    assert (audit.bodies, audit.checked, audit.sound) == (len(cited), len(cited), len(cited) - 3)
    # Missing first: a body with no file at all is the one to go and look for.
    assert audit.unsound[0] == gone


def test_each_night_draws_its_own_sample(db, store):
    cited = panel_bodies(db, store, "AAA", "BBB")
    shutil.rmtree(store.root)
    conn = db.connect("pipeline")

    drawn = set()
    for _ in range(20):
        audit = panel_audit.check(conn, store, sample=3)
        assert (audit.bodies, audit.checked, audit.sound) == (len(cited), 3, 0)
        assert audit.corrupt == ()
        drawn |= set(audit.missing)

    # Each draw is three of the ten, and over twenty draws the sample moves over the store.
    assert drawn <= set(cited)
    assert len(drawn) > 3


def test_the_default_cap_reads_the_whole_store_and_stays_inside_the_grace():
    """Sized from the measurement of 2026-11-08: 776 bodies read back in 2 seconds."""
    measured_bodies, measured_seconds = 776, 2
    seconds_at_cap = panel_audit.SAMPLE * measured_seconds / measured_bodies

    # The whole store as measured is read, with room to grow, and the bound is still a bound:
    # reading up to the cap at the measured rate takes under a tenth of the grace period.
    assert panel_audit.SAMPLE >= 100 * measured_bodies
    assert seconds_at_cap < schedule.PANEL_AUDIT_GRACE.total_seconds() / 10


def test_a_cap_below_the_store_still_draws_a_sample(db, store):
    cited = panel_bodies(db, store, "AAA", "BBB")

    audit = panel_audit.check(db.connect("pipeline"), store, sample=len(cited) - 1)

    assert (audit.bodies, audit.checked) == (len(cited), len(cited) - 1)


def test_an_empty_store_checks_nothing(db, store):
    audit = panel_audit.check(db.connect("pipeline"), store)

    assert (audit.bodies, audit.checked, audit.sound) == (0, 0, 0)
    assert audit.unsound == ()


# The record.


def test_a_period_is_recorded_once(db, store):
    cited = panel_bodies(db, store, "AAA")
    plan_audit(db)
    conn = db.connect("pipeline")
    claim = jobs.claim(conn, [schedule.PANEL_AUDIT], "pipeline", timedelta(minutes=5))
    audit = panel_audit.check(conn, store)

    first = panel_audit.record(conn, PERIOD, claim.job_id, audit, "pipeline")
    again = panel_audit.record(conn, PERIOD, claim.job_id, audit, "pipeline")

    assert first is not None
    assert again is None
    assert audits(db) == [(PERIOD, len(cited), len(cited), len(cited), [], [], "pipeline")]


def test_only_the_pipeline_records_a_check(db):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("bot").execute(
            "INSERT INTO agb.panel_audit (period, job_id, bodies, checked, sound, actor)"
            " VALUES (%s, 1, 0, 0, 0, 'bot')",
            (PERIOD,),
        )


# The nightly job, end to end.


def test_the_nightly_job_records_a_sound_store_and_says_nothing(db, store, tmp_path):
    cited = panel_bodies(db, store, "AAA", "BBB")
    plan_audit(db)
    conn = db.connect("pipeline")

    Pipeline(tmp_path / "lake", panel_dir=store.root)(conn)

    assert audits(db) == [(PERIOD, len(cited), len(cited), len(cited), [], [], "pipeline")]
    assert health.panel_bodies(db.connect("bot")) == []
    assert (
        conn.execute("SELECT state FROM agb.job_state WHERE kind = 'panel_audit'").fetchone()[0]
        == "completed"
    )


def test_a_corrupted_body_completes_the_job_and_raises_one_alert(db, store, tmp_path):
    cited = panel_bodies(db, store, "AAA", "BBB")
    rewritten, other = cited[0], cited[1]
    rewrite(store, rewritten, other)
    plan_audit(db)
    conn = db.connect("pipeline")

    Pipeline(tmp_path / "lake", panel_dir=store.root)(conn)

    # The job did its work, so it completes: the finding is the store's, not the job's.
    assert (
        conn.execute("SELECT state FROM agb.job_state WHERE kind = 'panel_audit'").fetchone()[0]
        == "completed"
    )
    assert audits(db) == [
        (PERIOD, len(cited), len(cited), len(cited) - 1, [], [rewritten], "pipeline")
    ]

    telegram = FakeTelegram()
    bot = db.connect("bot")
    Bot(telegram, CHAT, lambda: NOW)(bot)
    # The same night read again raises nothing new: one alert a night, not one per body.
    Bot(FakeTelegram(), CHAT, lambda: NOW + timedelta(minutes=1))(bot)

    assert [row for row in alerts(db) if row[0] == "panel_body_unsound"] == [
        ("panel_body_unsound", "panel store", PERIOD)
    ]
    assert [text for _, text in telegram.sent if "panel store" in text] == [
        f"{ALERT_HEADER}\n\n"
        "  panel store: 0 missing, 1 corrupt of 10 checked\n"
        f"    checked 10 of 10 referenced bodies on {PERIOD}; first unsound {rewritten}"
    ]


def test_the_sample_can_be_set_on_the_deploy_machine(monkeypatch):
    """The deliberate-corruption check needs a cap that covers the store, without a release."""
    monkeypatch.delenv(panel_audit.SAMPLE_ENV, raising=False)
    assert sample_from_env() == panel_audit.SAMPLE
    monkeypatch.setenv(panel_audit.SAMPLE_ENV, "5000")
    assert sample_from_env() == 5000
    assert Pipeline.from_env().sample == 5000
    for refused in ("all", "0", "-1"):
        monkeypatch.setenv(panel_audit.SAMPLE_ENV, refused)
        with pytest.raises(SystemExit, match="not a number"):
            sample_from_env()
