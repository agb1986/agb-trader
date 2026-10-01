"""The local UI (T27): the decision log, the health banner and the four views, each rendered
from a seeded database and read on the `ui` role's read-only connection.

The pages are the acceptance criteria: the log lists every cycle and expands into what the
decider saw (b), the banner shows a stale heartbeat, an overdue event and an undelivered alert
(c), the four views render from database state (d), nothing writes (e), no page carries
JavaScript and the repository has no build step for it (f), and the server answers on the
loopback address alone (a).
"""

import html
import re
import subprocess
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.client import HTTPConnection
from pathlib import Path

import psycopg
import pytest
from conftest import ROOT
from market_fake import PERIOD, SIGNAL, TRADE
from test_bot import beat_at, expect
from test_execution import AFTER, CALENDAR, CLOSE_AT, NOW, basket, broker
from test_gate import HELD, NAMES, SOUND, TURNOVER, hold, insert, run, state

from agb import execution, panel, ui
from agb.services import ui as service

# When the operator is looking: after October's close, with the month still running.
LATER = datetime(2026, 10, 1, 21, 30, tzinfo=UTC)
# The two equity readings October's realised return is measured between.
BEFORE = datetime(2026, 9, 30, 20, 0, tzinfo=UTC)
LAST = datetime(2026, 10, 30, 20, 0, tzinfo=UTC)


@pytest.fixture
def conn(db, monkeypatch):
    """What a page reads on: the `ui` role from the environment, read-only in the database."""
    for key, value in db.pg_env("ui").items():
        monkeypatch.setenv(key, value)
    with ui.connect() as reader:
        yield reader


def equity(db, at, amount):
    """An account equity reading, as execution writes one after a close."""
    db.connect("execution").execute(
        "INSERT INTO agb.broker_state (observed_at, account_status, suspend_trade, no_shorting,"
        " trading_blocked, account_blocked, equity_usd, cash_usd, buying_power_usd, actor)"
        " VALUES (%s, 'ACTIVE', false, false, false, false, %s, 0, 0, 'execution')",
        (at, amount),
    )


def alert(db, kind, subject, period, body="something to look at", failures=0, delivered=False):
    """An alert and its delivery attempts, as the bot records them."""
    conn = db.connect("bot")
    (alert_id,) = conn.execute(
        "INSERT INTO agb.alert (kind, subject, period, body, raised_at, raised_by)"
        " VALUES (%s, %s, %s, %s, %s, 'bot') RETURNING id",
        (kind, subject, period, body, LATER - timedelta(hours=3)),
    ).fetchone()
    for attempt in range(failures):
        conn.execute(
            "INSERT INTO agb.alert_event (alert_id, event, channel, detail, actor, at)"
            " VALUES (%s, 'failed', 'telegram', 'proxy unreachable', 'bot', %s)",
            (alert_id, LATER - timedelta(hours=3) + timedelta(minutes=attempt + 1)),
        )
    if delivered:
        conn.execute(
            "INSERT INTO agb.alert_event (alert_id, event, channel, telegram_message_id, actor, at)"
            " VALUES (%s, 'delivered', 'telegram', 4821, 'bot', %s)",
            (alert_id, LATER - timedelta(hours=2)),
        )
    return alert_id


def ranks(db, basket_id, symbols):
    """The stage-1 ranking behind a basket, which the Positions view reads each name's rank
    from. The gate freezes a basket's content, so the ranks go in with the targets."""
    conn = db.connect("pipeline")
    for rank, symbol in enumerate(symbols, start=1):
        conn.execute(
            "INSERT INTO agb.basket_rank (basket_id, symbol, rank, score) VALUES (%s, %s, %s, %s)",
            (basket_id, symbol, rank, Decimal("0.5")),
        )


def panel_scores(db, totals):
    """A scored panel run, the smallest one the store accepts: a config, a run, one retrieval
    per name, the Judge's call and its score out of 24."""
    config_id = panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)
    conn = db.connect("pipeline")
    (run_id,) = conn.execute(
        "INSERT INTO agb.panel_run (panel_config_id, as_of, actor)"
        " VALUES (%s, %s, 'pipeline') RETURNING id",
        (config_id, SIGNAL),
    ).fetchone()
    for symbol, total in totals.items():
        (retrieval_id,) = conn.execute(
            "INSERT INTO agb.panel_retrieval (run_id, symbol, cik, company, outcome)"
            " VALUES (%s, %s, '0000000001', %s, 'retrieved') RETURNING id",
            (run_id, symbol, f"{symbol} Inc"),
        ).fetchone()
        (call_id,) = conn.execute(
            "INSERT INTO agb.panel_model_call (run_id, retrieval_id, role, model, response_model,"
            " request_sha256, response_sha256, cached, input_tokens, output_tokens,"
            " cache_read_tokens, cache_creation_tokens)"
            " VALUES (%s, %s, 'judge', 'm', 'm', %s, %s, false, 1, 1, 0, 0) RETURNING id",
            (run_id, retrieval_id, "a" * 64, "b" * 64),
        ).fetchone()
        conn.execute(
            "INSERT INTO agb.panel_score (retrieval_id, run_id, panel_config_id, sector,"
            " judge_call_id, outcome, total, score)"
            " VALUES (%s, %s, %s, 'Industrials', %s, 'scored', %s, %s)",
            (retrieval_id, run_id, config_id, call_id, total, total / 24),
        )


@pytest.fixture
def traded(db):
    """A month that traded: October's basket through the gate, submitted MOC, filled at the
    close and snapshotted, with the equity readings its return is measured between and the
    ranking and panel scores the Positions view shows."""
    conn = db.connect("execution")
    basket_id = insert(db, SOUND.target)
    ranks(db, basket_id, NAMES)
    run(db, basket_id, SOUND)
    assert state(db, basket_id)[:2] == ("approved", 2)
    target = basket(db, basket_id)
    fake = broker()
    assert execution.submit(conn, fake, target, NOW).state == "submitted"
    fake.close(CLOSE_AT)
    assert execution.reconcile(conn, fake, target, AFTER).state == "filled"
    assert execution.snapshot(conn, fake, TRADE, CALENDAR)
    equity(db, BEFORE, Decimal(100_000))
    equity(db, LAST, Decimal(104_000))
    panel_scores(db, {NAMES[0]: 20})
    return target


@pytest.fixture
def halted(db):
    """A cycle the gate stopped: September's book held, then October's basket selling seven of
    its ten names, which breaches the turnover limit."""
    hold(db, TURNOVER.book)
    basket_id = insert(db, TURNOVER.target)
    run(db, basket_id, TURNOVER)
    assert state(db, basket_id)[0] == "halted"
    return basket_id


# -- the decision log ----------------------------------------------------------------------


def test_the_log_lists_every_cycle_with_its_state_decider_return_and_turnover(conn, traded):
    """Acceptance criterion (b), the row: one line per cycle, newest first, carrying the state
    it reached, who decided it, the realised return of the month it traded and its turnover."""
    page = ui.log(conn, LATER)

    assert "Decision log" in page
    assert PERIOD in page and "filled" in page
    # October ran 100,000 to 104,000, and the basket had no book to turn over.
    assert "+4.00%" in page
    assert "no book" in page
    assert "20 names" in page
    assert "execution" in page, "the actor that moved it last is on the row"


def test_a_cycle_expands_into_the_diff_the_gate_result_and_the_flags(conn, db, halted):
    """Acceptance criterion (b), the expansion: the basket diff at decision time, the gate's
    verdict with what failed, that cycle's flags and every audit-log line naming it."""
    alert(db, "gate_breach", f"live basket {PERIOD}", PERIOD)
    page = ui.log(conn, LATER)

    # Both cycles, October first: the halted rebalance and the September book behind it.
    assert page.index(PERIOD) < page.index("2026-09")
    assert "halted" in page
    # The diff: seven held names sold, seventeen bought, and the turnover it asked for.
    assert "<details" in page, "a row expands with no JavaScript"
    for symbol in HELD[3:]:
        assert symbol in page
    assert NAMES[0] in page
    assert "70.0%" in page, "the row's turnover"
    # The gate: the breach, with what it measured against what it needed.
    assert "70.00%" in page and "FAIL" not in page
    assert re.search(r"\d+ of \d+ checks passed", page)
    assert "turnover" in page
    # The flags of the month, and the audit log's own account of the cycle.
    assert "gate_breach" in page
    assert "basket proposed" in page and "basket halted" in page


def test_a_month_with_no_equity_readings_says_so_rather_than_inventing_a_return(conn, db):
    insert(db)

    page = ui.log(conn, LATER)

    assert "n/a" in page
    assert "no equity readings in the month" in page


def test_every_page_renders_before_anything_has_happened(conn):
    """A new install is not an error: each page says what has not happened yet."""
    for path, view in ui.ROUTES.items():
        page = view(conn, LATER)
        assert page.startswith("<!doctype html>"), path
        assert "agb-trader" in page, path
    assert "No rebalance has been proposed yet." in ui.log(conn, LATER)
    assert "No live basket has been proposed yet." in ui.rebalance(conn, LATER)
    assert "none yet" in ui.positions(conn, LATER)
    assert "The gate has not run yet." in ui.gate(conn, LATER)
    assert "Nothing has been raised." in ui.alerts(conn, LATER)


# -- the health banner ---------------------------------------------------------------------


def test_the_banner_names_a_stale_heartbeat_an_overdue_event_and_an_undelivered_alert(conn, db):
    """Acceptance criterion (c): each of the three is seeded, and each is named in the banner
    above the log."""
    beat_at(db, "scheduler", LATER - timedelta(hours=2))
    expect(db, "submission", PERIOD, LATER - timedelta(hours=1))
    alert(db, "rebalance_pending", f"live basket {PERIOD}", PERIOD, failures=2)

    block = ui.banner(conn, LATER)

    assert "banner unwell" in block
    assert "stale heartbeat: scheduler" in block
    assert f"overdue: submission {PERIOD}" in block
    assert f"alert not delivered: rebalance_pending live basket {PERIOD} {PERIOD}" in block
    assert "2 failed attempts" in block
    assert "proxy unreachable" in block
    assert block in ui.log(conn, LATER), "the banner sits above the log"


def test_a_scheduler_that_died_reads_as_a_stale_beat_and_an_unscheduled_cycle(conn, db, traded):
    """The ticket's requirement: a dead scheduler is a stale heartbeat and a rebalance nothing
    has scheduled, never an absence."""
    beat_at(db, "scheduler", LATER - timedelta(days=2))

    block = ui.banner(conn, LATER)

    assert "stale heartbeat: scheduler" in block
    assert "no rebalance within horizon" in block
    assert "last scheduled none" in block


def test_the_banner_never_reports_the_service_reading_it(conn, db):
    beat_at(db, "ui", LATER - timedelta(hours=2))
    beat_at(db, "bot", LATER - timedelta(hours=2))

    block = ui.banner(conn, LATER)

    assert "stale heartbeat: bot" in block
    assert "stale heartbeat: ui" not in block


def test_a_healthy_banner_names_the_next_rebalance(conn, db):
    expect(db, "submission", "2026-11", LATER + timedelta(days=29))

    block = ui.banner(conn, LATER)

    assert "banner clear" in block
    assert "Healthy" in block
    assert "next rebalance 2026-11" in block


# -- the four views ------------------------------------------------------------------------


def test_the_rebalance_view_shows_the_basket_its_orders_and_its_standing(conn, traded):
    """Acceptance criterion (d), Rebalance: the latest live basket, read as the bot reads it."""
    page = ui.rebalance(conn, LATER)

    assert f"Rebalance {PERIOD}" in page
    assert "filled since" in page
    assert "5.00%" in page, "each target's weight"
    assert "new" in page, "a name bought this rebalance"
    assert f"{PERIOD}-{NAMES[0]}" in page, "the client order id the broker was given"
    assert "buy" in page


def test_a_basket_still_waiting_says_that_no_answer_means_no_trades(conn, db):
    """The vocabulary the ticket requires of every pending-basket surface."""
    insert(db)

    page = ui.rebalance(conn, LATER)

    assert "proposed since" in page
    assert "No answer by the session's cutoff means no trades this month." in html.unescape(page)


def test_the_positions_view_shows_the_book_with_its_panel_scores_and_ranks(conn, traded):
    """Acceptance criterion (d), Positions: the latest broker snapshot, each name with its
    weight, return since entry, how long it has been held, panel score and rank."""
    page = ui.positions(conn, LATER)

    assert f"20 held, $96,000.00, snapshot {TRADE}" in page
    assert NAMES[0] in page
    assert "5.0%" in page, "each name's weight in the book"
    assert "0.83 (20/24)" in page, "the panel's score out of 24"
    assert str(TRADE) in page, "held since the snapshot that first held it"


def test_the_gate_view_shows_every_check_and_what_it_measured(conn, traded):
    """Acceptance criterion (d), Gate: the latest run on a live basket, failures first."""
    page = ui.gate(conn, LATER)

    assert f"Gate — rebalance {PERIOD}" in page
    assert "checks passed." in page
    assert "pass" in page and "FAIL" not in page
    assert "broker checks" in page


def test_the_gate_view_puts_what_failed_first(conn, halted):
    page = ui.gate(conn, LATER)

    assert "checks failed. The rebalance was halted." in page
    assert page.index("FAIL") < page.index(">pass<")


def test_the_alerts_view_says_whether_each_alert_reached_the_operator(conn, db):
    """Acceptance criterion (d), Alerts: every alert with its delivery, which is the one place
    besides the banner an undelivered alert is visible."""
    alert(db, "partial_rebalance", f"live basket {PERIOD}", PERIOD, delivered=True)
    alert(db, "halt_open", "pause", PERIOD, body="the kill switch is on", failures=3)

    page = ui.alerts(conn, LATER)

    assert "partial_rebalance" in page and "delivered 2026-10-01" in page
    assert "halt_open" in page and "not delivered, 3 failed attempts" in page
    assert "the kill switch is on" in page


# -- read-only, and no JavaScript ----------------------------------------------------------


def test_no_page_can_write_because_the_connection_is_read_only(conn, traded):
    """Acceptance criterion (e): the pages are SELECTs, and the database refuses a write on
    the connection they read on, so a mistake cannot become one."""
    for path, view in ui.ROUTES.items():
        assert view(conn, LATER), path
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        conn.execute(
            "INSERT INTO agb.alert (kind, subject, period, body, raised_by)"
            " VALUES ('x', 'x', 'x', 'x', 'ui')"
        )
    source = (ROOT / "src/agb/ui.py").read_text() + (ROOT / "src/agb/services/ui.py").read_text()
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|TRUNCATE)\b", source)


def test_the_pages_carry_no_javascript_and_nothing_builds_any(conn, traded):
    """Acceptance criterion (f): the rows expand with <details>, so there is no script on any
    page and no Node or bundler anywhere in the repository."""
    for path, view in ui.ROUTES.items():
        page = view(conn, LATER)
        for forbidden in ("<script", "onclick", "javascript:", "fetch("):
            assert forbidden not in page, f"{forbidden} on {path}"
    tracked = subprocess.run(
        ["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True
    ).stdout.split()
    assert not [p for p in tracked if p.endswith((".js", ".mjs", ".ts", ".jsx", ".tsx"))]
    built = {"package.json", "package-lock.json", "yarn.lock", "pnpm-lock.yaml"}
    assert not [p for p in tracked if Path(p).name in built]
    assert "node" not in (ROOT / "Dockerfile").read_text().lower()


# -- the server ----------------------------------------------------------------------------


def test_the_server_answers_on_the_loopback_address_alone(db, monkeypatch, traded):
    """Acceptance criterion (a): the socket binds 127.0.0.1, every view answers a GET over it,
    an unknown path is a 404 and nothing but GET and HEAD is answered at all. The port is
    taken from the module so the operator's own 8080 is left alone."""
    assert (service.HOST, service.PORT) == ("127.0.0.1", 8080)
    for key, value in db.pg_env("ui").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(service, "PORT", 0)
    httpd = service.server()
    host, port = httpd.server_address[:2]
    assert host == "127.0.0.1"
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        for path, label in ui.VIEWS:
            client = HTTPConnection(host, port)
            client.request("GET", path)
            response = client.getresponse()
            body = response.read().decode()
            assert response.status == 200, path
            assert response.getheader("Content-Type") == "text/html; charset=utf-8"
            assert response.getheader("Cache-Control") == "no-store"
            assert f">{label}</a>" in body
            client.close()
        client = HTTPConnection(host, port)
        client.request("GET", "/positions?all=1")
        assert client.getresponse().status == 200, "a query string is not a different page"
        client.close()
        client = HTTPConnection(host, port)
        client.request("GET", "/nowhere")
        found = client.getresponse()
        assert found.status == 404
        assert "There is no /nowhere here." in found.read().decode()
        client.close()
        client = HTTPConnection(host, port)
        client.request("POST", "/", body=b"")
        assert client.getresponse().status == 501, "nothing but a read is answered"
        client.close()
    finally:
        httpd.shutdown()
        httpd.server_close()
