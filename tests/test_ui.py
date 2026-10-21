"""The local UI: the decision log, the health banner and the views, each rendered from a
seeded database and read on the `ui` role's read-only connection (T27), and the two write
paths the surface is allowed — answering a rebalance and the kill switch (T28).

The pages are T27's acceptance criteria: the log lists every cycle and expands into what the
decider saw (b), the banner shows a stale heartbeat, an overdue event and an undelivered alert
(c), the views render from database state (d), no page writes (e), no page carries JavaScript
and the repository has no build step for it (f), and the server answers on the loopback
address alone (a).

The writes are T28's: the routes are enumerated and are the two Telegram has, a wrong or
missing reference is refused and recorded, the loser of a race is told who answered and when,
pause, resume and liquidate read the same from either surface because both call
`agb.killswitch`, and every write leaves a `command` row naming the actor and the surface.
"""

import html
import re
import subprocess
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.client import HTTPConnection
from pathlib import Path
from urllib.parse import urlencode

import psycopg
import pytest
from conftest import ROOT
from market_fake import PERIOD, SIGNAL, TRADE
from test_bot import CHAT, FakeTelegram, beat_at, expect
from test_bot_commands import ET
from test_execution import AFTER, CALENDAR, CLOSE_AT, NOW, basket, broker
from test_gate import EVENING, HELD, NAMES, SOUND, TURNOVER, hold, insert, rows, run, state

from agb import approval, execution, panel, ui
from agb.services import ui as service
from agb.services.bot import Bot

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


def test_the_server_binds_the_loopback_address_unless_a_container_says_otherwise(monkeypatch):
    """Where the socket binds: the loopback address by default, which is what a bare process on
    the workstation wants, and every interface when the container asks for it, because there the
    published port arrives on the container's own address and Compose decides who reaches it."""
    assert service.host_from_env() == "127.0.0.1"
    monkeypatch.setenv("AGB_UI_HOST", "0.0.0.0")
    assert service.host_from_env() == "0.0.0.0"


def test_the_server_answers_on_the_loopback_address_alone(db, monkeypatch, traded):
    """Acceptance criterion (a): the socket binds 127.0.0.1, every view answers a GET over it,
    an unknown path is a 404 and nothing but GET and HEAD is answered at all. The port is
    taken from the module so the operator's own 8080 is left alone."""
    assert (service.HOST, service.PORT) == ("127.0.0.1", 8080)
    monkeypatch.delenv("AGB_UI_HOST", raising=False)
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
        answer = client.getresponse()
        assert answer.status == 405, "a page is read, never posted to"
        assert answer.getheader("Allow") == "GET, HEAD"
        answer.read()
        client.close()
    finally:
        httpd.shutdown()
        httpd.server_close()


# -- the two write paths -------------------------------------------------------------------

# A form as a browser posts one.
FORM = {"Content-Type": "application/x-www-form-urlencoded"}


@pytest.fixture
def writer(db, monkeypatch):
    """What a write path runs on: the same `ui` role, on the connection the server opens for a
    POST, which may insert what the role is granted and nothing else."""
    for key, value in db.pg_env("ui").items():
        monkeypatch.setenv(key, value)
    with ui.connect(write=True) as conn:
        yield conn


@pytest.fixture
def waiting(db):
    """A gated basket put to the human, which is the only state an answer is taken in: paper
    mode set to ask, the gate run, and the basket moved to pending as the bot moves it."""
    approval.set_paper_approval(db.connect("cli"), True, "operator", "rehearse the loop")
    basket_id = insert(db)
    run(db, basket_id)
    conn = db.connect("bot")
    [basket] = approval.waiting(conn)
    assert approval.put(conn, basket, EVENING, CALENDAR)
    assert state(db, basket_id)[:2] == ("pending", 2)
    return basket_id


def reply_of(body):
    """The reply the shared module produced, as the answering page carries it."""
    [reply] = re.findall(r'<pre class="reply">(.*?)</pre>', body, re.S)
    return html.unescape(reply)


def commands(db):
    """Every command either operator surface recorded, oldest first. The operator's own CLI is
    left out: the fixtures configure the month with it, and what is on trial here is what the
    two surfaces a human drives write."""
    return rows(
        db,
        "SELECT verb, outcome, reason, actor, surface FROM agb.command"
        " WHERE surface IN ('ui', 'telegram') ORDER BY id",
    )


def test_the_write_routes_are_the_two_telegram_has_and_nothing_else(conn, waiting):
    """The acceptance criterion that a test enumerates the UI's write routes. The five are
    answering a rebalance two ways and the three of the kill switch; a page is never one of
    them, every form on every page posts to one of them, and the only things a form asks the
    operator to type are a reference, a reason and a token. No route resizes a position, edits
    the universe, overrides the ranker, moves the ladder or promotes a model."""
    assert set(ui.WRITES) == {"/approve", "/reject", "/pause", "/resume", "/liquidate"}
    assert not set(ui.WRITES) & set(ui.ROUTES), "a page is read, a write path is posted to"

    for path, view in ui.ROUTES.items():
        page = view(conn, LATER)
        for attrs in re.findall(r"<form([^>]*)>", page):
            assert 'method="post"' in attrs, f"{path}: a write is a POST"
            [action] = re.findall(r'action="([^"]+)"', attrs)
            assert action in ui.WRITES, f"{action} on {path} is not a declared write"
        typed = set(re.findall(r'<input type="text" name="([^"]+)"', page))
        assert typed <= {"reference", "reason", "token"}, f"{path} asks for {typed}"
        assert 'value="' not in page, f"{path} prefills a field, which is a one-click answer"


def test_answering_a_rebalance_needs_the_reference_typed_and_nothing_is_prefilled(conn, waiting):
    """Not a one-click approval: the page says to type the reference and hands over an empty
    box, so the deliberation is the typing."""
    page = ui.rebalance(conn, LATER)

    assert "Your answer" in page
    assert "Type the reference, 2026-10, into the box to answer." in page
    assert '<input type="text" name="reference"' in page
    assert 'action="/approve"' in page and 'action="/reject"' in page


def test_an_answer_with_no_reference_is_refused_and_recorded(writer, db, waiting):
    """The acceptance criterion: a missing reference is refused, and the refusal is a row."""
    body = ui.approve(writer, {"reference": "   "}, EVENING)

    assert "/approve needs the rebalance's reference" in reply_of(body)
    assert commands(db) == [
        ("approve", "refused", "/approve needs the rebalance's reference", "ui:operator", "ui")
    ]
    assert state(db, waiting)[:2] == ("pending", 2), "nothing moved"


def test_an_answer_naming_the_wrong_rebalance_is_refused_and_recorded(writer, db, waiting):
    body = ui.reject(writer, {"reference": "2026-12", "reason": "mistyped"}, EVENING)

    assert "no rebalance 2026-12" in reply_of(body)
    assert commands(db) == [("reject", "refused", "no rebalance 2026-12", "ui:operator", "ui")]
    assert state(db, waiting)[:2] == ("pending", 2)


def test_approving_writes_the_row_and_the_event_the_bot_writes_with_the_ui_surface(
    writer, db, waiting
):
    """The acceptance criterion that every UI write is a `command` row with its actor and
    surface, and that answering moves the basket exactly as Telegram's answer does."""
    body = ui.approve(writer, {"reference": "2026-10"}, EVENING)

    assert re.match(rf"APPROVED 2026-10 by ui:operator at {ET}", reply_of(body))
    assert commands(db) == [("approve", "accepted", None, "ui:operator", "ui")]
    assert state(db, waiting)[:3] == ("approved", 3, "ui:operator")
    event = rows(
        db,
        "SELECT to_state, actor, surface, reason FROM agb.basket_event"
        " WHERE basket_id = %s ORDER BY seq",
        waiting,
    )[-1]
    assert event == ("approved", "ui:operator", "ui", "/approve 2026-10")


def test_rejecting_carries_the_reason_that_was_typed_with_it(writer, db, waiting):
    body = ui.reject(writer, {"reference": "2026-10", "reason": "turnover too high"}, EVENING)

    reply = reply_of(body)
    assert "REJECTED 2026-10 by ui:operator" in reply
    assert "reason: turnover too high" in reply
    assert rows(db, "SELECT reason, surface FROM agb.basket_event WHERE to_state = 'rejected'") == [
        ("turnover too high", "ui")
    ]


def test_the_surface_that_loses_the_race_is_told_who_answered_and_when(
    writer, db, waiting, monkeypatch
):
    """The acceptance criterion that the race is handled on both surfaces. Both read the basket
    pending; Telegram's answer lands first, and the UI's loses the compare-and-set on the
    basket's own sequence, so it is told who answered and left as one refused row."""
    stale = approval.find(writer, "2026-10")
    first = approval.Source("telegram:7", "telegram", "/approve 2026-10", CHAT, 11)
    assert "APPROVED" in approval.decide(
        db.connect("bot"), "approve", ["2026-10"], first, EVENING, CALENDAR
    )
    monkeypatch.setattr(approval, "find", lambda conn, reference: stale)

    body = ui.reject(writer, {"reference": "2026-10", "reason": "changed my mind"}, EVENING)

    assert re.match(rf"rebalance 2026-10: already approved by telegram:7 at {ET}", reply_of(body))
    assert state(db, waiting)[:3] == ("approved", 3, "telegram:7")
    assert commands(db)[-1][:2] == ("reject", "refused")


def test_the_kill_switch_page_shows_what_is_latched_and_what_resume_would_clear(conn, writer, db):
    assert "No halt cause is open." in ui.kill_switch(conn, LATER)
    assert "No token is outstanding." in ui.kill_switch(conn, LATER)

    ui.pause(writer, {}, LATER)
    page = ui.kill_switch(conn, LATER)

    assert "Open cause" in page and "ui:operator" in page
    assert "Clears every open cause, one clear each: pause." in page


def test_pause_resume_and_their_rows_read_the_same_from_either_surface(writer, db):
    """The acceptance criterion that the kill switch is identical to the bot's. Both surfaces
    call `agb.killswitch`, so the reply is the same text and the rows differ only in who threw
    the switch and from where."""
    from_ui = reply_of(ui.pause(writer, {}, LATER))
    cleared = reply_of(ui.resume(writer, {}, LATER))
    message = {"message_id": 11, "chat": {"id": int(CHAT)}, "from": {"id": 7}, "text": "/pause"}
    from_bot = Bot(FakeTelegram(), CHAT, lambda: LATER).pause(db.connect("bot"), message)

    assert from_ui == from_bot, "one lever, one reply"
    assert "cleared: pause" in cleared
    assert commands(db) == [
        ("pause", "accepted", None, "ui:operator", "ui"),
        ("resume", "accepted", None, "ui:operator", "ui"),
        ("pause", "accepted", None, "telegram:7", "telegram"),
    ]
    assert rows(db, "SELECT cause, action, actor FROM agb.halt_event ORDER BY id") == [
        ("pause", "set", "ui:operator"),
        ("pause", "clear", "ui:operator"),
        ("pause", "set", "telegram:7"),
    ]


def test_liquidating_from_the_ui_is_two_steps_with_the_token_typed_back(conn, writer, db, traded):
    """The kill switch's second lever, as the bot has it: what closing the book would mean and
    a token, then that token typed back. The page says a token is outstanding but never shows
    it, and a wrong one is refused, recorded and spent."""
    first = reply_of(ui.liquidate(writer, {}, LATER))
    assert "LIQUIDATE — step 1 of 2" in first
    assert "20 held" in first and "This cannot be undone." in first
    issued = rows(db, "SELECT token FROM agb.liquidation ORDER BY id")[-1][0]

    page = ui.kill_switch(conn, LATER)
    assert "A token is outstanding until" in page
    assert issued not in page, "a page that redisplayed the token would be one click"

    refused = reply_of(ui.liquidate(writer, {"token": "nope12"}, LATER))
    assert "wrong liquidation token" in refused
    assert [row[:2] for row in commands(db)] == [
        ("liquidate", "accepted"),
        ("liquidate", "refused"),
    ]

    reply_of(ui.liquidate(writer, {}, LATER))
    issued = rows(db, "SELECT token FROM agb.liquidation WHERE kind = 'token' ORDER BY id")[-1][0]
    done = reply_of(ui.liquidate(writer, {"token": issued}, LATER))

    assert "LIQUIDATING — step 2 of 2" in done
    assert ("liquidation", "set", "ui:operator") in rows(
        db, "SELECT cause, action, actor FROM agb.halt_event ORDER BY id"
    )


def test_the_server_posts_a_form_to_a_write_route_and_refuses_everything_else(db, monkeypatch):
    """The write paths over HTTP: a form posted to one of them is answered with the reply, a
    form from another site is refused before it is read and recorded nowhere because it names
    no operator, a POST to a page is 405 and an unknown path is 404."""
    for key, value in db.pg_env("ui").items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AGB_UI_HOST", raising=False)
    monkeypatch.setattr(service, "PORT", 0)
    httpd = service.server()
    host, port = httpd.server_address[:2]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        client = HTTPConnection(host, port)
        client.request(
            "POST", "/pause", body="", headers=FORM | {"Origin": f"http://{host}:{port}"}
        )
        response = client.getresponse()
        answer = html.unescape(response.read().decode())
        assert response.status == 200
        assert "PAUSED" in answer and "halt cause pause: open." in answer
        client.close()

        client = HTTPConnection(host, port)
        client.request(
            "POST", "/pause", body=urlencode({}), headers=FORM | {"Origin": "http://elsewhere"}
        )
        response = client.getresponse()
        assert response.status == 403
        assert "That request did not come from here." in response.read().decode()
        client.close()

        for path in ui.ROUTES:
            client = HTTPConnection(host, port)
            client.request("POST", path, body="", headers=FORM)
            response = client.getresponse()
            assert response.status == 405, path
            assert response.getheader("Allow") == "GET, HEAD"
            assert "is a page, and pages are read with GET." in response.read().decode()
            client.close()

        client = HTTPConnection(host, port)
        client.request("POST", "/nowhere", body="", headers=FORM)
        response = client.getresponse()
        assert response.status == 404
        response.read()
        client.close()
    finally:
        httpd.shutdown()
        httpd.server_close()

    assert commands(db) == [("pause", "accepted", None, "ui:operator", "ui")]
