"""The local UI: the decision log, the health banner and the nine views beside it, each
rendered from a seeded database and read on the `ui` role's read-only connection (T27), the
two write paths the surface is allowed — answering a rebalance and the kill switch (T28) — and
the four evidence views, which read what the record will support and no more (T29).

The pages are T27's acceptance criteria: the log lists every cycle and expands into what the
decider saw (b), the banner shows a stale heartbeat, an overdue event and an undelivered alert
(c), the views render from database state (d), no page writes (e), no page carries JavaScript
and the repository has no build step for it (f), and the server answers on the loopback
address alone (a).

The writes are T28's: the routes are enumerated and are the two Telegram has, a wrong or
missing reference is refused and recorded, the loser of a race is told who answered and when,
pause, resume and liquidate read the same from either surface because both call
`agb.killswitch`, and every write leaves a `command` row naming the actor and the surface.

The evidence views are T29's, and each test holds the page to the thing that keeps a figure
honest: Validation shows every gate beside the threshold it was applied against and ends by
naming what the certification covers (a), Promotion carries the date, the actor and the
evidence of every transition (b), Performance shows no live figure without the backtest
expectation beside it and says outright that three months cannot tell skill from luck (c),
Panel gives each dimension the Judge's own reason and names the filings it was read from (d),
and none of the four has a write path (e).
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
from psycopg.types.json import Jsonb
from test_bot import CHAT, FakeTelegram, beat_at, expect
from test_bot_commands import ET
from test_execution import AFTER, CALENDAR, CLOSE_AT, NOW, basket, broker
from test_gate import EVENING, HELD, NAMES, SOUND, TURNOVER, hold, insert, rows, run, state
from test_health import dividend, mode, months
from test_ladder import at_level

from agb import analyst, approval, cost, debate, execution, panel, ui
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


def panel_scores(db, totals, *, flags=None, missing=()):
    """A panel run over a set of names: a config, a run, one retrieval each and, for a name
    EDGAR answered for, the Judge's call and its score out of 24. `flags` marks a name as a
    delinquent filer or a recent listing, and `missing` names the ones EDGAR failed on, which
    are never debated. Returns the run, its config and each name's retrieval and score."""
    flags = flags or {}
    config_id = panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)
    conn = db.connect("pipeline")
    (run_id,) = conn.execute(
        "INSERT INTO agb.panel_run (panel_config_id, as_of, actor)"
        " VALUES (%s, %s, 'pipeline') RETURNING id",
        (config_id, SIGNAL),
    ).fetchone()
    scored = {}
    for symbol in (*totals, *missing):
        failed = symbol in missing
        delinquent, ipo = flags.get(symbol, (None, False))
        (retrieval_id,) = conn.execute(
            "INSERT INTO agb.panel_retrieval (run_id, symbol, cik, company, outcome, detail,"
            " delinquent, ipo) VALUES (%s, %s, '0000000001', %s, %s, %s, %s, %s) RETURNING id",
            (
                run_id,
                symbol,
                f"{symbol} Inc",
                "edgar_failure" if failed else "retrieved",
                "EDGAR answered 503 three times" if failed else None,
                delinquent,
                ipo,
            ),
        ).fetchone()
        if failed:
            scored[symbol] = (retrieval_id, None)
            continue
        (call_id,) = conn.execute(
            "INSERT INTO agb.panel_model_call (run_id, retrieval_id, role, model, response_model,"
            " request_sha256, response_sha256, cached, input_tokens, output_tokens,"
            " cache_read_tokens, cache_creation_tokens)"
            " VALUES (%s, %s, 'judge', 'm', 'm', %s, %s, false, 1, 1, 0, 0) RETURNING id",
            (run_id, retrieval_id, "a" * 64, "b" * 64),
        ).fetchone()
        (score_id,) = conn.execute(
            "INSERT INTO agb.panel_score (retrieval_id, run_id, panel_config_id, sector,"
            " judge_call_id, outcome, total, score)"
            " VALUES (%s, %s, %s, 'Industrials', %s, 'scored', %s, %s) RETURNING id",
            (retrieval_id, run_id, config_id, call_id, totals[symbol], totals[symbol] / 24),
        ).fetchone()
        scored[symbol] = (retrieval_id, score_id)
    return run_id, config_id, scored


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


# -- what the evidence views read ----------------------------------------------------------

# The stand-in model the evidence views read, and the capital its certification was sized for.
EVIDENCE = "evidence-fake"
CERTIFIED = Decimal(100_000)
CERTIFIED_AT = datetime(2026, 1, 5, tzinfo=UTC)
# The gate record `agb.research.gates` writes: the four gates the Validation view names, each
# with the value measured and the rule it was measured against, beside the diagnostics the same
# object carries. Those diagnostics are not gates, and the page must not read them as ones.
GATES = {
    "dsr": {"value": 0.97, "threshold": 0.95, "rule": "at least", "passed": True},
    "pbo": {"value": 0.1, "threshold": 0.5, "rule": "at most", "passed": True},
    "sharpe_net": {"value": 0.8, "threshold": 0.5, "rule": "above", "passed": True},
    "dsr_2x": {"value": 0.96, "threshold": 0.95, "rule": "at least", "passed": True},
    "thresholds": {"dsr": 0.95, "pbo": 0.5},
    "start_dates": ["2010-01-01", "2011-01-01"],
    "start_date_stable": True,
    "paths": 240,
    "months": 192,
    "candidates": 1200,
}
# The path distribution the same certification recorded, which the live account is read against.
PATHS = {
    "paths": 240,
    "quantiles": [0.05, 0.25, 0.5, 0.75, 0.95],
    "max_drawdown": {
        "min": -0.3,
        "q05": -0.1,
        "q25": -0.07,
        "q50": -0.05,
        "q75": -0.03,
        "q95": -0.01,
        "max": -0.005,
    },
    "longest_losing_run": {
        "min": 0,
        "q05": 1,
        "q25": 1,
        "q50": 2,
        "q75": 3,
        "q95": 4,
        "max": 6,
    },
    "tracking_dispersion": {
        "min": 0.01,
        "q05": 0.015,
        "q25": 0.02,
        "q50": 0.03,
        "q75": 0.04,
        "q95": 0.05,
        "max": 0.08,
    },
    "monthly_return": {
        "min": -0.02,
        "q05": -0.005,
        "q25": 0.005,
        "q50": 0.01,
        "q75": 0.015,
        "q95": 0.02,
        "max": 0.03,
    },
}

_CERTIFY = """
    INSERT INTO agb.model_certification (
        model_id, certifies, passed, artifact_sha256, artifact_path, ledger_sha256, features,
        train_start, train_end, declared_trials, dsr, pbo, sharpe_net, dsr_2x,
        break_even_multiple, sharpe_gross, gross_sharpe_flag, participation_max,
        participation_mean, cost_model_version, certified_capital, path_quantiles, gates,
        certified_at, actor)
    VALUES (%(model_id)s, 'stage 1 (classical ranker) only', true, %(artifact)s, %(path)s,
            %(ledger)s, %(features)s, '2010-01-01', '2025-12-31', 1200, 0.97, 0.1, 0.8, 0.96,
            3.0, 1.0, false, 0.001, 0.0005, %(cost_model_version)s, %(certified_capital)s,
            %(path_quantiles)s, %(gates)s, %(certified_at)s, 'research')
    RETURNING id
"""
# A live basket the panel scored, which is the one the Panel view reads. The gate's fixtures
# write baskets without a panel run, so this view needs its own.
_PANEL_BASKET = """
    INSERT INTO agb.basket (reference, period, session_date, signal_date, snapshot_date,
        partition_hash, universe_size, ranker, cost_model_version, capital_usd,
        panel_config_id, panel_run_id, panel_w, actor)
    VALUES ('2026-11', '2026-11', '2026-11-02', '2026-10-30', '2026-10-30', %s, 2000,
            'momentum', '1', 100000, %s, %s, 0.2, 'pipeline')
    RETURNING id
"""
_BLEND = """
    INSERT INTO agb.basket_blend (basket_id, symbol, stage1_rank, stage1_rank_pct,
        panel_score_id, llm_total, llm_score, llm_score_pct, neutral, final_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
_DIMENSION = """
    INSERT INTO agb.panel_dimension (score_id, dimension, score, reason, source, judge_score,
        judge_reason)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
_FILING = """
    INSERT INTO agb.panel_filing (retrieval_id, document, form, accession, filed, report_date,
        section, document_sha256, text_sha256, chars, sent_chars)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def certify(db, gates: dict | None = None) -> int:
    """The research side's certification of the stand-in model, holding the gate record and the
    path distribution the Validation view reads."""
    [(certification_id,)] = (
        db.connect("research")
        .execute(
            _CERTIFY,
            {
                "model_id": EVIDENCE,
                "artifact": "ef" * 32,
                "path": f"/research/certifications/{EVIDENCE}",
                "ledger": "fa" * 32,
                "features": Jsonb(["mom_12_1"]),
                "cost_model_version": cost.CURRENT.version,
                "certified_capital": CERTIFIED,
                "path_quantiles": Jsonb(PATHS),
                "gates": Jsonb(GATES if gates is None else gates),
                "certified_at": CERTIFIED_AT,
            },
        )
        .fetchall()
    )
    return certification_id


def promote(db) -> int:
    """That certification promoted, as `agb promote` records it: the operator's command and the
    promotion event naming the certification it made live."""
    certification_id = certify(db)
    conn = db.connect("cli")
    with conn.transaction():
        [(command_id,)] = conn.execute(
            "INSERT INTO agb.command (actor, surface, raw_text, verb, outcome)"
            " VALUES ('operator', 'cli', %s, 'promote', 'accepted') RETURNING id",
            (f"agb promote {EVIDENCE}",),
        ).fetchall()
        conn.execute(
            "INSERT INTO agb.promotion_event (certification_id, command_id, actor)"
            " VALUES (%s, %s, 'operator')",
            (certification_id, command_id),
        )
    return certification_id


@pytest.fixture
def certified(db):
    """A promoted model and three paper months measured against it: January up five per cent,
    February down nine once the dividend the operator recorded is added back, March down five.
    The ladder is at L1, so the level above wants something the record does not yet show."""
    certification_id = promote(db)
    mode(db, "paper")
    months(db, "0.05", "-0.10", "-0.05")
    dividend(db, "2026-02", NAMES[0], "1050.00", "2026-02-20")
    at_level(db, "L1")
    return certification_id


@pytest.fixture
def blended(db):
    """A live basket's shortlist as the blend wrote it: one name the panel scored on all six
    dimensions and read two filings for, one delinquent filer whose two capped dimensions a
    filing rule set rather than the Judge, and one name EDGAR failed on, which keeps the
    neutral middle of the range."""
    best, flagged, unscored = NAMES[0], NAMES[1], NAMES[2]
    run_id, config_id, scores = panel_scores(
        db,
        {best: 20, flagged: 14},
        flags={flagged: ("nt_10k", False)},
        missing=(unscored,),
    )
    conn = db.connect("pipeline")
    [(basket_id,)] = conn.execute(_PANEL_BASKET, ("ab" * 32, config_id, run_id)).fetchall()
    blend_rows = [
        (basket_id, best, 1, 0.0005, scores[best][1], 20, 20 / 24, 20 / 24, False, 0.9),
        (basket_id, flagged, 2, 0.001, scores[flagged][1], 14, 14 / 24, 14 / 24, False, 0.6),
        (basket_id, unscored, 3, 0.0015, None, None, None, 0.5, True, 0.5),
    ]
    for row in blend_rows:
        conn.execute(_BLEND, row)
    conn.execute(
        "INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface)"
        " VALUES (%s, 1, NULL, 'proposed', 'pipeline', 'pipeline')",
        (basket_id,),
    )
    # What the Judge scored each dimension, in order. The delinquent filer's first two are
    # the dimensions the filing rule caps, so its row keeps the Judge's score beside the one
    # the rule imposed, and the totals above are what the dimensions add up to.
    judged = {best: (4, 4, 3, 3, 3, 3), flagged: (3, 4, 3, 3, 3, 3)}
    for symbol in (best, flagged):
        for dimension, judge_score in enumerate(judged[symbol], start=1):
            rule = symbol == flagged and dimension in debate.DELINQUENT_DIMENSIONS
            reason = f"Dimension {dimension} reads clean in the filings."
            conn.execute(
                _DIMENSION,
                (
                    scores[symbol][1],
                    dimension,
                    debate.DELINQUENT_CAP if rule else judge_score,
                    "Capped by rule: the annual report is late." if rule else reason,
                    "rule" if rule else "judge",
                    judge_score,
                    reason,
                ),
            )
    filings = [
        ("D1", "10-K", "0000000001-26-000001", "2026-02-15", "Item 1A", 2000, 8000),
        ("D2", "8-K", "0000000001-26-000002", "2026-03-01", "Item 8.01", 1200, 1200),
    ]
    for document, form, accession, filed, section, sent, chars in filings:
        conn.execute(
            _FILING,
            (
                scores[best][0],
                document,
                form,
                accession,
                filed,
                filed,
                section,
                "cd" * 32,
                "ef" * 32,
                chars,
                sent,
            ),
        )
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


# -- the views beside the log --------------------------------------------------------------


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


# -- the four evidence views ---------------------------------------------------------------


def test_the_validation_view_shows_every_gate_beside_the_threshold_it_was_applied_against(
    conn, certified
):
    """Acceptance criterion (a): each gate with what it measured and the threshold it was
    measured against, so a figure cannot be read without the rule it was judged by."""
    page = html.unescape(ui.validation(conn, LATER))

    assert f"{EVIDENCE} passed its gates, certified 2026-01-04 19:00 ET by research" in page
    assert "on 2010-01-01\u20132025-12-31 at $100,000.00 under cost model 1." in page
    assert "Deflated Sharpe" in page and "0.970" in page and "at least 0.950" in page
    assert "Probability of backtest overfitting" in page and "at most 0.500" in page
    assert "Cost-inclusive Sharpe (total return)" in page and "above 0.50" in page
    assert "Deflated Sharpe at 2x cost" in page and "0.960" in page
    assert "FAIL" not in page and page.count(">pass<") == 4, "four gates, every one passed"
    assert "Declared trials 1,200, which is what the deflated Sharpe is deflated by" in page
    assert "Break-even cost multiple 3.00" in page
    assert "Gross Sharpe 1.00, unflagged." in page


def test_the_validation_view_names_the_returns_the_sharpe_gate_read(conn, db):
    """Issue #167: the cost-inclusive Sharpe is labelled with its basis, read from the gate the
    research profile recorded; a record without one is a certification that gated total
    returns."""
    sharpe = {**GATES["sharpe_net"], "basis": "excess_spy", "defined": True, "cause": None}
    certify(db, {**GATES, "sharpe_net": sharpe})

    page = html.unescape(ui.validation(conn, LATER))

    assert "Cost-inclusive Sharpe (excess over SPY)" in page
    assert "(total return)" not in page


def test_the_validation_view_ends_by_naming_what_the_certification_does_not_cover(conn, certified):
    """The sentence the ticket asks for in as many words: these gates are stage 1's."""
    page = html.unescape(ui.validation(conn, LATER))

    assert "These gates certify stage 1 (classical ranker) only." in page
    assert "The LLM safety panel, and the blend that puts its score beside the ranker's, are" in (
        page
    )
    assert "certified by nothing on this page" in page


def test_the_validation_view_marks_the_live_account_where_it_is_outside_the_paths(conn, certified):
    """The live column is the point of the path table: a drawdown worse than the worst 5% of
    certified paths is marked, and a reading inside the distribution is not."""
    page = ui.validation(conn, LATER)

    assert "240 combinatorially purged cross-validation paths." in page
    assert '<span class="bad">-13.55%</span>' in page, "worse than the 5th percentile's -10.00%"
    assert '<span class="bad">7.21%</span>' in page, "wider than the 95th percentile's 5.00%"
    assert '<span class="bad">2.00</span>' not in page, "a losing run at the median is ordinary"


def test_the_validation_view_gives_the_runs_own_diagnostics_no_gate_row(conn, certified):
    """`research/certify.py` stores the run's diagnostics in the same object as its gates: the
    thresholds it used, the path and month counts, the start-date sweep. None of those says
    whether it passed, so none of them is a gate, and inventing a row for one would read as a
    gate that measures nothing."""
    page = html.unescape(ui.validation(conn, LATER))

    for diagnostic in ("thresholds", "start_dates", "start_date_stable", "candidates", "months"):
        assert diagnostic not in page, f"{diagnostic} is a diagnostic, not a gate"


def test_the_promotion_view_names_which_certification_is_live_and_who_promoted_it(conn, certified):
    """Acceptance criterion (b): the live model, by certification, actor and artifact."""
    page = html.unescape(ui.promotion(conn, LATER))

    assert f"{EVIDENCE} is live: certification {certified}, promoted" in page
    assert "by operator, passed its gates on 1,200 declared trials under cost model 1," in page
    assert f"artifact {'ef' * 6}." in page
    assert "Its gates and its path distribution are on the" in page


def test_the_promotion_view_carries_the_actor_and_the_evidence_of_every_transition(conn, certified):
    """Acceptance criterion (b), the history: every transition the ladder has made, each with
    when it happened, who made it and the evidence they made it on."""
    page = html.unescape(ui.promotion(conn, LATER))

    assert "Every transition" in page
    assert "When" in page and "Actor" in page and "Evidence" in page
    assert "mode paper" in page and "cli" in page
    assert "certified" in page and f"{EVIDENCE} passed its gates" in page
    assert f"artifact {'ef' * 6}" in page, "the certification's own evidence"
    assert "promoted" in page and f"certification {certified}" in page
    assert "level L0" in page and "live trading starts at L0" in page
    assert "level L1" in page and "raised to L1 by ada" in page
    assert "ada" in page and "the record is sound" in page, "the attestation it was raised on"


def test_the_promotion_view_shows_the_level_the_mode_and_what_the_next_level_wants(conn, certified):
    """Acceptance criterion (b), the standing: where the ladder is, and every criterion the
    rung above wants with what the record measures against it."""
    page = html.unescape(ui.promotion(conn, LATER))

    assert "Trading paper since 2025-11-30 19:00 ET, set by cli." in page
    assert "Level L1 since" in page and "set by ada" in page
    assert "Attested: the record is sound" in page
    assert "Towards L2" in page and "L2 is not earned: days_at_l1." in page
    assert "days_at_l1" in page and ">= 365" in page and "unmet" in page
    assert "rejections_at_l1" in page and "met" in page
    assert "Towards live" in page
    assert "Paper, with 8 of these criteria outstanding: paper_rebalances," in page
    assert "fill_tolerance" in page and "a recorded pass" in page


def test_no_live_figure_on_the_performance_view_appears_without_its_backtest_expectation(
    conn, certified
):
    """Acceptance criterion (c): the realised return is never shown alone. Beside it stands
    what the certified paths expected over the same number of months, and every measure in the
    path table carries the distribution it is read against."""
    page = html.unescape(ui.performance(conn, LATER))

    assert f"Read against {EVIDENCE}'s 240 certified paths." in page
    assert "return with dividends -9.23%" in page
    assert "backtest expectation +3.03%" in page
    assert "+1.00% a month over 3 months, the median of 240 certified paths" in page
    assert page.index("return with dividends") < page.index("backtest expectation")
    assert "Beside the backtest" in page
    assert "Median" in page and "95th" in page, "the distribution beside the live column"
    assert "Worst month 2026-02 at -9.00%, against a certified median path averaging 1.00% a" in (
        page
    )
    assert "Those certified figures are each path's average month and not its worst one" in page


def test_the_performance_view_says_three_months_cannot_tell_skill_from_luck(conn, certified):
    """Acceptance criterion (c), the caveat: the page says outright that the record is far too
    short to judge the model by, rather than leaving the operator to infer it."""
    page = html.unescape(ui.performance(conn, LATER))

    assert "3 months measured. That is far too few to tell a model that works from one that" in (
        page
    )
    assert "the certified distribution above is what says which readings are ordinary" in page


def test_the_performance_view_adds_back_the_dividends_the_paper_broker_never_credited(
    conn, certified
):
    """Acceptance criterion (c), the months: each month with its mode, its return, the
    dividends the operator recorded for it and how it compares with a certified average
    month. A paper month is restated, because the paper broker credits no dividend and the
    backtest's expectation assumes them."""
    page = html.unescape(ui.performance(conn, LATER))

    assert "Every month" in page
    assert "Dividends added back" in page
    assert "2026-01" in page and "5.00%" in page and "+4.00 points" in page
    assert "2026-02" in page and "-9.00%" in page and "$1,050.00" in page
    assert "-10.00 points" in page, "February against a certified average month of +1.00%"
    assert "2026-03" in page and "-5.00%" in page and "-6.00 points" in page
    assert "3 of these 3 months were paper months, restated with the dividends the operator" in (
        page
    )
    assert "the paper broker credits none, and the backtest's expectation assumes them." in page
    assert "Live equity already holds them." in page


def test_the_panel_view_shows_every_dimension_with_the_judges_own_reason(conn, blended):
    """Acceptance criterion (d): each name's six rubric dimensions, each with the score out of
    four and the reason the Judge wrote for it."""
    page = html.unescape(ui.panel(conn, LATER))

    assert "Rebalance 2026-11: 3 names shortlisted out of a universe of 2,000," in page
    assert "each blended at w = 0.2 between the ranker's place and the panel's safety score" in (
        page
    )
    assert "Panel run 1." in page
    assert f"{NAMES[0]} — stage-1 rank 1; safety 20/24 (0.83), 1st safest of the 2 scored" in page
    assert "The Judge's reason" in page
    for number, (title, _) in analyst.RUBRIC.items():
        assert title in page, f"dimension {number} by name"
        assert f"Dimension {number} reads clean in the filings." in page
    assert page.count("4/4") == 2 and page.count("3/4") == 8


def test_the_panel_view_says_the_panel_was_never_told_which_company_it_read(conn, blended):
    """The note the ticket asks for: the scores were given on a name-scrubbed extract."""
    page = html.unescape(ui.panel(conn, LATER))

    assert "The panel scored a name-scrubbed extract of the filings above." in page
    assert "It was not told which company it was looking at, or its ticker, industry or" in page


def test_the_panel_view_names_the_filings_the_analyst_read_and_how_much_of_each(conn, blended):
    """Acceptance criterion (d), the sources: the filings behind the score, each with the
    section quoted from and how much of it was sent, because a score read from the first two
    thousand characters of a risk-factors section is a weaker reading than one read in full."""
    page = html.unescape(ui.panel(conn, LATER))

    assert "10-K" in page and "Item 1A" in page
    assert "first 2,000 of 8,000 characters" in page
    assert "8-K" in page and "Item 8.01" in page and "in full" in page
    assert "2026-02-15" in page, "when it was filed"
    assert "No filing section was recorded for this name." in page, "the name with no filings"


def test_a_filing_rule_that_caps_a_dimension_says_so_and_keeps_the_judges_score(conn, blended):
    """A delinquent filer's capped dimensions are the one place a score on the page is not the
    Judge's. The page names the dimensions the rule touched, the cap it imposed, and what the
    Judge had scored them, so a reader is never shown a rule's number as a judgement."""
    page = html.unescape(ui.panel(conn, LATER))
    capped = ui.names_of(debate.DELINQUENT_DIMENSIONS)

    assert f"{NAMES[1]} — stage-1 rank 2; safety 14/24 (0.58), 2nd safest of the 2 scored" in page
    assert "Flagged nt_10k from its filings: a late or missing annual report caps" in page
    assert f"caps {capped} at {debate.DELINQUENT_CAP} by rule" in page
    assert "dimension 1 (going concern and liquidity)" in capped
    assert "dimension 2 (accounting integrity)" in capped
    assert f"at {debate.DELINQUENT_CAP} by rule, whatever the Judge scored them." in page
    assert "Capped by rule: the annual report is late." in page
    assert "Set by a filing rule; the Judge scored 4." in page
    assert "Set by a filing rule; the Judge scored 3." in page
    assert f"{debate.DELINQUENT_CAP}/4" in page


def test_a_name_edgar_failed_on_keeps_the_neutral_middle_and_says_why(conn, blended):
    """A name the panel could not read is not scored zero: the blend holds it at the middle of
    the range, and the page says what failed rather than showing an empty record."""
    page = html.unescape(ui.panel(conn, LATER))

    assert f"{NAMES[2]} — stage-1 rank 3; safety not scored, held at the neutral middle" in page
    assert "EDGAR returned edgar_failure: EDGAR answered 503 three times" in page


def test_the_evidence_views_carry_no_write_path(conn, certified, blended):
    """Acceptance criterion (e) for these four: they are evidence, and evidence is read. None
    of them is a write route, and none renders a form for one."""
    for route in ("/validation", "/promotion", "/performance", "/panel"):
        assert route not in ui.WRITES
        page = ui.ROUTES[route](conn, LATER)
        assert "<form" not in page
        assert "method=" not in page and "<button" not in page


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


def allow_own_address(httpd):
    """The address a test server bound, which is a random port, made the one name it answers:
    the default list names port 8080, which the operator's own UI may hold on this machine."""
    host, port = httpd.server_address[:2]
    httpd.allowed_hosts = frozenset({f"{host}:{port}"})
    return host, port


def assert_protected(response):
    """The four headers every response carries, whatever its status."""
    assert response.getheader("X-Frame-Options") == "DENY"
    assert "frame-ancestors 'none'" in response.getheader("Content-Security-Policy", "")
    assert response.getheader("X-Content-Type-Options") == "nosniff"
    # Not `no-referrer`: under it a browser sends `Origin: null` on the page's own form posts,
    # which the write check refuses, so the UI's forms would fail. Do not tighten it back.
    assert response.getheader("Referrer-Policy") == "same-origin"


def test_the_server_answers_the_tunnels_two_names_unless_configured_otherwise(monkeypatch):
    """The names a request may be addressed to: the two the operator's tunnel opens the UI as,
    or the comma-separated list AGB_UI_ALLOWED_HOSTS names instead, compared without case."""
    monkeypatch.delenv("AGB_UI_ALLOWED_HOSTS", raising=False)
    assert service.allowed_hosts_from_env() == {"127.0.0.1:8080", "localhost:8080"}
    monkeypatch.setenv("AGB_UI_ALLOWED_HOSTS", " 127.0.0.1:9090, LocalHost:9090 ,")
    assert service.allowed_hosts_from_env() == {"127.0.0.1:9090", "localhost:9090"}
    monkeypatch.setenv("AGB_UI_ALLOWED_HOSTS", " , ")
    assert service.allowed_hosts_from_env() == {"127.0.0.1:8080", "localhost:8080"}


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
    host, port = allow_own_address(httpd)
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
            assert_protected(response)
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
    host, port = allow_own_address(httpd)
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


def test_a_request_addressed_to_another_name_or_a_write_with_no_origin_is_refused(
    db, monkeypatch, waiting
):
    """Issue #171. A page that rebinds its own name to the loopback address arrives carrying that
    name as `Host` and a matching `Origin`, so the name is checked against the allowed list and
    anything else is a 421, a read as much as a write: the pages carry the basket reference. A
    write must also declare an `Origin` from the same list. None of it is recorded, and every
    response, refusals and the stdlib's own errors included, forbids framing and sniffing."""
    for key, value in db.pg_env("ui").items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AGB_UI_HOST", raising=False)
    monkeypatch.setattr(service, "PORT", 0)
    httpd = service.server()
    host, port = allow_own_address(httpd)
    here = f"http://{host}:{port}"
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    def send(method, path, headers):
        client = HTTPConnection(host, port)
        client.request(method, path, body="" if method == "POST" else None, headers=headers)
        response = client.getresponse()
        body = response.read().decode()
        client.close()
        assert_protected(response)
        return response.status, body

    try:
        rebound = {"Host": "evil.example", "Origin": "http://evil.example"}
        for path in ui.WRITES:
            status, body = send("POST", path, FORM | rebound)
            assert status == 421, path
            assert "That request was not addressed to this server." in body
            status, _ = send("POST", path, FORM | rebound | {"Origin": here})
            assert status == 421, f"{path}: an allowed origin does not excuse the host"
        for path in ui.ROUTES:
            assert send("GET", path, {"Host": "evil.example"})[0] == 421, path
        assert send("HEAD", "/", {"Host": "evil.example"})[0] == 421
        assert send("GET", "/", {"Host": f"evil.example:{port}"})[0] == 421
        assert send("GET", "/", {"Host": ""})[0] == 421, "a request that names no host"

        for path in ui.WRITES:
            status, body = send("POST", path, FORM)
            assert status == 403, f"{path}: a write that declares no origin"
            assert "That request did not come from here." in body
            for origin in ("null", "http://evil.example", f"https://{host}:{port}"):
                status, _ = send("POST", path, FORM | {"Origin": origin})
                assert status == 403, f"{path} from {origin}"

        assert send("GET", "/", {"Host": f"{host}:{port}".upper()})[0] == 200
        assert send("HEAD", "/", {})[0] == 200
        assert send("GET", "/nowhere", {})[0] == 404
        assert send("PUT", "/", {})[0] == 501, "the stdlib's own error carries the headers too"
    finally:
        httpd.shutdown()
        httpd.server_close()

    assert commands(db) == []
    assert state(db, waiting)[0] == "pending", "nothing answered the rebalance"
