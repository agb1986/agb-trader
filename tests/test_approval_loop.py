"""The approval loop (T13): a gated basket put to the human, the reminders, the answer, the
miss at the cutoff and the rerun, with paper mode set to ask the human as live mode does.
The bot talks to a fake Telegram on a clock the test moves; the database is real."""

import os
import re
from datetime import UTC, date, datetime, timedelta

import pytest
from market_fake import PERIOD, Market
from psycopg import errors
from test_basket import plan_rebalance
from test_basket import run as build
from test_bot import CHAT, FakeTelegram, expect
from test_bot_commands import ET, blocks, say
from test_execution import CALENDAR, NOW, basket, broker, expect_submission, halts
from test_gate import EVENING, LATE, insert, move, rows, run, state

from agb import approval, cli, execution
from agb.services.bot import Bot
from agb.services.scheduler import Scheduler

MINUTE = timedelta(minutes=1)
# The morning of the session after the October cutoff, when a rerun is built.
NEXT_MORNING = datetime(2026, 10, 2, 14, 0, tzinfo=UTC)
OPERATOR = os.environ.get("USER") or "operator"
FORM = "/approve 2026-10   /reject 2026-10 [reason]"
NOTHING_WAITING = "Nothing is waiting for an answer.  /status"


class Chat:
    """One bot talking to the configured chat, on a clock the test moves. One bot for the
    whole test, as in the service: it remembers which messages it has read."""

    def __init__(self, db, now=EVENING):
        self.now = now
        self.telegram = FakeTelegram()
        self.bot = Bot(self.telegram, CHAT, lambda: self.now)
        self.conn = db.connect("bot")
        self.read = 0

    def cycle(self, now=None):
        """A whole bot cycle, at `now` if given; returns what it sent."""
        self.now = now or self.now
        self.bot(self.conn)
        return self.new()

    def say(self, *texts):
        """The human sends `texts`; returns the replies."""
        say(self.telegram, *texts)
        self.bot.poll(self.conn, self.now)
        return self.new()

    def new(self):
        sent = [text for chat, text in self.telegram.sent[self.read :]]
        assert all(chat == CHAT for chat, _ in self.telegram.sent)
        self.read = len(self.telegram.sent)
        return sent


def one(texts, prefix):
    [text] = [t for t in texts if t.startswith(prefix)]
    return text


def none(texts, prefix):
    return [t for t in texts if t.startswith(prefix)] == []


@pytest.fixture
def asks(db):
    """Paper mode set to ask the human, as the operator would to rehearse the loop."""
    approval.set_paper_approval(db.connect("cli"), True, "operator", "rehearse the loop")


def gated(db, reference=None):
    """A basket that passed the gate in paper mode and waits for the human."""
    basket_id = insert(db, reference=reference)
    run(db, basket_id)
    assert state(db, basket_id)[:2] == ("proposed", 1)
    return basket_id


def completion(db, kind, period=PERIOD):
    return rows(
        db,
        "SELECT c.ref_table, c.actor FROM agb.expected_event_completion c"
        " JOIN agb.expected_event e ON e.id = c.expected_event_id"
        " WHERE e.kind = %s AND e.period = %s",
        kind,
        period,
    )


def decided_by(db, basket_id):
    """The event that decided the basket, with the command it came from."""
    return rows(
        db,
        "SELECT e.to_state, e.reason, c.verb, c.args, c.outcome, c.surface, c.raw_text"
        " FROM agb.basket_event e JOIN agb.command c ON c.id = e.command_id"
        " WHERE e.basket_id = %s",
        basket_id,
    )


def cli_main(db, monkeypatch, capsys, *argv, now=LATE + MINUTE):
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(cli, "clock", lambda: now)
    code = cli.main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


# Paper mode asks.


def test_paper_mode_runs_unattended_until_the_operator_sets_it_to_ask(db, monkeypatch, capsys):
    code, out, _ = cli_main(db, monkeypatch, capsys, "paper-approval", "show")
    assert code == 0
    assert re.fullmatch(rf"paper mode runs unattended, since {ET}, set by system: .+\n", out)

    code, out, _ = cli_main(
        db, monkeypatch, capsys, "paper-approval", "require", "--actor", "alex", "--reason", "drill"
    )
    assert code == 0
    assert re.fullmatch(rf"paper mode asks the human first, since {ET}, set by alex: drill\n", out)
    basket_id = gated(db)
    assert rows(db, "SELECT mode FROM agb.gate_run WHERE basket_id = %s", basket_id) == [("paper",)]
    assert rows(db, "SELECT actor, event FROM agb.audit_log WHERE source = 'paper_approval_event'")[
        -1
    ] == ("alex", "paper approval required")


def test_only_the_cli_sets_paper_approval_and_the_history_is_kept(db):
    for role in ("scheduler", "pipeline", "execution", "bot", "ui"):
        with pytest.raises(errors.InsufficientPrivilege):
            approval.set_paper_approval(db.connect(role), True, role)
    conn = db.connect("cli")
    approval.set_paper_approval(conn, True, "operator")
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute("DELETE FROM agb.paper_approval_event")


# AC1: a gated basket goes pending and the request arrives in the spec's order.


def test_a_gated_basket_goes_pending_and_the_request_arrives_in_order(db, tmp_path, asks):
    plan_rebalance(db)
    expect(db, approval.APPROVAL_REQUEST, PERIOD, EVENING + timedelta(hours=1))
    build(db, Market(tmp_path).names(25).land())
    [(basket_id,)] = rows(db, "SELECT id FROM agb.basket")
    assert state(db, basket_id) == ("proposed", 1, "pipeline", "pipeline")
    assert rows(db, "SELECT mode FROM agb.gate_run WHERE basket_id = %s", basket_id) == [("paper",)]

    chat = Chat(db)
    request = one(chat.cycle(), "REBALANCE")

    assert state(db, basket_id) == ("pending", 2, "bot", "telegram")
    header, out, in_, hold, numbers, flags, gate, prompt = blocks(request)
    assert header == (
        "REBALANCE 2026-10 — approval needed\n"
        "PAPER · L0 · deadline 2026-10-01 15:45 ET (in 21h 45m)"
    )
    assert out == "OUT (none)"
    title, *bought = in_.split("\n")
    assert title == "IN (20)"
    ranks = [re.fullmatch(r"  \+ N\d\d {4}rank (\d+)", line) for line in bought]
    assert all(ranks), in_
    assert [int(rank.group(1)) for rank in ranks] == list(range(1, 21))
    assert hold == "HOLD 0 unchanged"
    assert re.fullmatch(r"turnover none, no book yet  ·  cash after \d+\.\d%", numbers)
    assert flags == "FLAGS (none)"
    assert re.fullmatch(r"GATE  (\d+) of \1 passed  /gate", gate)
    assert prompt == (
        "/approve 2026-10   /reject 2026-10   /basket\n"
        "No answer by 2026-10-01 15:45 ET means no trades this month."
    )
    # The request reaching the chat is the approval request the month expected.
    assert completion(db, approval.APPROVAL_REQUEST) == [("alert_event", "bot")]
    assert none(chat.cycle(), "REBALANCE")


def test_a_basket_put_on_the_day_says_the_deadline_as_a_time(db, asks):
    gated(db)
    chat = Chat(db, now=datetime(2026, 10, 1, 14, 0, tzinfo=UTC))
    header = blocks(one(chat.cycle(), "REBALANCE"))[0]
    assert header.endswith("deadline 15:45 ET (in 5h 45m)")


def test_a_basket_whose_cutoff_passed_before_it_was_put_is_left_for_the_scheduler(db, asks):
    basket_id = gated(db)
    Chat(db, now=LATE).cycle()
    assert state(db, basket_id)[:2] == ("proposed", 1)


# AC2: /approve and /reject.


def test_approve_moves_the_basket_on_and_execution_submits_it(db, asks):
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()

    [reply] = chat.say("/approve 2026-10")

    assert reply == (
        "APPROVED 2026-10 by telegram:7 at 2026-09-30 18:00 ET\n"
        "Execution submits it before the 15:45 ET cutoff.  /status"
    )
    assert state(db, basket_id) == ("approved", 3, "telegram:7", "telegram")
    assert decided_by(db, basket_id) == [
        (
            "approved",
            "/approve 2026-10",
            "approve",
            {"reference": "2026-10", "basket_id": basket_id},
            "accepted",
            "telegram",
            "/approve 2026-10",
        )
    ]
    submitted = execution.submit(db.connect("execution"), broker(), basket(db, basket_id), NOW)
    assert submitted.state == "submitted"


def test_reject_records_the_reason_and_nothing_trades(db, asks):
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()

    [reply] = chat.say("/reject 2026-10 too much turnover")

    assert reply == (
        "REJECTED 2026-10 by telegram:7 at 2026-09-30 18:00 ET\n"
        "No trades this month. reason: too much turnover"
    )
    assert state(db, basket_id) == ("rejected", 3, "telegram:7", "telegram")
    [(to_state, reason, verb, args, outcome, *_)] = decided_by(db, basket_id)
    assert (to_state, reason, verb, outcome) == (
        "rejected",
        "too much turnover",
        "reject",
        "accepted",
    )
    assert args == {"reference": "2026-10", "basket_id": basket_id, "reason": "too much turnover"}
    assert halts(db) == []
    assert execution.waiting(db.connect("cli"), "approved") == []


# AC3: a bare command or a wrong reference is refused, re-prints the form, and is recorded.


@pytest.mark.parametrize(
    ("text", "why"),
    [
        ("/approve", "/approve needs the rebalance's reference"),
        ("/reject", "/reject needs the rebalance's reference"),
        ("/approve 2026-11", "no rebalance 2026-11"),
        ("/reject 2026-10r1 wrong month", "no rebalance 2026-10r1"),
        ("/approve 2026-10 now", "usage: /approve <reference>, nothing after it"),
    ],
)
def test_a_bare_or_wrong_answer_is_refused_with_the_form_and_recorded(db, asks, text, why):
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()

    [reply] = chat.say(text)

    assert reply == f"{why}\n\n{FORM}"
    assert state(db, basket_id)[:2] == ("pending", 2)
    [(verb, args, outcome, reason, actor, chat_id)] = rows(
        db, "SELECT verb, args, outcome, reason, actor, chat_id FROM agb.command"
    )
    assert (verb, outcome, reason, actor, chat_id) == (
        text.split()[0][1:],
        "refused",
        why,
        "telegram:7",
        CHAT,
    )
    assert args == {"args": text.split()[1:]}


def test_an_answer_before_the_basket_is_put_is_refused(db, asks):
    gated(db)
    source = approval.Source("ui:operator", "ui", "approve 2026-10")
    reply = approval.decide(db.connect("ui"), "approve", ["2026-10"], source, EVENING, CALENDAR)
    assert reply == f"rebalance 2026-10 is not yet waiting for an answer\n\n{NOTHING_WAITING}"


# AC4: two answers race; one wins and the other is told who answered and when.


def test_the_second_answer_is_told_who_answered_first_and_when(db, asks):
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()
    chat.say("/approve 2026-10")

    source = approval.Source("ui:operator", "ui", "reject 2026-10")
    reply = approval.decide(db.connect("ui"), "reject", ["2026-10"], source, EVENING, CALENDAR)

    assert re.fullmatch(
        rf"rebalance 2026-10: already approved by telegram:7 at {ET}\n\n{NOTHING_WAITING}", reply
    )
    assert state(db, basket_id)[:3] == ("approved", 3, "telegram:7")


def test_an_answer_read_before_the_other_landed_loses_the_compare_and_set(db, asks, monkeypatch):
    """Both surfaces read the basket pending; the bot's answer lands first."""
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()
    ui = db.connect("ui")
    stale = approval.find(ui, "2026-10")
    chat.say("/approve 2026-10")
    monkeypatch.setattr(approval, "find", lambda conn, reference: stale)

    source = approval.Source("ui:operator", "ui", "reject 2026-10 changed my mind")
    words = ["2026-10", "changed", "my", "mind"]
    reply = approval.decide(ui, "reject", words, source, EVENING, CALENDAR)

    assert re.match(rf"rebalance 2026-10: already approved by telegram:7 at {ET}\n\n", reply)
    assert state(db, basket_id)[:3] == ("approved", 3, "telegram:7")
    # The losing answer is one refused command; its accepted row went with its transaction.
    assert rows(db, "SELECT verb, outcome FROM agb.command WHERE surface = 'ui'") == [
        ("reject", "refused")
    ]


# AC5: the reminders at T-60 and T-20, two lines each.


def reminders(texts):
    return [t for t in texts if t.startswith("REBALANCE 2026-10 — still waiting")]


def test_the_reminders_go_out_at_t_minus_60_and_t_minus_20_two_lines_each(db, asks):
    gated(db)
    chat = Chat(db)
    chat.cycle()

    assert reminders(chat.cycle(LATE - 61 * MINUTE)) == []
    [t60] = reminders(chat.cycle(LATE - 60 * MINUTE))
    assert reminders(chat.cycle(LATE - 55 * MINUTE)) == []
    assert reminders(chat.cycle(LATE - 21 * MINUTE)) == []
    [t20] = reminders(chat.cycle(LATE - 20 * MINUTE))
    assert reminders(chat.cycle(LATE - MINUTE)) == []

    assert t60 == (
        "REBALANCE 2026-10 — still waiting for your answer, 60m left\n"
        "No answer by 15:45 ET means no trades this month.  /pending"
    )
    assert t20 == (
        "REBALANCE 2026-10 — still waiting for your answer, 20m left\n"
        "No answer by 15:45 ET means no trades this month.  /pending"
    )
    reminded = "SELECT subject FROM agb.alert WHERE kind = 'approval_reminder' ORDER BY id"
    subjects = rows(db, reminded)
    assert subjects == [("2026-10 T-60",), ("2026-10 T-20",)]


def test_a_basket_put_late_is_reminded_with_the_time_really_left(db, asks):
    gated(db)
    chat = Chat(db)
    one(chat.cycle(LATE - 30 * MINUTE), "REBALANCE 2026-10 — approval needed")
    [late] = reminders(chat.cycle(LATE - 29 * MINUTE))
    assert late.startswith("REBALANCE 2026-10 — still waiting for your answer, 29m left\n")


def test_no_reminder_for_a_basket_already_answered(db, asks):
    gated(db)
    chat = Chat(db)
    chat.cycle()
    chat.say("/approve 2026-10")
    assert reminders(chat.cycle(LATE - 60 * MINUTE)) == []


# AC6: the cutoff passes with no answer.


def test_at_the_cutoff_the_scheduler_marks_it_missed_and_no_latch_opens(db, asks):
    expect_submission(db)
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()

    Scheduler(lambda: LATE - MINUTE)(db.connect("scheduler"))
    assert state(db, basket_id)[:2] == ("pending", 2)
    Scheduler(lambda: LATE)(db.connect("scheduler"))

    assert state(db, basket_id) == ("missed", 3, "system", "scheduler")
    assert halts(db) == []
    # A miss is the month's outcome, not a failure: the submission it expected is settled.
    assert completion(db, "submission") == [("basket_event", "scheduler")]
    assert rows(db, "SELECT raised_by FROM agb.alert WHERE kind = 'approval_expired'") == [
        ("scheduler",)
    ]
    notice = one(chat.cycle(LATE + MINUTE), "EXPIRED")
    assert notice == (
        "EXPIRED — rebalance 2026-10 had no answer by 2026-10-01 15:45 ET.\n\n"
        "Marked missed by system. No trades this month: nothing was sent to the broker and "
        "no halt is set.\n\n"
        "To trade it at the next session's close instead: agb rerun 2026-10"
    )
    [reply] = chat.say("/approve 2026-10")
    assert re.fullmatch(
        rf"rebalance 2026-10: already missed by system at {ET}\n\n{NOTHING_WAITING}", reply
    )
    Scheduler(lambda: LATE + 2 * MINUTE)(db.connect("scheduler"))
    assert state(db, basket_id)[:2] == ("missed", 3)


def test_a_gated_basket_never_put_to_the_human_is_missed_too(db, asks):
    basket_id = gated(db)
    Scheduler(lambda: LATE)(db.connect("scheduler"))
    assert state(db, basket_id) == ("missed", 2, "system", "scheduler")


def test_an_answer_after_the_cutoff_but_before_the_sweep_is_refused(db, asks):
    basket_id = gated(db)
    chat = Chat(db)
    chat.cycle()
    chat.now = LATE
    [reply] = chat.say("/approve 2026-10")
    assert reply == (f"rebalance 2026-10: the deadline, 2026-10-01 15:45 ET, has passed\n\n{FORM}")
    assert state(db, basket_id)[:2] == ("pending", 2)


# AC7: the operator reruns a missed basket at the next session's close.


def test_a_missed_rebalance_is_rerun_through_the_gate_and_the_approval(
    db, tmp_path, asks, monkeypatch, capsys
):
    plan_rebalance(db)
    root = Market(tmp_path).names(25).land()
    build(db, root)
    [(original,)] = rows(db, "SELECT id FROM agb.basket")
    chat = Chat(db)
    chat.cycle()
    Scheduler(lambda: LATE)(db.connect("scheduler"))
    assert state(db, original)[:2] == ("missed", 3)

    code, out, _ = cli_main(db, monkeypatch, capsys, "rerun", "2026-10", "--actor", "alex")

    assert code == 0
    assert out.startswith(
        "rerun 2026-10r1 asked for: rebalance 2026-10 again, at the close of 2026-10-02.\n"
    )
    assert rows(db, "SELECT reference, session_date FROM agb.rerun_pending") == [
        ("2026-10r1", date(2026, 10, 2))
    ]

    build(db, root, clock=lambda: NEXT_MORNING)

    [(rerun, deviation_of, session, period)] = rows(
        db,
        "SELECT id, deviation_of, session_date, period FROM agb.basket"
        " WHERE reference = '2026-10r1'",
    )
    assert (deviation_of, session, period) == (original, date(2026, 10, 2), PERIOD)
    assert state(db, rerun) == ("proposed", 1, "pipeline", "pipeline")
    assert rows(db, "SELECT mode FROM agb.gate_run WHERE basket_id = %s", rerun) == [("paper",)]
    assert rows(db, "SELECT count(*) FROM agb.rerun_pending") == [(0,)]
    same = "SELECT symbol, side, qty FROM agb.basket_order WHERE basket_id = %s ORDER BY symbol"
    assert rows(db, same, rerun) == rows(db, same, original)

    request = one(chat.cycle(NEXT_MORNING), "REBALANCE 2026-10r1 — approval needed")
    assert "deadline 15:45 ET (in 5h 45m)" in blocks(request)[0]
    [reply] = chat.say("/approve 2026-10r1")
    assert reply.startswith("APPROVED 2026-10r1 by telegram:7 at 2026-10-02 10:00 ET\n")
    assert state(db, rerun)[:3] == ("approved", 3, "telegram:7")
    assert state(db, original)[:2] == ("missed", 3)

    code, _, err = cli_main(db, monkeypatch, capsys, "rerun", "2026-10")
    assert code == 1
    assert err == "agb: refused: rebalance 2026-10 is already rerun as 2026-10r1\n"
    code, _, err = cli_main(db, monkeypatch, capsys, "rerun", "2026-10r1")
    assert code == 1
    assert err == (
        "agb: refused: rebalance 2026-10r1 is approved; only a missed rebalance is rerun\n"
    )
    assert rows(
        db, "SELECT actor, surface, outcome FROM agb.command WHERE verb = 'rerun' ORDER BY id"
    ) == [("alex", "cli", "accepted"), *[(OPERATOR, "cli", "refused")] * 2]


def test_only_a_missed_rebalance_is_rerun(db, asks, monkeypatch, capsys):
    gated(db)
    code, _, err = cli_main(db, monkeypatch, capsys, "rerun", "2026-10")
    assert code == 1
    assert err == "agb: refused: rebalance 2026-10 is proposed; only a missed rebalance is rerun\n"
    code, _, err = cli_main(db, monkeypatch, capsys, "rerun", "2026-12")
    assert (code, err) == (1, "agb: refused: no rebalance 2026-12\n")


def test_a_rerun_after_a_weekend_trades_on_the_next_session():
    # Friday's miss is rerun on Monday.
    friday = date(2026, 10, 2)
    at = CALENDAR.moc_cutoff(friday) + MINUTE
    assert approval.rerun_session(friday, at, CALENDAR) == date(2026, 10, 5)
    # Asked for days later, it trades at the first session whose cutoff is still ahead.
    assert approval.rerun_session(friday, datetime(2026, 10, 7, 20, 0, tzinfo=UTC), CALENDAR) == (
        date(2026, 10, 8)
    )


# AC8: only a human's surface decides a pending basket.


@pytest.fixture
def pending_basket(db, asks):
    basket_id = gated(db)
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
    return basket_id


@pytest.mark.parametrize("to_state", ["approved", "rejected"])
@pytest.mark.parametrize("role", ["scheduler", "pipeline", "execution"])
def test_no_service_but_the_bot_and_the_ui_decides_a_pending_basket(
    db, pending_basket, role, to_state
):
    with pytest.raises(errors.InsufficientPrivilege, match="basket_transition"):
        move(db, pending_basket, 3, "pending", to_state, role)


def test_the_cli_writes_no_basket_event(db, pending_basket):
    with pytest.raises(errors.InsufficientPrivilege, match="permission denied"):
        move(db, pending_basket, 3, "pending", "approved", "cli")


def test_the_system_never_decides_a_pending_basket(db, pending_basket):
    with pytest.raises(errors.InsufficientPrivilege, match="basket_policy.*only a human decides"):
        move(db, pending_basket, 3, "pending", "approved", "bot", "telegram", actor="system")


def test_while_paper_asks_the_gate_may_not_approve_alone(db, asks):
    basket_id = gated(db)
    with pytest.raises(errors.InsufficientPrivilege, match="basket_policy.*ask the human first"):
        move(db, basket_id, 2, "proposed", "approved", "pipeline", actor="system")


@pytest.mark.parametrize("to_state", ["approved", "rejected"])
@pytest.mark.parametrize(("role", "surface"), [("bot", "telegram"), ("ui", "ui")])
def test_the_bot_and_the_ui_decide(db, pending_basket, role, surface, to_state):
    move(db, pending_basket, 3, "pending", to_state, role, surface, actor="telegram:7")
    assert state(db, pending_basket) == (to_state, 3, "telegram:7", surface)
