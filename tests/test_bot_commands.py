"""The bot's read-only commands, the fill confirmation, the gate-halt alert and the position
mismatch alert, against a paper rebalance run through `execution` with the in-memory broker
from tests/test_execution.py. Every message is checked for the house style: a header, then
items of exactly two lines, then an optional footer, and never longer than Telegram takes."""

import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import registry_fake
from conftest import SUPERUSER
from market_fake import PERIOD, TRADE
from psycopg import errors
from test_bot import CHAT, UPDATE_IDS, FakeTelegram, events, sent
from test_execution import (
    AFTER,
    CALENDAR,
    CLOSE_AT,
    CUTOFF,
    NEXT,
    NOW,
    approved,
    broker,
)
from test_gate import BASE, insert, rows, run, state
from test_health import distribution, dividend, mode, promote

from agb import approval, execution, readout
from agb.alerts import acknowledge
from agb.services.bot import (
    MESSAGE_MAX,
    REFUSAL_LIMIT,
    REFUSAL_WINDOW,
    Bot,
    help_text,
    parse,
    split,
)

SENDER = 7
ET = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} ET"


def say(telegram, *texts, chat=CHAT, sender=SENDER, at=AFTER):
    """Queue one message per text, sent at `at`, as Telegram would hand them to the bot."""
    for text in texts:
        message = {
            "message_id": 100 + len(telegram.updates),
            "chat": {"id": int(chat)},
            "from": {"id": sender},
            "date": sent(at),
        }
        if text is not None:
            message["text"] = text
        telegram.updates.append({"update_id": next(UPDATE_IDS), "message": message})


def ask(db, *texts):
    """Send the commands to a fresh bot and return its replies, one per command."""
    telegram = FakeTelegram()
    say(telegram, *texts)
    Bot(telegram, CHAT, lambda: AFTER).poll(db.connect("bot"), AFTER)
    assert all(chat == CHAT for chat, _ in telegram.sent)
    return [text for _, text in telegram.sent]


def blocks(text):
    return text.split("\n\n")


def assert_style(text):
    """A header, items of two lines (what, then its facts), and an optional footer."""
    assert len(text) <= MESSAGE_MAX
    header, *rest = blocks(text)
    assert header and not header.startswith(" ")
    for block in rest:
        lines = block.split("\n")
        if block.startswith(" "):
            assert len(lines) == 2, block
            what, facts = lines
            assert what.startswith("  ") and what.strip(), block
            assert facts.startswith("    ") and facts.strip(), block
        else:
            assert all(not line.startswith(" ") for line in lines), block


def commands(db):
    return rows(
        db,
        "SELECT actor, surface, raw_text, verb, args, outcome, reason, chat_id,"
        " telegram_message_id FROM agb.command ORDER BY id",
    )


def alerts(db):
    return rows(db, "SELECT kind, subject, period FROM agb.alert ORDER BY id")


def body(db, kind):
    [(text,)] = rows(db, "SELECT body FROM agb.alert WHERE kind = %s", kind)
    return text


def cycle(db, telegram, now=AFTER):
    Bot(telegram, CHAT, lambda: now)(db.connect("bot"))


@pytest.fixture
def paper(db):
    """The October basket approved by the gate, submitted MOC, filled at the close and
    snapshotted: a whole paper rebalance. Returns the broker and the basket."""
    conn = db.connect("execution")
    target = approved(db)
    fake = broker()
    assert execution.submit(conn, fake, target, lambda: NOW, CUTOFF).state == "submitted"
    fake.close(CLOSE_AT)
    assert execution.reconcile(conn, fake, target, AFTER).state == "filled"
    assert execution.snapshot(conn, fake, TRADE, CALENDAR)
    return fake, target


# The commands.


def test_each_command_answers_from_the_database_after_a_paper_rebalance(db, paper):
    """Acceptance criterion 1: every read-only command answers from what execution and the
    gate recorded, in the house style."""
    replies = ask(db, "/status", "/positions", "/pnl", "/basket", "/gate", "/log", "/help")
    status, positions, pnl, basket, gate, log, help_ = replies

    for reply in replies:
        assert_style(reply)

    assert blocks(status)[0] == "agb-trader — PAPER · ACTIVE"
    assert re.fullmatch(
        rf"  account ACTIVE\n    equity \$100,000.00, cash \$100,000.00; observed {ET}",
        blocks(status)[1],
    )
    assert blocks(status)[2] == "  20 positions held\n    worth $96,000.00, snapshot 2026-10-01"
    assert re.fullmatch(
        rf"  rebalance {PERIOD} filled\n    since {ET}, by execution;"
        r" model momentum-fake",
        blocks(status)[3],
    )
    assert re.fullmatch(
        rf"  model momentum-fake live\n    certification \d+, promoted {ET} by operator",
        blocks(status)[4],
    )

    assert blocks(positions)[0] == "POSITIONS — 20 held, $96,000.00, snapshot 2026-10-01"
    assert blocks(positions)[1:3] == [
        "  N01  240 shares\n    $4,800.00 · 5.0% · entry $20.00",
        "  N02  240 shares\n    $4,800.00 · 5.0% · entry $20.00",
    ]
    assert len(blocks(positions)) == 21

    assert blocks(pnl)[0] == "PNL — since the first reading"
    assert blocks(pnl)[1] == "  return +0.00%\n    equity $100,000.00 to $100,000.00, $0.00"
    assert (
        blocks(pnl)[-1]
        == "  backtest expectation\n    momentum-fake recorded no monthly distribution"
    )

    header, *targets = blocks(basket)
    assert re.fullmatch(
        rf"BASKET {PERIOD} — FILLED · 20 names · \$100,000.00\n"
        rf"filled {ET} by execution; model momentum-fake",
        header,
    )
    assert targets[0] == "  N01  5.00%\n    240 shares · new"
    assert len(targets) == 20

    assert re.fullmatch(
        rf"GATE — rebalance {PERIOD}, run {ET}; broker checks {ET}", blocks(gate)[0]
    )
    assert "FAIL" not in gate
    assert blocks(gate)[1] == "  ok  snapshot_date\n        measured 2026-09-30, needs 2026-09-30"
    assert "  ok  notional, 20 orders\n        measured 4.80% of 100000.00, needs <= 8%" in gate
    assert "  ok  suspend_trade\n        measured false, needs false" in gate
    assert blocks(gate)[-1] == "153 of 153 checks passed."

    assert blocks(log)[0] == f"LOG — last {readout.LOG_DEFAULT} events, oldest first"
    assert len(blocks(log)) == 1 + readout.LOG_DEFAULT

    assert help_ == help_text()
    assert blocks(help_)[0] == "COMMANDS"


def test_status_says_the_mode_is_not_set_when_there_is_no_mode_row(db):
    """The migrations record paper, and the append-only rule keeps it; a superuser bypassing
    that rule is the only way to lose it, and /status still answers."""
    with db.cluster.connect(db.name, SUPERUSER) as conn:
        conn.execute("SET session_replication_role = replica")
        conn.execute("DELETE FROM agb.mode_event")
    [status] = ask(db, "/status")
    assert_style(status)
    assert blocks(status)[0] == "agb-trader — MODE NOT SET · ACTIVE"


def test_status_says_the_live_model_is_live_under_the_paper_waiver(db, paper):
    """A model promoted under the paper-only measurement waiver reads as such in /status:
    it failed its gates, so the operator is told a move to live is refused while it stands.
    tests/test_registry.py covers the waiver itself and tests/test_ladder.py the refusal."""
    registry_fake.waive(db)

    [status] = ask(db, "/status")

    assert_style(status)
    assert re.fullmatch(
        rf"  model {registry_fake.FAILED_ID} live under a paper waiver\n"
        rf"    certification \d+, promoted {ET} by operator; it failed its gates,"
        rf" waived {ET}, so a move to live is refused until another is promoted",
        blocks(status)[4],
    )


def test_pnl_for_a_month_runs_from_the_last_reading_before_it(db, paper):
    conn = db.connect("execution")
    for at, equity in [
        (datetime(2026, 9, 30, 21, 0, tzinfo=UTC), 100000),
        (datetime(2026, 10, 15, 21, 0, tzinfo=UTC), 101000),
        (datetime(2026, 10, 30, 21, 0, tzinfo=UTC), 102500),
        (datetime(2026, 11, 2, 21, 0, tzinfo=UTC), 90000),
    ]:
        conn.execute(
            "INSERT INTO agb.broker_state (observed_at, account_status, suspend_trade,"
            " no_shorting, trading_blocked, account_blocked, equity_usd, actor)"
            " VALUES (%s, 'ACTIVE', false, false, false, false, %s, 'execution')",
            (at, equity),
        )

    [october, december] = ask(db, "/pnl 2026-10", "/pnl 2026-12")

    assert_style(october)
    assert blocks(october) == [
        "PNL — 2026-10",
        "  return +2.50%\n    equity $100,000.00 to $102,500.00, $2,500.00",
        "  window\n    2026-09-30 17:00 ET to 2026-10-30 17:00 ET",
        "  20 fills\n    fees $0.00",
        "  backtest expectation\n    momentum-fake recorded no monthly distribution",
    ]
    assert blocks(december) == [
        "PNL — 2026-12",
        "  no equity readings\n    execution has not read the account",
    ]


def test_pnl_reads_the_month_against_the_backtest_with_paper_dividends_added_back(db, paper):
    """The realised return, the same return with the dividends the paper broker never
    credited, and what the promoted certification's median path expected over as many
    months: three figures the operator can read against each other."""
    conn = db.connect("execution")
    for at, equity in [
        (datetime(2026, 9, 30, 21, 0, tzinfo=UTC), 100000),
        (datetime(2026, 10, 30, 21, 0, tzinfo=UTC), 102500),
    ]:
        conn.execute(
            "INSERT INTO agb.broker_state (observed_at, account_status, suspend_trade,"
            " no_shorting, trading_blocked, account_blocked, equity_usd, actor)"
            " VALUES (%s, 'ACTIVE', false, false, false, false, %s, 'execution')",
            (at, equity),
        )
    # The add-back only applies to months the system was in paper, which it reads from the
    # mode in force at each closing reading, so the paper event has to precede them.
    mode(db, "paper")
    promote(db, distribution(monthly=0.01))
    dividend(db, "2026-10", "AAA", "400.00", "2026-10-20")

    [october] = ask(db, "/pnl 2026-10")

    assert_style(october)
    assert blocks(october)[1] == (
        "  return +2.50%\n    equity $100,000.00 to $102,500.00, $2,500.00"
    )
    assert blocks(october)[-2:] == [
        "  return with dividends +2.90%\n    $400.00 recorded over 1 paper month and added"
        " back; the paper broker credits none",
        "  backtest expectation +1.00%\n    +1.00% a month over 1 month, the median of"
        " 240 certified paths",
    ]


def test_before_anything_has_run_each_command_says_so(db):
    replies = ask(db, "/status", "/positions", "/pnl", "/basket", "/gate", "/log")

    for reply in replies:
        assert_style(reply)
    assert [blocks(reply)[0] for reply in replies] == [
        "agb-trader — PAPER · ACTIVE",
        "POSITIONS — no snapshot yet",
        "PNL — since the first reading",
        "BASKET — none yet",
        "GATE — no run yet",
        "LOG — last 9 events, oldest first",
    ]
    assert blocks(replies[0])[1:] == [
        "  account not yet observed\n    execution has not read the broker",
        "  no positions snapshot yet\n    execution snapshots after each close",
        "  no rebalance yet\n    no live basket has been proposed",
        "  no model promoted\n    rebalances fail until `agb promote` runs",
        "  no rebalance scheduled\n    the scheduler has none due",
    ]


def test_every_message_is_a_command_row_with_its_outcome(db):
    """Acceptance criterion 3: valid commands, unknown verbs, malformed arguments, commands a
    later ticket brings, and text that is not a command are each recorded, and each refusal
    is answered with one line: the reason, and where /help is."""
    texts = [
        "/status",
        "/STATUS@agb_trader_bot",
        "/log 3",
        "/pnl 2026-10",
        "/frobnicate now",
        "/log 500",
        "/pnl October",
        "/positions N01",
        "/why 2026-10",
        "/level 2",
        "what is going on",
        "/",
        None,
    ]
    replies = ask(db, *texts)

    expected = [
        ("status", [], "accepted", None),
        ("status", [], "accepted", None),
        ("log", ["3"], "accepted", None),
        ("pnl", ["2026-10"], "accepted", None),
        ("frobnicate", ["now"], "refused", "unknown command /frobnicate"),
        ("log", ["500"], "refused", "usage: /log [1-100]"),
        ("pnl", ["October"], "refused", "usage: /pnl [YYYY-MM]"),
        ("positions", ["N01"], "refused", "usage: /positions, no arguments"),
        ("why", ["2026-10"], "refused", "usage: /why <TICKER>"),
        ("level", ["2"], "refused", "usage: /level, no arguments"),
        (None, ["what", "is", "going", "on"], "refused", "not a command"),
        (None, [], "refused", "not a command"),
        (None, [], "refused", "not a command"),
    ]
    assert commands(db) == [
        (
            f"telegram:{SENDER}",
            "telegram",
            text or "",
            verb,
            {"args": args},
            outcome,
            reason,
            CHAT,
            100 + i,
        )
        for i, (text, (verb, args, outcome, reason)) in enumerate(zip(texts, expected, strict=True))
    ]
    assert len(replies) == len(texts)
    for reply, (_, _, outcome, reason) in zip(replies, expected, strict=True):
        assert_style(reply)
        if outcome == "refused":
            assert reply == f"{reason} — /help lists the commands"
            assert "\n" not in reply
    assert blocks(replies[0])[0] == blocks(replies[1])[0] == "agb-trader — PAPER · ACTIVE"


def test_the_parser_reads_the_verb_and_checks_the_arguments():
    assert parse("/start") == parse("/help")
    assert parse("/log 100").refusal is None
    assert parse("/log 0").refusal == "usage: /log [1-100]"
    assert parse("/log 1 2").refusal == "usage: /log [1-100]"
    assert parse("/pnl 2026-13").refusal == "usage: /pnl [YYYY-MM]"
    assert parse("  /gate  ").refusal is None
    assert parse("/g-te").refusal == "not a command"


def test_a_message_from_another_chat_is_recorded_and_never_answered(db):
    telegram = FakeTelegram()
    say(telegram, "/positions", chat="999", sender=31)
    Bot(telegram, CHAT, lambda: AFTER).poll(db.connect("bot"), AFTER)

    assert telegram.sent == []
    assert commands(db) == [
        ("telegram:31", "telegram", "/positions", "positions", {"args": []}, "refused",
         "not the configured chat", "999", 100),
    ]  # fmt: skip


def test_an_unknown_command_from_the_operator_gets_one_line_pointing_at_help(db):
    [reply] = ask(db, "/frobnicate now")
    assert reply == "unknown command /frobnicate — /help lists the commands"
    assert help_text() not in reply
    # A long verb is echoed cut short, not at its full length.
    [long] = ask(db, "/" + "a" * 3000)
    assert len(long) < 300 and long.endswith(" — /help lists the commands")


def test_a_thousand_messages_from_another_chat_write_a_bounded_number_of_rows_and_send_nothing(db):
    telegram = FakeTelegram()
    say(telegram, *["/status"] * 1000, chat="999", sender=31)
    clock = [AFTER]
    bot = Bot(telegram, CHAT, lambda: clock[0])
    conn = db.connect("bot")
    bot.poll(conn, AFTER)

    assert telegram.sent == []
    refused = commands(db)
    # The limit's worth of rows, then one summary row that names the limit and carries no text.
    assert len(refused) == REFUSAL_LIMIT + 1
    assert all(row[5:8] == ("refused", "not the configured chat", "999") for row in refused[:-1])
    *_, (actor, _, text, verb, args, outcome, reason, chat, _) = refused
    assert (actor, text, verb, args, outcome, chat) == (
        "telegram:31", "", None, {}, "refused", "999"
    )  # fmt: skip
    assert reason.startswith(f"refusal limit: {REFUSAL_LIMIT} refused messages from chat 999")
    # Past the limit not even the update is kept: the table does not grow with the flood.
    (claimed,) = conn.execute("SELECT count(*) FROM agb.telegram_update").fetchone()
    assert claimed == REFUSAL_LIMIT + 1

    # The same flood the next minute adds nothing, and the hour after it starts again.
    for update in telegram.updates:
        update["update_id"] += 10_000
    clock[0] = AFTER + timedelta(minutes=1)
    bot.poll(conn, clock[0])
    assert len(commands(db)) == REFUSAL_LIMIT + 1
    for update in telegram.updates:
        update["update_id"] += 10_000
    clock[0] = AFTER + REFUSAL_WINDOW
    bot.poll(conn, clock[0])
    assert len(commands(db)) == 2 * (REFUSAL_LIMIT + 1)
    assert telegram.sent == []


def test_a_flooding_chat_does_not_use_up_another_chats_rows(db):
    telegram = FakeTelegram()
    say(telegram, *["hi"] * (REFUSAL_LIMIT + 5), chat="999")
    say(telegram, "hi", chat="998")
    Bot(telegram, CHAT, lambda: AFTER).poll(db.connect("bot"), AFTER)
    assert [row[7] for row in commands(db)].count("998") == 1


def test_the_operators_refusals_are_limited_too_but_still_answered(db):
    telegram = FakeTelegram()
    say(telegram, *["hello"] * (REFUSAL_LIMIT + 3))
    Bot(telegram, CHAT, lambda: AFTER).poll(db.connect("bot"), AFTER)
    assert len(telegram.sent) == REFUSAL_LIMIT + 3
    assert len(commands(db)) == REFUSAL_LIMIT + 1
    assert commands(db)[-1][5] == "refused"


def test_only_the_human_surfaces_write_commands_and_nothing_changes_one(db):
    insert_command = (
        "INSERT INTO agb.command (actor, surface, raw_text, verb, outcome, reason)"
        " VALUES ('telegram:1', 'telegram', '/status', 'status', %s, %s)"
    )
    for role in ("bot", "ui", "cli"):
        db.connect(role).execute(insert_command, ("accepted", None))
    for role in ("pipeline", "execution", "scheduler"):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect(role).execute(insert_command, ("accepted", None))
    # A refusal always carries its reason, and an accepted command never has one.
    with pytest.raises(errors.CheckViolation):
        db.connect("bot").execute(insert_command, ("refused", None))
    with pytest.raises(errors.CheckViolation):
        db.connect("bot").execute(insert_command, ("accepted", "why"))
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("bot").execute("UPDATE agb.command SET outcome = 'refused'")


# The gate and the log.


def test_after_a_gate_halt_gate_leads_with_the_failing_check_and_its_measurement(db):
    """Acceptance criteria 2 and 6: /gate shows what each check measured and leads with the
    check that failed; the bot raises one gate-halt alert naming it."""
    basket_id = insert(db, replace(BASE, universe_size=1499))
    run(db, basket_id)
    assert state(db, basket_id)[0] == "halted"

    [gate] = ask(db, "/gate")

    assert_style(gate)
    assert blocks(gate)[1] == "  FAIL  universe_size\n          measured 1499, needs 1500 to 3500"
    assert blocks(gate)[2] == "  ok  snapshot_date\n        measured 2026-09-30, needs 2026-09-30"
    assert blocks(gate)[-1] == "1 of 109 checks failed. The rebalance was halted."

    telegram = FakeTelegram()
    cycle(db, telegram)
    cycle(db, telegram)

    assert ("gate_halt", PERIOD, PERIOD) in alerts(db)
    halted = body(db, "gate_halt")
    assert halted == (
        f"HALTED — rebalance {PERIOD} was not submitted.\n\n"
        "  FAIL  universe_size\n          measured 1499, needs 1500 to 3500\n\n"
        f"{readout.HALT_FOOTER}\n\n/gate for every check"
    )
    assert_style(halted)
    assert [text for _, text in telegram.sent].count(halted) == 1
    assert [e[:3] for e in events(db) if e[0] == "gate_halt"] == [
        ("gate_halt", "delivered", "telegram")
    ]


def test_a_failed_broker_check_raises_the_gate_halt_alert_too(db):
    conn = db.connect("execution")
    target = approved(db)
    assert (
        execution.submit(conn, broker(suspend_trade=True), target, lambda: NOW, CUTOFF).state
        == "halted"
    )

    cycle(db, FakeTelegram())

    halted = body(db, "gate_halt")
    assert blocks(halted)[1] == "  FAIL  suspend_trade\n          measured true, needs false"
    [gate] = ask(db, "/gate")
    assert blocks(gate)[1] == blocks(halted)[1]
    assert blocks(gate)[-1] == "1 of 153 checks failed. The rebalance was halted."


def test_log_reads_the_audit_view_and_shows_the_system_as_an_actor(db, paper):
    """Acceptance criterion 2: /log reads `agb.audit_log`, oldest first, and the gate's
    approval appears with `system` as its actor."""
    # A hundred events run past Telegram's limit, so the answer comes in parts.
    everything = "\n\n".join(ask(db, "/log 100"))
    [three] = ask(db, "/log 3")

    assert re.search(
        rf"\n\n  {ET}  basket approved\n    by system · live basket {PERIOD}"
        r" · passed the pre-trade gate in paper mode\n\n",
        everything,
    )
    assert re.search(
        rf"\n\n  {ET}  basket filled\n    by execution · live basket {PERIOD} · 20 of 20 orders",
        everything,
    )

    assert_style(three)
    header, *items = blocks(three)
    assert header == "LOG — last 3 events, oldest first"
    assert len(items) == 3
    # The command asking is on record before it is answered.
    assert re.fullmatch(
        rf"  {ET}  command accepted\n    by telegram:7 · telegram /log 3", items[-1]
    )
    assert re.fullmatch(
        rf"  {ET}  command accepted\n    by telegram:7 · telegram /log 100", items[-2]
    )


def test_a_reply_longer_than_telegram_takes_goes_out_in_parts(db, paper):
    telegram = FakeTelegram()
    say(telegram, "/log 100")
    Bot(telegram, CHAT, lambda: AFTER).poll(db.connect("bot"), AFTER)

    parts = [text for _, text in telegram.sent]
    assert len(parts) > 1
    assert all(len(part) <= MESSAGE_MAX for part in parts)
    assert "\n\n".join(parts) == readout.log(db.connect("bot"), 100)


def test_split_cuts_between_items_and_only_cuts_an_item_longer_than_the_limit():
    text = "\n\n".join(["header", *(f"  item {i}\n    {'x' * 30}" for i in range(10))])
    parts = split(text, 100)
    assert all(len(part) <= 100 for part in parts)
    assert "\n\n".join(parts) == text
    assert all(part.startswith(("header", "  item")) for part in parts)

    assert split("a" * 250, 100) == ["a" * 100, "a" * 100, "a" * 50]
    assert split("short") == ["short"]


# The fill confirmation and the mismatch alert.


def test_the_fill_confirmation_arrives_after_the_close_and_only_once(db):
    """Acceptance criterion 4: nothing while the orders wait for the close; once the basket
    is filled, one confirmation with what was traded, delivered like any alert."""
    conn = db.connect("execution")
    target = approved(db)
    fake = broker()
    execution.submit(conn, fake, target, lambda: NOW, CUTOFF)
    telegram = FakeTelegram()

    cycle(db, telegram, NOW)
    assert "fill_confirmation" not in [kind for kind, _, _ in alerts(db)]

    fake.close(CLOSE_AT)
    execution.reconcile(conn, fake, target, AFTER)
    cycle(db, telegram)
    cycle(db, telegram)

    assert ("fill_confirmation", PERIOD, PERIOD) in alerts(db)
    filled = body(db, "fill_confirmation")
    assert_style(filled)
    names = ", ".join(f"N{i:02d}" for i in range(1, 21))
    assert blocks(filled)[:4] == [
        f"REBALANCE {PERIOD} filled at the close.",
        "  sold 0\n    nothing",
        f"  bought 20\n    {names}",
        "  20 of 20 orders filled in full\n"
        "    shortfall +0.0 bp vs mid, modelled 0.0 bp; $96,000.00 traded, fees $0.00",
    ]
    assert re.fullmatch(rf"  approved by system\n    at {ET}", blocks(filled)[4])
    assert blocks(filled)[5] == "/positions"
    assert [text for _, text in telegram.sent].count(filled) == 1
    assert [e[:3] for e in events(db) if e[0] == "fill_confirmation"] == [
        ("fill_confirmation", "delivered", "telegram")
    ]


class Canned:
    """A connection that answers each query with the rows given for it, for states the
    schema does not let a test write."""

    def __init__(self, answers):
        self.answers = answers

    def execute(self, query, params=None):
        self.rows = self.answers[query]
        return self

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


def filled(*orders):
    return readout.filled_text(
        Canned({readout._ORDERS: list(orders), readout._APPROVED: []}), 1, PERIOD
    )


@pytest.mark.parametrize(
    "unmeasured",
    [
        ("N02", "buy", Decimal(10), Decimal(10), Decimal(100), None, Decimal(2), Decimal(0)),
        ("N02", "buy", Decimal(10), Decimal(10), Decimal(100), Decimal(5), None, Decimal(0)),
    ],
    ids=["no shortfall", "no modelled cost"],
)
def test_the_fill_confirmation_weighs_only_the_orders_it_can_measure(unmeasured):
    measured = ("N01", "buy", Decimal(10), Decimal(10), Decimal(100), Decimal(5), Decimal(2), 0)

    assert blocks(filled(measured, unmeasured))[3] == (
        "  2 of 2 orders filled in full\n"
        "    shortfall +5.0 bp vs mid, modelled 2.0 bp (1 not measured);"
        " $2,000.00 traded, fees $0.00"
    )
    assert blocks(filled(unmeasured))[3] == (
        "  1 of 1 orders filled in full\n    shortfall not measured; $1,000.00 traded, fees $0.00"
    )


def test_a_position_the_fills_do_not_explain_raises_a_mismatch_alert(db, paper):
    """Acceptance criterion 5: the broker's book disagrees with the previous snapshot plus
    the fills, and the bot raises one alert naming the symbol and both quantities."""
    fake, _ = paper
    telegram = FakeTelegram()
    cycle(db, telegram)
    assert "position_mismatch" not in [kind for kind, _, _ in alerts(db)]

    fake.book["N03"] = replace(fake.book["N03"], qty=Decimal(200))
    fake.now = CALENDAR.close_at(NEXT)
    execution.snapshot(db.connect("execution"), fake, NEXT, CALENDAR)
    cycle(db, telegram)
    cycle(db, telegram)

    assert ("position_mismatch", "N03", NEXT.isoformat()) in alerts(db)
    mismatch = body(db, "position_mismatch")
    assert_style(mismatch)
    assert blocks(mismatch)[1] == (
        "  position mismatch: N03\n"
        "    broker holds 200, expected 240 from the previous snapshot plus fills;"
        f" snapshot {NEXT}"
    )
    assert [text for _, text in telegram.sent].count(mismatch) == 1


# Acknowledging an alert (issue #209).


def raise_alert(db, kind="halt_open", subject="pause", period=PERIOD, at=AFTER):
    """An alert as the bot raises one, and its id, which is what /ack takes."""
    (ident,) = (
        db.connect("bot")
        .execute(
            "INSERT INTO agb.alert (kind, subject, period, body, raised_at, raised_by)"
            " VALUES (%s, %s, %s, 'the kill switch is on', %s, 'bot') RETURNING id",
            (kind, subject, period, at - timedelta(hours=1)),
        )
        .fetchone()
    )
    return ident


def acknowledged(db):
    """Every acknowledgement, as (alert, channel, actor, chat) and oldest first."""
    return rows(
        db,
        "SELECT alert_id, channel, actor, chat_id FROM agb.alert_event"
        " WHERE event = 'acknowledged' ORDER BY id",
    )


def test_the_parser_checks_that_ack_takes_one_whole_number():
    assert parse("/ack 12").refusal is None
    for text in ("/ack", "/ack 1 2", "/ack x", "/ack -1", "/ack 1.5", "/ack " + "9" * 30):
        assert parse(text).refusal == "usage: /ack <alert id>", text


def test_ack_acknowledges_the_alert_once_naming_who_and_where(db):
    ident = raise_alert(db)

    [reply] = ask(db, f"/ack {ident}")

    assert reply == f"Alert {ident} (halt_open, pause {PERIOD}) acknowledged by telegram:{SENDER}."
    assert acknowledged(db) == [(ident, "telegram", f"telegram:{SENDER}", CHAT)]
    assert commands(db) == [
        (
            f"telegram:{SENDER}",
            "telegram",
            f"/ack {ident}",
            "ack",
            {"alert": ident},
            "accepted",
            None,
            CHAT,
            100,
        )
    ], "one command row, recorded once"


def test_a_second_ack_is_refused_by_name_and_writes_no_second_event(db):
    ident = raise_alert(db)

    first, second = ask(db, f"/ack {ident}", f"/ack {ident}")

    assert first.endswith(f"acknowledged by telegram:{SENDER}.")
    assert re.fullmatch(
        rf"Alert {ident} \(halt_open, pause {PERIOD}\) was already acknowledged by "
        rf"telegram:{SENDER} from telegram at {ET}\. Nothing changed\.",
        second,
    )
    assert len(acknowledged(db)) == 1
    assert [(row[3], row[5]) for row in commands(db)] == [("ack", "accepted"), ("ack", "refused")]


def test_ack_of_an_alert_that_does_not_exist_is_refused_and_recorded(db):
    ident = raise_alert(db)

    [reply] = ask(db, f"/ack {ident + 50}")

    assert reply == f"There is no alert {ident + 50}; nothing was acknowledged."
    assert acknowledged(db) == []
    [command] = commands(db)
    assert (command[3], command[5], command[6]) == ("ack", "refused", reply)


def test_ack_with_no_id_or_a_bad_one_is_refused_with_its_usage(db):
    raise_alert(db)

    replies = ask(db, "/ack", "/ack soon")

    assert replies == ["usage: /ack <alert id> — /help lists the commands"] * 2
    assert acknowledged(db) == []
    assert [(row[3], row[5]) for row in commands(db)] == [("ack", "refused")] * 2


def test_ack_from_another_chat_is_refused_and_acknowledges_nothing(db):
    ident = raise_alert(db)
    telegram = FakeTelegram()
    say(telegram, f"/ack {ident}", chat="999", sender=31)

    Bot(telegram, CHAT, lambda: AFTER).poll(db.connect("bot"), AFTER)

    assert telegram.sent == []
    assert acknowledged(db) == []
    [command] = commands(db)
    assert command[:2] == ("telegram:31", "telegram") and command[5:7] == (
        "refused",
        "not the configured chat",
    )


def test_an_ack_in_the_ui_and_one_in_telegram_acknowledge_an_alert_once_between_them(db):
    ident = raise_alert(db)
    ui_source = approval.Source("ui:operator", "ui", f"/ack {ident}")
    reply = acknowledge(db.connect("ui"), ui_source, str(ident), AFTER)
    assert reply.endswith("acknowledged by ui:operator.")

    [late] = ask(db, f"/ack {ident}")

    assert "already acknowledged by ui:operator from ui at" in late
    assert acknowledged(db) == [(ident, "ui", "ui:operator", None)]


def test_status_counts_the_unacknowledged_alerts_and_names_the_newest_to_acknowledge(db):
    [none] = ask(db, "/status")
    assert "alerts of the last" not in none

    old = raise_alert(db, "gate_halt", "rebalance", "2026-09", at=AFTER - timedelta(days=9))
    seen = raise_alert(db, "partial_rebalance", "live basket", PERIOD)
    newest = raise_alert(db, "halt_open", "pause", PERIOD)
    ask(db, f"/ack {seen}")

    [status] = ask(db, "/status")

    assert_style(status)
    assert (
        f"  1 of 2 alerts of the last 7 days unacknowledged\n"
        f"    newest is {newest}, halt_open for pause {PERIOD}: /ack {newest}"
    ) in status
    assert f"/ack {old}" not in status, "an alert older than the window is not counted"

    ask(db, f"/ack {newest}")
    [status] = ask(db, "/status")
    assert "  all 2 alerts of the last 7 days acknowledged\n    nothing awaits you" in status
