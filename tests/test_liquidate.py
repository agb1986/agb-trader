"""`/liquidate` end to end. The bot issues a token with a summary of what closing every
position means; the token typed back in time opens the `liquidation` halt cause with the
confirmation; execution calls the broker's close-all and records its per-symbol answer; the
result comes back to the chat as an alert, and the cause stays open until `/resume`. The
broker is `alpaca_fake.FakeBroker`, Telegram is `test_bot.FakeTelegram`, and the client's
parsing of Alpaca's 207 Multi-Status runs against a fake opener.
"""

import io
import json
import re
import urllib.error
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from psycopg import errors
from psycopg.types.json import Jsonb
from test_bot import CHAT, UPDATE_IDS
from test_execution import (
    AFTER,
    CALENDAR,
    MID,
    NAMES,
    NEXT,
    NOW,
    TRADE,
    Clock,
    broker,
    rows,
    state,
    submitted,
)
from test_pause_resume import (
    ACTOR,
    BEFORE_DEADLINE,
    OPERATOR,
    Operator,
    commands,
    kinds,
    open_causes,
)

from agb import execution
from agb.alpaca import Alpaca, AlpacaError, Closed
from agb.execution import ExecutionError
from agb.services.bot import TOKEN_VALID, parse
from agb.services.execution import Execution

TOKEN = re.compile(r"^/liquidate ([a-z0-9]{6})$", re.MULTILINE)
HELD = {NAMES[0]: Decimal(10), NAMES[1]: Decimal(4), NAMES[2]: Decimal(7)}


def holding(**changes):
    """A broker holding `HELD`."""
    fake = broker(**changes)
    for symbol, qty in HELD.items():
        fake._hold(symbol, qty, MID)
    return fake


@pytest.fixture
def clock():
    return Clock(NOW)


@pytest.fixture
def operator(db, clock):
    return Operator(db, clock)


def observed(db, fake):
    """One execution cycle after the close: the positions snapshot and the account state the
    step-one summary reads."""
    Execution(fake, clock=Clock(AFTER))(db.connect("execution"))


def issued(reply: str) -> str:
    [token] = TOKEN.findall(reply)
    return token


def liquidations(db):
    return rows(
        db,
        "SELECT kind, follows IS NOT NULL, command_id IS NOT NULL, actor"
        " FROM agb.liquidation ORDER BY id",
    )


def result(db):
    [(symbols, detail)] = rows(
        db, "SELECT symbols, detail FROM agb.liquidation WHERE kind = 'result'"
    )
    return symbols, detail


def alert(db, kind):
    [(body,)] = rows(db, "SELECT body FROM agb.alert WHERE kind = %s", kind)
    return body


def cycle(db, fake, now=NOW):
    Execution(fake, clock=Clock(now))(db.connect("execution"))


# -- step one ------------------------------------------------------------------------------


def test_step_one_prints_the_summary_and_a_token(db, operator):
    """Acceptance criterion (a), first half: position count, approximate equity, open
    orders cancelled first, not undoable, no re-entry until the next rebalance, and a short
    random token valid for five minutes."""
    observed(db, holding())

    reply = operator.says("/liquidate")

    lines = reply.splitlines()
    assert lines[0] == "LIQUIDATE — step 1 of 2"
    assert lines[1].startswith("positions: 3 held, worth $420.00 at the 2026-10-01 snapshot")
    assert re.fullmatch(r"equity: about \$100,000\.00, read \d{4}-\d\d-\d\d \d\d:\d\d ET", lines[2])
    assert lines[3:6] == [
        "Open orders are cancelled first, then every position is closed at market.",
        "This cannot be undone.",
        "The system will not re-enter these names until the next scheduled rebalance.",
    ]
    assert "within 5 minutes, by 2026-10-01 15:40 ET" in reply
    token = issued(reply)
    assert lines[-1] == "A wrong or late token is refused, and you start again from /liquidate."

    [(kind, stored, expires_at, detail, actor)] = rows(
        db, "SELECT kind, token, expires_at, detail, actor FROM agb.liquidation"
    )
    assert (kind, stored, actor) == ("token", token, ACTOR)
    assert expires_at == NOW + TOKEN_VALID
    assert token not in detail
    assert commands(db) == [
        (ACTOR, "telegram", "/liquidate", "liquidate", {"step": "token"}, "accepted", None)
    ]
    # Nothing is paused and nothing reaches the broker at step one.
    assert open_causes(db) == []
    assert execution.pending_liquidation(db.connect("execution")) == []


def test_step_one_says_so_when_nothing_has_been_observed(db, operator):
    reply = operator.says("/liquidate")

    assert "positions: no snapshot yet, so the count is unknown" in reply
    assert "equity: not read from the broker yet" in reply
    issued(reply)


def test_a_token_reply_that_fails_to_send_is_retried_without_a_second_token(db, operator, clock):
    """A reply Telegram would not take is queued and sent again as it was: the token is not
    issued a second time, and its five minutes are not restarted by the retry."""
    observed(db, holding())
    operator.telegram.fail_once_on = 1
    operator.bot.retry_after = timedelta(0)
    operator.telegram.updates.append(
        {
            "update_id": next(UPDATE_IDS),
            "message": {
                "chat": {"id": int(CHAT)},
                "from": {"id": OPERATOR},
                "date": int(clock().timestamp()),
                "text": "/liquidate",
            },
        }
    )
    operator.bot.poll(db.connect("bot"))
    assert operator.telegram.sent == []
    [(_, token, expires_at)] = rows(db, "SELECT kind, token, expires_at FROM agb.liquidation")
    assert expires_at == NOW + TOKEN_VALID

    # The retry comes two minutes later, inside the token's life.
    clock.now = NOW + timedelta(minutes=2)
    [reply] = [text for text in operator.cycle() if text.startswith("LIQUIDATE")]
    assert issued(reply) == token
    assert "within 5 minutes, by 2026-10-01 15:40 ET" in reply
    # One token, the one first issued, with the expiry it was first given.
    assert rows(db, "SELECT token, expires_at FROM agb.liquidation") == [(token, expires_at)]
    # The command was recorded once, and a further cycle sends nothing.
    assert len(commands(db)) == 1
    assert not [text for text in operator.cycle() if text.startswith("LIQUIDATE")]


def test_each_token_is_random(db, operator):
    tokens = {issued(operator.says("/liquidate")) for _ in range(5)}
    assert len(tokens) == 5


# -- step two ------------------------------------------------------------------------------


def test_the_right_token_in_time_pauses_first_then_liquidates(db, operator, clock):
    """Acceptance criteria (a), (c), (e) and (f): the token typed back within five minutes
    opens the liquidation cause before execution calls close-all; the result is reported
    per symbol and the cause stays open until /resume. Every step is a command row and a
    liquidation row."""
    fake = holding()
    observed(db, fake)
    at_close_all = []
    fake.on_close_all = lambda _: at_close_all.append(open_causes(db))
    token = issued(operator.says("/liquidate"))

    clock.now = NOW + TOKEN_VALID - timedelta(seconds=1)
    reply = operator.says(f"/liquidate {token}")

    assert reply == (
        "LIQUIDATING — step 2 of 2\n"
        "halt cause liquidation: open. No further order goes out.\n"
        "Execution now cancels open orders and closes every position at market. The "
        "broker's answer arrives here symbol by symbol, failures included.\n"
        "The liquidation cause stays open until /resume."
    )
    assert open_causes(db) == [("liquidation", ACTOR)]
    assert rows(db, "SELECT reason FROM agb.halt_event") == [("/liquidate",)]
    assert [c.actor for c in execution.pending_liquidation(db.connect("execution"))] == [ACTOR]
    assert ("close_all",) not in fake.calls

    cycle(db, fake, clock.now)

    # (c) The latch was open when the broker was asked, and suspend_trade was left alone,
    # since it would refuse the closing orders.
    assert at_close_all == [[("liquidation", ACTOR)]]
    assert fake.calls.count(("close_all",)) == 1
    assert ("suspend", True) not in fake.calls
    assert execution.pending_liquidation(db.connect("execution")) == []

    symbols, detail = result(db)
    assert [(s["symbol"], s["status"], s["message"]) for s in symbols] == [
        (name, 200, None) for name in HELD
    ]
    assert all(s["order_id"] for s in symbols)
    lines = detail.splitlines()
    assert lines[0].startswith(f"LIQUIDATION by {ACTOR} at 20")
    assert lines[1:3] == ["all 3 positions closing", "open orders cancelled by the broker first"]
    assert [line.split(":")[0] for line in lines[3:6]] == list(HELD)
    assert lines[-1] == "halt cause liquidation: stays open until /resume"
    assert alert(db, "liquidation") == detail

    # The bot delivers the result, and the cause stays open until /resume.
    assert detail in operator.cycle()
    cycle(db, fake, clock.now + timedelta(minutes=1))
    assert fake.calls.count(("close_all",)) == 1
    assert open_causes(db) == [("liquidation", ACTOR)]

    assert operator.says("/resume").splitlines()[1] == "cleared: liquidation"
    assert open_causes(db) == []

    # (f) Token, confirmation and result are liquidation rows; every step is a command row.
    assert liquidations(db) == [
        ("token", False, True, ACTOR),
        ("confirm", True, True, ACTOR),
        ("result", True, False, "execution"),
    ]
    assert [(verb, args, outcome) for _, _, _, verb, args, outcome, _ in commands(db)] == [
        ("liquidate", {"step": "token"}, "accepted"),
        ("liquidate", {"step": "confirm", "token_row": 1}, "accepted"),
        ("resume", {"cleared": ["liquidation"]}, "accepted"),
    ]


def test_a_token_is_accepted_in_capitals(db, operator):
    """A phone keyboard may capitalise the token; the letters are what count."""
    token = issued(operator.says("/liquidate"))

    assert operator.says(f"/liquidate {token.upper()}").startswith("LIQUIDATING")


def test_a_wrong_token_is_refused_recorded_and_spends_the_token(db, operator):
    """Acceptance criterion (b): a wrong token refuses, is recorded, and is not retried: the
    right token afterwards is refused too, since the wrong one spent it."""
    fake = holding()
    token = issued(operator.says("/liquidate"))

    reply = operator.says("/liquidate zzzzzz" if token != "zzzzzz" else "/liquidate yyyyyy")

    assert reply == (
        "LIQUIDATE refused\n"
        "wrong liquidation token; it is spent, send /liquidate to start again\n"
        "Nothing has been done."
    )
    assert operator.says(f"/liquidate {token}").splitlines()[1] == (
        "no liquidation token is outstanding: send /liquidate first"
    )
    cycle(db, fake)

    assert open_causes(db) == []
    assert ("close_all",) not in fake.calls
    assert liquidations(db) == [("token", False, True, ACTOR), ("refused", True, True, ACTOR)]
    assert rows(db, "SELECT detail FROM agb.liquidation WHERE kind = 'refused'") == [
        ("wrong liquidation token; it is spent, send /liquidate to start again",)
    ]
    assert [(args, outcome, reason) for *_, args, outcome, reason in commands(db)] == [
        ({"step": "token"}, "accepted", None),
        (
            {"step": "refused", "token_row": 1},
            "refused",
            "wrong liquidation token; it is spent, send /liquidate to start again",
        ),
        (
            {"step": "refused"},
            "refused",
            "no liquidation token is outstanding: send /liquidate first",
        ),
    ]


def test_a_token_with_non_ascii_characters_is_refused_not_a_crash(db, operator):
    """What the operator types is whatever they typed. A token comparison that raises takes
    the bot's process down with it, and the unacknowledged update is read again on restart,
    so the service never comes back."""
    token = issued(operator.says("/liquidate"))

    reply = operator.says("/liquidate \u043f\u0430\u0440\u043e\u043b\u044c")

    assert reply.splitlines()[1] == (
        "wrong liquidation token; it is spent, send /liquidate to start again"
    )
    assert operator.says(f"/liquidate {token}").splitlines()[1] == (
        "no liquidation token is outstanding: send /liquidate first"
    )
    assert liquidations(db) == [("token", False, True, ACTOR), ("refused", True, True, ACTOR)]


def test_an_expired_token_is_refused_recorded_and_spent(db, operator, clock):
    """Acceptance criterion (b): at five minutes the token has expired."""
    fake = holding()
    token = issued(operator.says("/liquidate"))

    clock.now = NOW + TOKEN_VALID
    reply = operator.says(f"/liquidate {token}")

    assert reply.splitlines()[1] == (
        "the liquidation token expired at 2026-10-01 15:40 ET; it is spent, "
        "send /liquidate to start again"
    )
    assert operator.says(f"/liquidate {token}").splitlines()[0] == "LIQUIDATE refused"
    cycle(db, fake, clock.now)

    assert open_causes(db) == []
    assert ("close_all",) not in fake.calls
    assert liquidations(db) == [("token", False, True, ACTOR), ("refused", True, True, ACTOR)]
    assert [outcome for *_, outcome, _ in commands(db)] == ["accepted", "refused", "refused"]


def test_a_new_token_supersedes_the_last(db, operator):
    """Only the latest token counts: the earlier one typed back is a wrong token, and it
    spends the latest, so the operator starts again."""
    first = issued(operator.says("/liquidate"))
    second = issued(operator.says("/liquidate"))
    assert first != second

    assert operator.says(f"/liquidate {first}").splitlines()[1] == (
        "wrong liquidation token; it is spent, send /liquidate to start again"
    )
    assert operator.says(f"/liquidate {second}").splitlines()[0] == "LIQUIDATE refused"
    assert open_causes(db) == []
    assert [kind for kind, *_ in liquidations(db)] == ["token", "token", "refused"]


def test_a_token_without_a_step_one_is_refused_and_recorded(db, operator):
    reply = operator.says("/liquidate abcdef")

    assert reply.splitlines()[:2] == [
        "LIQUIDATE refused",
        "no liquidation token is outstanding: send /liquidate first",
    ]
    assert liquidations(db) == []
    [(_, _, text, verb, args, outcome, _)] = commands(db)
    assert (text, verb, args, outcome) == (
        "/liquidate abcdef",
        "liquidate",
        {"step": "refused"},
        "refused",
    )


def test_two_arguments_are_refused_as_usage():
    assert parse("/liquidate a b").refusal == "usage: /liquidate [token]"
    assert parse("/liquidate").refusal is None
    assert parse("/liquidate abc123").refusal is None


def test_help_lists_liquidate(db, operator):
    assert "/liquidate [token]" in operator.says("/help")


# -- the broker's answer ---------------------------------------------------------------------


def test_a_symbol_the_broker_cannot_close_is_reported_as_failed(db, operator):
    """Acceptance criterion (d): a per-symbol failure inside the 207 appears in the result
    message and the result row, and the rest still close."""
    halted = NAMES[1]
    fake = holding(close_fails={halted: "asset is halted"})
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    cycle(db, fake)

    symbols, detail = result(db)
    assert {s["symbol"]: (s["status"], s["message"]) for s in symbols} == {
        NAMES[0]: (200, None),
        halted: (403, "asset is halted"),
        NAMES[2]: (200, None),
    }
    lines = detail.splitlines()
    assert lines[1] == "2 of 3 positions closing, 1 FAILED and still held"
    assert f"{halted}: FAILED, HTTP 403: asset is halted" in lines
    assert detail in operator.cycle()
    assert open_causes(db) == [("liquidation", ACTOR)]


def test_a_broker_that_does_not_answer_is_alerted_once_and_retried(db, operator):
    fake = holding(close_all_fails=AlpacaError("DELETE /v2/positions: timed out"))
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    cycle(db, fake)
    cycle(db, fake, NOW + timedelta(minutes=1))

    assert fake.calls.count(("close_all",)) == 2
    assert kinds(db) == ["liquidation_failed"]
    body = alert(db, "liquidation_failed")
    assert "could not reach the broker: DELETE /v2/positions: timed out" in body
    assert rows(db, "SELECT 1 FROM agb.liquidation WHERE kind = 'result'") == []
    assert len(execution.pending_liquidation(db.connect("execution"))) == 1

    fake.close_all_fails = None
    cycle(db, fake, NOW + timedelta(minutes=2))

    assert fake.calls.count(("close_all",)) == 3
    assert kinds(db) == ["liquidation_failed", "liquidation"]
    assert result(db)[1].splitlines()[1] == "all 3 positions closing"


def test_a_confirmation_resumed_before_execution_reaches_it_is_not_carried_out(db, operator):
    fake = holding()
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")
    operator.says("/resume")

    cycle(db, fake)

    assert ("close_all",) not in fake.calls
    symbols, detail = result(db)
    assert symbols == []
    assert "not carried out, the liquidation cause was cleared" in detail
    assert alert(db, "liquidation") == detail


def test_a_submission_in_flight_is_cancelled_and_its_basket_halted(db, operator, clock):
    conn = db.connect("execution")
    fake = broker(("H01", *NAMES))
    target = submitted(db, conn, fake)
    clock.now = BEFORE_DEADLINE
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    cycle(db, fake, BEFORE_DEADLINE)

    assert state(db, target.id)[0] == "halted"
    assert all(o.terminal for o in fake.orders.values() if o.client_order_id.startswith("agb"))
    detail = result(db)[1]
    assert f"basket {target.reference} halted at " in detail
    assert fake.calls.index(("close_all",)) > max(
        i for i, c in enumerate(fake.calls) if c[0] == "cancel"
    )


def test_an_order_still_being_cancelled_is_named_when_a_liquidation_halts_its_basket(
    db, operator, clock
):
    """Issue #149: the broker took the cancel of one order but had not carried it out when the
    liquidation went on to close the account. The order may have filled, so the halted basket
    names it with its status instead of reporting every order cancelled."""
    conn = db.connect("execution")
    fake = broker(("H01", *NAMES))
    target = submitted(db, conn, fake)
    sent = len(fake.orders)
    fake.cancel_pending.add("ord-0001")
    pending = fake.orders["ord-0001"]
    named = f"{pending.side} {pending.qty} {pending.symbol} (pending_cancel, 0 filled)"
    clock.now = BEFORE_DEADLINE
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    cycle(db, fake, BEFORE_DEADLINE)

    assert state(db, target.id)[0] == "halted"
    reason = (
        f"paused mid-submission: {sent - 1} of {sent} orders cancelled; "
        f"still live or filled at the broker: {named}"
    )
    [halted] = [line for line in result(db)[1].splitlines() if line.startswith("basket ")]
    assert halted.startswith(f"basket {target.reference} halted at ")
    assert halted.endswith(f": {reason}")
    assert alert(db, "partial_rebalance_paused") == (
        f"basket {target.reference} halted by a pause mid-submission; still live or filled at "
        f"the broker: {named}"
    )
    assert rows(
        db,
        "SELECT s.kind, s.status FROM agb.order_state s"
        " JOIN agb.broker_order o ON o.id = s.order_id WHERE o.client_order_id = %s",
        pending.client_order_id,
    ) == [("status", "pending_cancel")]


# -- the record of the answer --------------------------------------------------------------


def answered(db):
    return rows(
        db,
        "SELECT o.symbol, o.status, o.broker_order_id IS NOT NULL, o.message, o.actor"
        " FROM agb.liquidation_order o ORDER BY o.id",
    )


def test_the_closing_fills_are_recorded_so_the_next_snapshot_matches(db, operator):
    """Issue #150: the closing orders' fills are recorded against the liquidation, so the
    snapshot after it finds the book it expects, not a mismatch on every closed name."""
    conn = db.connect("execution")
    fake = holding()
    assert execution.snapshot(conn, fake, TRADE, CALENDAR) is True
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    assert execution.apply_liquidation(conn, fake, CALENDAR, NOW) == 1
    assert answered(db) == [(name, 200, True, None, "execution") for name in HELD]
    fake.close(datetime.now(UTC))
    assert fake.positions() == []
    fake.now = CALENDAR.close_at(NEXT)
    assert execution.snapshot(conn, fake, NEXT, CALENDAR) is True

    assert rows(db, "SELECT count(*) FROM agb.position_mismatch") == [(0,)]
    assert rows(
        db,
        "SELECT o.symbol, f.side, f.qty, f.price, f.actor FROM agb.liquidation_fill f"
        " JOIN agb.liquidation_order o ON o.id = f.order_id ORDER BY f.id",
    ) == [(name, "sell", qty, MID, "execution") for name, qty in HELD.items()]
    assert rows(db, "SELECT count(*) FROM agb.fill") == [(0,)]
    # Recording the fills again adds nothing.
    assert execution.ingest_fills(conn, fake, NOW - timedelta(days=1)) == 0


def test_a_failure_after_close_all_keeps_the_answer_and_is_retried_from_it(
    db, operator, monkeypatch
):
    """Issue #150: the broker's answer is in the database before anything that could fail, so
    a failure after close-all neither loses it nor sends a second close-all."""
    conn = db.connect("execution")
    fake = holding(close_fails={NAMES[1]: "asset is halted"})
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    def broken(*_):
        raise RuntimeError("the database went away")

    with monkeypatch.context() as m:
        m.setattr(execution, "_liquidation_text", broken)
        with pytest.raises(RuntimeError, match="the database went away"):
            execution.apply_liquidation(conn, fake, CALENDAR, NOW)

    assert answered(db) == [
        (NAMES[0], 200, True, None, "execution"),
        (NAMES[1], 403, False, "asset is halted", "execution"),
        (NAMES[2], 200, True, None, "execution"),
    ]
    assert rows(db, "SELECT 1 FROM agb.liquidation WHERE kind = 'result'") == []
    assert len(execution.pending_liquidation(conn)) == 1

    cycle(db, fake, NOW + timedelta(minutes=1))

    assert fake.calls.count(("close_all",)) == 1
    symbols, detail = result(db)
    assert [(s["symbol"], s["status"], s["message"]) for s in symbols] == [
        (NAMES[0], 200, None),
        (NAMES[1], 403, "asset is halted"),
        (NAMES[2], 200, None),
    ]
    assert detail.splitlines()[1] == "2 of 3 positions closing, 1 FAILED and still held"
    assert execution.pending_liquidation(conn) == []


def test_a_basket_that_moves_under_the_liquidation_cancel_is_alerted_and_retried(
    db, operator, monkeypatch
):
    """Issue #150: `cancel_in_flight` raises `ExecutionError` when a basket moves under it;
    the liquidation alerts and waits for the next cycle instead of failing the cycle."""
    conn = db.connect("execution")
    fake = holding()
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")

    def moved(*_):
        raise ExecutionError("basket 1 is no longer submitted")

    with monkeypatch.context() as m:
        m.setattr(execution, "cancel_in_flight", moved)
        assert execution.apply_liquidation(conn, fake, CALENDAR, NOW) == 0

    assert ("close_all",) not in fake.calls
    assert kinds(db) == ["liquidation_failed"]
    assert "could not reach the broker: basket 1 is no longer submitted" in alert(
        db, "liquidation_failed"
    )
    assert answered(db) == []
    assert len(execution.pending_liquidation(conn)) == 1

    cycle(db, fake, NOW + timedelta(minutes=1))

    assert fake.calls.count(("close_all",)) == 1
    assert result(db)[1].splitlines()[1] == "all 3 positions closing"


def test_a_basket_that_moves_under_the_pause_cancel_is_alerted_and_retried(
    db, operator, monkeypatch
):
    """Issue #150: the same for a pause, whose cancel is the same call."""
    conn = db.connect("execution")
    fake = holding()
    operator.says("/pause")

    def moved(*_):
        raise ExecutionError("basket 1 is no longer submitted")

    with monkeypatch.context() as m:
        m.setattr(execution, "cancel_in_flight", moved)
        assert execution.apply_latch(conn, fake, CALENDAR, NOW) == 0

    assert ("suspend", True) not in fake.calls
    assert kinds(db) == ["latch_failed"]
    assert "basket 1 is no longer submitted" in alert(db, "latch_failed")
    assert len(execution.pending_latch(conn)) == 1

    assert execution.apply_latch(conn, fake, CALENDAR, NOW + timedelta(minutes=1)) == 1
    assert ("suspend", True) in fake.calls
    assert execution.pending_latch(conn) == []


# -- the schema --------------------------------------------------------------------------------


def test_who_writes_which_liquidation_row(db, operator):
    """The bot issues, confirms and refuses; only execution records a result; a token is
    followed once; and nothing is changed afterwards."""
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")
    [(token_id,), (confirm_id,)] = rows(db, "SELECT id FROM agb.liquidation ORDER BY id")
    insert = (
        "INSERT INTO agb.liquidation (kind, follows, token, expires_at, symbols, detail, actor)"
        " VALUES (%s, %s, %s, %s, %s, 'x', %s)"
    )

    bot = db.connect("bot")
    with pytest.raises(errors.InsufficientPrivilege):
        bot.execute(insert, ("result", confirm_id, None, None, Jsonb([]), "bot"))
    bot.rollback()
    operator.says("/help")
    [(help_id,)] = rows(db, "SELECT max(id) FROM agb.command")
    with pytest.raises(errors.UniqueViolation):
        bot.execute(
            "INSERT INTO agb.liquidation (kind, follows, command_id, detail, actor)"
            " VALUES ('refused', %s, %s, 'again', 'bot')",
            (token_id, help_id),
        )
    bot.rollback()

    ex = db.connect("execution")
    with pytest.raises(errors.InsufficientPrivilege):
        ex.execute(insert, ("token", None, "abcdef", NOW, None, "execution"))
    ex.rollback()
    with pytest.raises(errors.CheckViolation):
        ex.execute(insert, ("result", token_id, None, None, Jsonb([]), "execution"))
    ex.rollback()

    for role in ("bot", "execution"):
        conn = db.connect(role)
        with pytest.raises(errors.InsufficientPrivilege):
            conn.execute("UPDATE agb.liquidation SET detail = 'changed'")
        conn.rollback()


def test_who_writes_the_record_of_a_liquidation(db, operator):
    """Only execution records a closing order or its fill; a closing order answers a
    confirmation and nothing else; and neither is changed afterwards."""
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")
    [(token_id,), (confirm_id,)] = rows(db, "SELECT id FROM agb.liquidation ORDER BY id")
    insert = (
        "INSERT INTO agb.liquidation_order (confirm_id, symbol, status, broker_order_id, actor)"
        " VALUES (%s, 'N01', 200, %s, %s)"
    )

    bot = db.connect("bot")
    with pytest.raises(errors.InsufficientPrivilege):
        bot.execute(insert, (confirm_id, "ord-x", "bot"))
    bot.rollback()

    ex = db.connect("execution")
    with pytest.raises(errors.CheckViolation):
        ex.execute(insert, (token_id, "ord-x", "execution"))
    ex.rollback()
    ex.execute(insert, (confirm_id, "ord-x", "execution"))
    with pytest.raises(errors.UniqueViolation):
        ex.execute(insert, (confirm_id, "ord-x", "execution"))
    ex.rollback()
    with pytest.raises(errors.CheckViolation):
        ex.execute(
            "INSERT INTO agb.liquidation_fill (order_id, execution_id, side, qty, price,"
            " filled_at, fee_model_version, actor)"
            " SELECT id, 'act-x', 'sell_short', 1, 1, now(), 'v', 'execution'"
            " FROM agb.liquidation_order"
        )
    ex.rollback()

    for table in ("liquidation_order", "liquidation_fill"):
        with pytest.raises(errors.InsufficientPrivilege):
            ex.execute(f"UPDATE agb.{table} SET actor = 'execution'")
        ex.rollback()
        with pytest.raises(errors.InsufficientPrivilege):
            ex.execute(f"DELETE FROM agb.{table}")
        ex.rollback()


def test_the_audit_log_shows_each_step_without_the_token(db, operator):
    fake = holding()
    token = issued(operator.says("/liquidate"))
    operator.says(f"/liquidate {token}")
    cycle(db, fake)

    events = rows(
        db,
        "SELECT event, subject, detail FROM agb.audit_log WHERE source = 'liquidation'"
        " ORDER BY row_id",
    )
    assert [event for event, _, _ in events] == [
        "liquidation token",
        "liquidation confirm",
        "liquidation result",
    ]
    assert events[0][1] is None
    assert all(token not in (detail or "") for _, _, detail in events)
    assert ("halt set liquidation",) in rows(
        db, "SELECT event FROM agb.audit_log WHERE source = 'halt_event'"
    )


# -- the Alpaca client ---------------------------------------------------------------------


class Answering:
    """An opener that answers every request with `status` and `body`, and records each."""

    def __init__(self, status: int, body) -> None:
        self.status = status
        self.body = json.dumps(body).encode()
        self.requests = []

    def open(self, request, timeout):
        self.requests.append(request)
        if self.status >= 400:
            raise urllib.error.HTTPError(
                request.full_url, self.status, "error", {}, io.BytesIO(self.body)
            )
        return io.BytesIO(self.body)


MULTI_STATUS = [
    {
        "symbol": "AAPL",
        "status": 200,
        "body": {"id": "ord-1", "symbol": "AAPL", "side": "sell", "qty": "10"},
    },
    {
        "symbol": "HALT",
        "status": 403,
        "body": {"code": 40310000, "message": "asset HALT is not tradable"},
    },
    {"symbol": "ODD", "status": 500, "body": None},
]


def test_the_client_reads_a_207_symbol_by_symbol():
    opener = Answering(207, MULTI_STATUS)
    client = Alpaca("key", "secret", trading="https://paper-api.example", opener=opener)

    closed = client.close_all()

    [request] = opener.requests
    assert request.get_method() == "DELETE"
    assert request.full_url == "https://paper-api.example/v2/positions?cancel_orders=true"
    assert closed == [
        Closed("AAPL", 200, "ord-1", None),
        Closed("HALT", 403, None, "asset HALT is not tradable"),
        Closed("ODD", 500, None, "no message"),
    ]
    assert [c.ok for c in closed] == [True, False, False]


def test_the_client_reads_an_empty_book_as_nothing_closed():
    assert Alpaca("key", "secret", opener=Answering(207, [])).close_all() == []


def test_a_failed_close_all_call_is_an_error_without_the_keys():
    opener = Answering(403, {"code": 40310000, "message": "forbidden for PKTESTKEY"})

    with pytest.raises(AlpacaError) as raised:
        Alpaca("PKTESTKEY", "SKTESTSECRET", opener=opener).close_all()

    assert raised.value.status == 403
    assert str(raised.value) == "DELETE /v2/positions: HTTP 403: forbidden for <secret>"


def test_the_fake_answers_one_entry_per_position():
    fake = holding(close_fails={NAMES[0]: "halted"})

    closed = fake.close_all()

    assert [(c.symbol, c.ok) for c in closed] == [(n, n != NAMES[0]) for n in HELD]
    assert [o.client_order_id for o in fake.orders.values()] == [
        f"close-{n}" for n in list(HELD)[1:]
    ]
