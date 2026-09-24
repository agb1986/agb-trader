"""`/pause` and `/resume` end to end: the bot writes the halt latch and the command, execution
carries the command to the broker (`suspend_trade`, and a pause's cancel of what is in
flight), and the confirmation comes back to the chat as an alert. The broker is
`alpaca_fake.FakeBroker`, Telegram is `test_bot.FakeTelegram`, and the baskets come from the
execution tests.
"""

from datetime import UTC, datetime, timedelta

import pytest
from alpaca_fake import FakeBroker
from market_fake import PERIOD
from psycopg import errors
from test_bot import FakeTelegram
from test_execution import (
    AFTER,
    CLOSE_AT,
    NAMES,
    NOW,
    Clock,
    approved,
    broker,
    rows,
    state,
    submitted,
)

from agb import execution, health
from agb.alpaca import AlpacaError
from agb.roles import OWNER
from agb.services.bot import NO_COMMANDS, NOT_YET, Bot
from agb.services.execution import Execution

CHAT = "4242"
OPERATOR = 77
ACTOR = f"telegram:{OPERATOR}"
# 15:40 ET, before the 15:50 ET cancel deadline; 15:52 ET, after it.
BEFORE_DEADLINE = datetime(2026, 10, 1, 19, 40, tzinfo=UTC)
PAST_DEADLINE = datetime(2026, 10, 1, 19, 52, tzinfo=UTC)


class Operator:
    """The configured chat, with a bot polling it."""

    def __init__(self, db, clock=lambda: NOW) -> None:
        self.db = db
        self.telegram = FakeTelegram()
        self.bot = Bot(self.telegram, CHAT, clock)
        self._update = 0

    def says(self, text: str, user: int = OPERATOR) -> str:
        """Send `text` and run one bot cycle. Returns the bot's reply."""
        self._update += 1
        self.telegram.updates.append(
            {
                "update_id": self._update,
                "message": {"chat": {"id": int(CHAT)}, "from": {"id": user}, "text": text},
            }
        )
        before = len(self.telegram.sent)
        self.bot.poll(self.db.connect("bot"))
        [(_, reply)] = self.telegram.sent[before:]
        return reply

    def cycle(self) -> list[str]:
        """One full bot cycle. Returns what it sent."""
        before = len(self.telegram.sent)
        self.bot(self.db.connect("bot"))
        return [text for _, text in self.telegram.sent[before:]]


@pytest.fixture
def operator(db):
    return Operator(db)


def commands(db):
    return rows(
        db,
        "SELECT actor, surface, raw_text, verb, args, outcome, reason FROM agb.command ORDER BY id",
    )


def open_causes(db):
    return rows(db, "SELECT cause, actor FROM agb.halt_open ORDER BY at, cause")


def latches(db):
    return rows(
        db,
        "SELECT c.verb, l.suspend_trade, l.observed_suspend, l.actor"
        " FROM agb.broker_latch l JOIN agb.command c ON c.id = l.command_id ORDER BY l.id",
    )


def confirmation(db, verb):
    [(body,)] = rows(db, "SELECT body FROM agb.alert WHERE kind = %s", verb)
    return body


def kinds(db):
    return [kind for (kind,) in rows(db, "SELECT kind FROM agb.alert ORDER BY id")]


# -- /pause --------------------------------------------------------------------------------


def test_pause_opens_the_cause_sets_suspend_trade_and_confirms_both(db, operator):
    fake = broker()

    reply = operator.says("/pause")

    assert reply == (
        "PAUSED\nhalt cause pause: open. No further order goes out.\n"
        "broker suspend_trade: asked on. Execution confirms it here, with anything it cancelled."
    )
    assert open_causes(db) == [("pause", ACTOR)]
    assert commands(db) == [
        (ACTOR, "telegram", "/pause", "pause", {"already_open": False}, "accepted", None)
    ]
    assert rows(db, "SELECT reason FROM agb.halt_event") == [("/pause",)]
    assert [c.verb for c in execution.pending_latch(db.connect("execution"))] == ["pause"]

    Execution(fake, clock=Clock(NOW))(db.connect("execution"))

    assert fake.suspend_trade is True
    assert ("suspend", True) in fake.calls
    assert latches(db) == [("pause", True, True, "execution")]
    assert execution.pending_latch(db.connect("execution")) == []
    body = confirmation(db, "pause")
    lines = body.splitlines()
    assert lines[0].startswith(f"PAUSE by {ACTOR} at 20")
    assert lines[1].startswith("halt cause pause: open since ")
    assert lines[1].endswith(f", by {ACTOR}")
    assert lines[2:] == ["broker suspend_trade: on", "no submission in flight"]

    # The bot delivers the confirmation, and the broker agrees with the latch.
    assert body in operator.cycle()
    assert health.broker_mismatch(db.connect("bot")) == []


def test_a_second_pause_reasserts_the_broker_without_a_second_set(db, operator):
    fake = broker()
    operator.says("/pause")
    Execution(fake, clock=Clock(NOW))(db.connect("execution"))
    fake.suspend_trade = False

    reply = operator.says("/pause", user=78)

    assert "halt cause pause: already open since " in reply
    assert f"by {ACTOR}." in reply
    assert rows(db, "SELECT count(*) FROM agb.halt_event") == [(1,)]
    assert [(c[0], c[4]) for c in commands(db)] == [
        (ACTOR, {"already_open": False}),
        ("telegram:78", {"already_open": True}),
    ]
    Execution(fake, clock=Clock(NOW + timedelta(minutes=1)))(db.connect("execution"))
    assert fake.suspend_trade is True
    assert [latch[0] for latch in latches(db)] == ["pause", "pause"]


def test_pause_cancels_a_submitted_basket_before_the_deadline_and_halts_it(db, operator):
    conn = db.connect("execution")
    fake = broker(("H01", *NAMES))
    target = submitted(db, conn, fake)
    sent = len(fake.orders)

    operator.says("/pause")
    Execution(fake, clock=Clock(BEFORE_DEADLINE))(conn)

    assert {o.status for o in fake.orders.values()} == {"canceled"}
    reason = f"paused mid-submission: {sent} of {sent} orders cancelled"
    assert state(db, target.id) == ("halted", 4, "execution", "execution")
    assert rows(
        db, "SELECT reason FROM agb.basket_event WHERE basket_id = %s AND seq = 4", target.id
    ) == [(reason,)]
    assert fake.suspend_trade is True
    lines = confirmation(db, "pause").splitlines()
    assert lines[2] == "broker suspend_trade: on"
    [halted] = lines[3:]
    assert halted.startswith(f"basket {PERIOD} halted at 2026-")
    assert halted.endswith(f": {reason}")
    assert "partial_rebalance" not in kinds(db)
    # Cancelled before the rest of the account moves.
    assert fake.calls.index(("suspend", True)) > max(
        i for i, call in enumerate(fake.calls) if call[0] == "cancel"
    )


def test_a_cancel_that_fails_fires_the_partial_rebalance_alert_naming_what_is_live(db, operator):
    conn = db.connect("execution")
    fake = broker(("H01", *NAMES))
    target = submitted(db, conn, fake)
    fake.cancel_fails.add("ord-0001")
    stuck = fake.orders["ord-0001"]
    live = f"{stuck.side} {stuck.qty} {stuck.symbol}"

    operator.says("/pause")
    Execution(fake, clock=Clock(BEFORE_DEADLINE))(conn)

    assert fake.orders["ord-0001"].status == "accepted"
    assert state(db, target.id)[0] == "halted"
    [(subject, body)] = rows(
        db, "SELECT subject, body FROM agb.alert WHERE kind = 'partial_rebalance'"
    )
    assert subject == PERIOD
    assert body == (
        f"basket {PERIOD} halted by a pause mid-submission; still live at the broker: {live}"
    )
    assert confirmation(db, "pause").endswith(f"; still live at the broker: {live}")
    assert f"still live at the broker: {live}" in "\n".join(operator.cycle())


def test_after_the_cancel_deadline_a_pause_leaves_the_orders_to_the_close(db, operator):
    conn = db.connect("execution")
    fake = broker(("H01", *NAMES))
    target = submitted(db, conn, fake)
    sent = len(fake.orders)

    operator.says("/pause")
    service = Execution(fake, clock=Clock(PAST_DEADLINE))
    service(conn)

    assert not any(call[0] == "cancel" for call in fake.calls)
    assert state(db, target.id)[0] == "submitted"
    assert fake.suspend_trade is True
    assert confirmation(db, "pause").splitlines()[3:] == [
        f"basket {PERIOD}: past the 15:50 ET cancel deadline, {sent} orders left to the close"
    ]

    # The orders go through the close and are read back as usual.
    fake.close(CLOSE_AT)
    service.clock.now = AFTER
    service(conn)
    assert state(db, target.id)[0] == "filled"


def test_a_pause_during_the_submission_is_reported_with_what_it_cancelled(db, operator):
    """The pause lands between two orders: the submission stops itself, cancels what went
    out and halts the basket; the next cycle confirms, naming the halted basket."""
    conn = db.connect("execution")
    target = approved(db)

    def pause_after_five(fake, client_order_id):
        if len(fake.submits()) == 5:
            operator.says("/pause")

    fake = broker(on_submit=pause_after_five)
    service = Execution(fake, clock=Clock(NOW))
    service(conn)
    assert state(db, target.id)[0] == "halted"
    assert fake.suspend_trade is False
    assert "pause" not in kinds(db)

    service(conn)

    assert fake.suspend_trade is True
    [halted] = confirmation(db, "pause").splitlines()[3:]
    assert halted.endswith(
        ": halt open mid-submission (pause): 5 of 20 orders sent, 0 not cancelled"
    )
    assert {o.status for o in fake.orders.values()} == {"canceled"}


def test_a_broker_that_refuses_the_suspend_leaves_the_command_for_the_next_cycle(db, operator):
    fake = broker(suspend_fails=AlpacaError("PATCH /v2/account/configurations: HTTP 500"))
    conn = db.connect("execution")
    service = Execution(fake, clock=Clock(NOW))
    operator.says("/pause")

    service(conn)
    service(conn)

    assert fake.calls.count(("suspend", True)) == 2
    assert latches(db) == []
    [(subject, body)] = rows(db, "SELECT subject, body FROM agb.alert WHERE kind = 'latch_failed'")
    assert subject.startswith("command ")
    assert "could not set the broker's suspend_trade on: PATCH /v2/account/configurations" in body
    # Still waiting, so the disagreement is the command's, not a mismatch.
    assert health.broker_mismatch(db.connect("bot")) == []

    fake.suspend_fails = None
    service(conn)
    assert fake.suspend_trade is True
    assert latches(db) == [("pause", True, True, "execution")]


# -- /resume -------------------------------------------------------------------------------


def test_resume_clears_every_open_cause_once_unsets_suspend_trade_and_lists_them(db, operator):
    fake = broker()
    conn = db.connect("execution")
    service = Execution(fake, clock=Clock(NOW))
    operator.says("/pause")
    service(conn)
    db.connect(OWNER).execute(
        "INSERT INTO agb.halt_event (cause, action, actor, reason)"
        " VALUES ('gate_breach', 'set', 'pipeline', 'a check failed')"
    )

    reply = operator.says("/resume")

    assert reply == (
        "RESUMED\ncleared: pause, gate_breach\nbroker suspend_trade: asked off. Execution "
        "confirms it here.\nA halted rebalance is not resubmitted."
    )
    assert open_causes(db) == []
    assert rows(
        db, "SELECT cause, actor, reason FROM agb.halt_event WHERE action = 'clear' ORDER BY id"
    ) == [("pause", ACTOR, "/resume"), ("gate_breach", ACTOR, "/resume")]
    assert commands(db)[-1] == (
        ACTOR,
        "telegram",
        "/resume",
        "resume",
        {"cleared": ["pause", "gate_breach"]},
        "accepted",
        None,
    )

    service.clock.now += timedelta(minutes=1)
    service(conn)

    assert fake.suspend_trade is False
    assert latches(db)[-1] == ("resume", False, False, "execution")
    lines = confirmation(db, "resume").splitlines()
    assert lines[0].startswith(f"RESUME by {ACTOR} at ")
    assert lines[1:] == [
        "cleared: pause, gate_breach",
        "broker suspend_trade: off",
        "a halted rebalance is not resubmitted",
    ]
    assert confirmation(db, "resume") in operator.cycle()


def test_resume_with_nothing_open_says_so(db, operator):
    fake = broker()
    assert "cleared: nothing was open" in operator.says("/resume")
    Execution(fake, clock=Clock(NOW))(db.connect("execution"))
    assert ("suspend", False) in fake.calls
    assert confirmation(db, "resume").splitlines()[1] == "cleared: nothing was open"


def test_a_rebalance_halted_before_resume_is_not_resubmitted_after_it(db, operator):
    conn = db.connect("execution")
    target = approved(db)

    def pause_after_five(fake, client_order_id):
        if len(fake.submits()) == 5:
            operator.says("/pause")

    fake = broker(on_submit=pause_after_five)
    service = Execution(fake, clock=Clock(NOW))
    service(conn)
    service(conn)
    assert state(db, target.id)[0] == "halted"
    fake.on_submit = None

    operator.says("/resume")
    for minute in range(0, 10, 2):
        service.clock.now = NOW + timedelta(minutes=minute)
        service(conn)

    assert fake.submits() == [f"{PERIOD}-{s}" for s in NAMES[:5]]
    assert state(db, target.id)[0] == "halted"
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(5,)]
    assert fake.suspend_trade is False


def test_commands_reach_the_broker_in_the_order_they_were_given(db, operator):
    fake = broker()
    operator.says("/pause")
    operator.says("/resume")
    operator.says("/pause")

    Execution(fake, clock=Clock(NOW))(db.connect("execution"))

    assert [c for c in fake.calls if c[0] == "suspend"] == [
        ("suspend", True),
        ("suspend", False),
        ("suspend", True),
    ]
    assert [(verb, wanted) for verb, wanted, *_ in latches(db)] == [
        ("pause", True),
        ("resume", False),
        ("pause", True),
    ]
    assert fake.suspend_trade is True


# -- the mismatch --------------------------------------------------------------------------


def test_a_suspend_trade_mismatch_alerts_once_per_episode_in_both_directions(db, operator):
    fake = broker()
    conn = db.connect("execution")
    service = Execution(fake, clock=Clock(NOW))
    operator.says("/pause")
    # Waiting for execution: the broker is still off, and that is not a mismatch yet.
    execution.record_state(conn, fake)
    assert health.broker_mismatch(db.connect("bot")) == []
    service(conn)

    # Someone turns suspend_trade off at the broker while the pause is open.
    fake.suspend_trade = False
    execution.record_state(conn, fake)
    sent = operator.cycle()
    execution.record_state(conn, fake)
    sent += operator.cycle()
    [off] = [text for text in sent if "broker suspend_trade: off" in text]
    assert "pause open; observed " in off
    assert kinds(db).count("broker") == 1

    # The resume puts the two back in step.
    operator.says("/resume")
    assert health.broker_mismatch(db.connect("bot")) == []
    service.clock.now += timedelta(minutes=1)
    service(conn)
    assert health.broker_mismatch(db.connect("bot")) == []

    # And, a minute later, someone turns it on at the broker with no pause open. (An
    # episode is named by the second it was first seen, so this one is observed later.)
    db.connect(OWNER).execute(
        "INSERT INTO agb.broker_state (account_status, suspend_trade, no_shorting,"
        " trading_blocked, account_blocked, equity_usd, cash_usd, buying_power_usd, actor,"
        " observed_at) SELECT account_status, true, no_shorting, trading_blocked,"
        " account_blocked, equity_usd, cash_usd, buying_power_usd, actor,"
        " now() + interval '1 minute' FROM agb.broker_state ORDER BY id DESC LIMIT 1"
    )
    [on] = [text for text in operator.cycle() if "broker suspend_trade: on" in text]
    assert "no pause open; observed " in on
    assert kinds(db).count("broker") == 2


# -- the command record ----------------------------------------------------------------------


def test_every_message_is_a_command_and_the_rest_are_refused(db, operator):
    assert operator.says("/status") == NO_COMMANDS
    assert operator.says("hello") == NO_COMMANDS
    assert operator.says("/approve@agb_bot 2026-10 yes") == NO_COMMANDS

    assert commands(db) == [
        (ACTOR, "telegram", "/status", "status", {"args": []}, "refused", NOT_YET),
        (ACTOR, "telegram", "hello", None, {"args": ["hello"]}, "refused", NOT_YET),
        (
            ACTOR,
            "telegram",
            "/approve@agb_bot 2026-10 yes",
            "approve",
            {"args": ["2026-10", "yes"]},
            "refused",
            NOT_YET,
        ),
    ]
    assert execution.pending_latch(db.connect("execution")) == []
    assert open_causes(db) == []


def test_commands_are_append_only_and_only_the_surfaces_write_them(db, operator):
    operator.says("/pause")
    for statement in ("UPDATE agb.command SET actor = 'x'", "DELETE FROM agb.command"):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect("bot").execute(statement)
    for role in ("execution", "pipeline", "scheduler"):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect(role).execute(
                "INSERT INTO agb.command (actor, surface, raw_text, verb, outcome)"
                " VALUES ('x', 'telegram', '/pause', 'pause', 'accepted')"
            )
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("bot").execute(
            "INSERT INTO agb.broker_latch (command_id, suspend_trade, observed_suspend, detail,"
            " actor) SELECT id, true, true, 'x', 'bot' FROM agb.command"
        )
    with pytest.raises(errors.CheckViolation):
        db.connect("bot").execute(
            "INSERT INTO agb.command (actor, surface, raw_text, outcome)"
            " VALUES ('x', 'telegram', 'hi', 'refused')"
        )


def test_the_fake_broker_suspends_like_the_account_configuration():
    fake = FakeBroker(("AAA",))
    assert fake.suspend(True).suspend_trade is True
    assert fake.configuration().suspend_trade is True
    assert fake.suspend(False).suspend_trade is False
