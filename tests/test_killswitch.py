"""The kill switch called directly, without a bot or a broker: the token, the latch it opens
and clears, and every way the second step of a liquidation can be refused. The surfaces that
throw the switch (Telegram, the local UI) and execution's close-all are covered by
test_pause_resume and test_liquidate."""

import re
from datetime import UTC, datetime, timedelta

import pytest

from agb import killswitch
from agb.approval import Source

NOW = datetime(2026, 10, 1, 19, 40, tzinfo=UTC)
WHO = Source("telegram:77", "telegram", "/liquidate", chat_id="4242", message_id=1)


@pytest.fixture
def conn(db):
    return db.connect("bot")


def rows(conn, sql):
    return conn.execute(sql).fetchall()


def issued_token(reply):
    [found] = re.findall(r"^/liquidate ([a-z0-9]{6})$", reply, re.MULTILINE)
    return found


def spoken(token_text):
    return Source("telegram:77", "telegram", f"/liquidate {token_text}")


def test_a_token_is_six_lowercase_letters_and_digits():
    for _ in range(50):
        assert re.fullmatch(r"[a-z0-9]{6}", killswitch.token())


def test_tokens_are_not_repeated():
    assert len({killswitch.token() for _ in range(50)}) > 40


def test_pause_opens_the_cause_once_and_says_so_the_second_time(conn):
    first = killswitch.pause(conn, WHO)
    assert "halt cause pause: open." in first
    assert [cause for cause, *_ in killswitch.causes(conn)] == ["pause"]

    second = killswitch.pause(conn, WHO)
    assert "already open since" in second and "telegram:77" in second
    assert rows(conn, "SELECT cause, action FROM agb.halt_event ORDER BY id") == [("pause", "set")]
    assert rows(conn, "SELECT verb FROM agb.command ORDER BY id") == [("pause",), ("pause",)]


def test_resume_clears_every_open_cause_and_names_them(conn):
    killswitch.pause(conn, WHO)
    killswitch.liquidate(conn, WHO, killswitch.token(), NOW)  # refused: no token outstanding
    typed = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    killswitch.liquidate(conn, WHO, typed, NOW)
    assert [cause for cause, *_ in killswitch.causes(conn)] == ["pause", "liquidation"]

    reply = killswitch.resume(conn, WHO)
    assert "cleared: pause, liquidation" in reply
    assert killswitch.causes(conn) == []


def test_resume_with_nothing_open_says_nothing_was_open(conn):
    assert "cleared: nothing was open" in killswitch.resume(conn, WHO)
    assert rows(conn, "SELECT count(*) FROM agb.halt_event") == [(0,)]


def two_open_causes(conn):
    killswitch.pause(conn, WHO)
    typed = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    killswitch.liquidate(conn, WHO, typed, NOW)
    assert [cause for cause, *_ in killswitch.causes(conn)] == ["pause", "liquidation"]


def test_resume_can_clear_only_the_causes_it_is_told_to(conn):
    two_open_causes(conn)

    reply = killswitch.resume(conn, WHO, ["liquidation"])

    assert "cleared: liquidation" in reply
    assert "still open: pause" in reply
    assert "broker suspend_trade: stays on while pause is open." in reply
    assert [cause for cause, *_ in killswitch.causes(conn)] == ["pause"]
    assert rows(conn, "SELECT cause, action FROM agb.halt_event ORDER BY id")[-1] == (
        "liquidation",
        "clear",
    )
    [(args,)] = rows(conn, "SELECT args FROM agb.command WHERE verb = 'resume'")
    assert args == {"cleared": ["liquidation"], "left_open": ["pause"]}


def test_resume_of_the_pause_alone_asks_the_broker_off_and_leaves_the_rest(conn):
    two_open_causes(conn)

    reply = killswitch.resume(conn, WHO, ["pause"])

    assert "still open: liquidation" in reply
    assert "broker suspend_trade: asked off." in reply
    assert [cause for cause, *_ in killswitch.causes(conn)] == ["liquidation"]


def test_resume_choosing_no_cause_is_refused_and_recorded_and_clears_nothing(conn):
    two_open_causes(conn)
    events = rows(conn, "SELECT count(*) FROM agb.halt_event")

    reply = killswitch.resume(conn, WHO, [])

    assert reply == "RESUME refused\nno cause was chosen to clear\nNothing has been done."
    assert rows(conn, "SELECT count(*) FROM agb.halt_event") == events
    assert len(killswitch.causes(conn)) == 2
    assert rows(conn, "SELECT verb, outcome, reason FROM agb.command ORDER BY id")[-1] == (
        "resume",
        "refused",
        "no cause was chosen to clear",
    )


def test_resume_says_so_when_a_chosen_cause_is_not_open_any_more(conn):
    killswitch.pause(conn, WHO)

    reply = killswitch.resume(conn, WHO, ["pause", "liquidation"])

    assert "cleared: pause" in reply
    assert "not open any more, so not cleared: liquidation" in reply
    assert "still open" not in reply
    assert killswitch.causes(conn) == []


def test_resume_without_a_choice_replies_as_it_always_did(conn):
    killswitch.pause(conn, WHO)

    assert killswitch.resume(conn, WHO) == (
        "RESUMED\ncleared: pause\nbroker suspend_trade: asked off. Execution confirms it "
        "here.\nA halted rebalance is not resubmitted."
    )
    [(args,)] = rows(conn, "SELECT args FROM agb.command WHERE verb = 'resume'")
    assert args == {"cleared": ["pause"]}


def test_causes_list_the_oldest_first_with_who_and_why(conn):
    killswitch.pause(conn, WHO)
    [(cause, at, actor, reason)] = killswitch.causes(conn)
    assert (cause, actor, reason) == ("pause", "telegram:77", "/pause")
    assert at.tzinfo is not None


def test_a_token_request_says_what_is_held_and_how_long_the_token_lasts(conn):
    reply = killswitch.liquidate(conn, WHO, None, NOW)
    assert reply.startswith("LIQUIDATE — step 1 of 2")
    assert "positions: no snapshot yet, so the count is unknown" in reply
    assert "equity: not read from the broker yet" in reply
    assert "within 5 minutes" in reply
    assert "This cannot be undone." in reply
    [(kind, expires_at)] = rows(conn, "SELECT kind, expires_at FROM agb.liquidation")
    assert (kind, expires_at) == ("token", NOW + killswitch.TOKEN_VALID)
    assert killswitch.causes(conn) == []


def test_the_summary_counts_the_latest_snapshot_and_the_account_equity(db, conn):
    execution = db.connect("execution")
    execution.execute(
        "INSERT INTO agb.position_snapshot (snapshot_date, positions, actor)"
        " VALUES ('2026-09-30', 1, 'execution')"
    )
    execution.execute(
        "INSERT INTO agb.position_snapshot (snapshot_date, positions, actor)"
        " VALUES ('2026-10-01', 2, 'execution')"
    )
    snapshot = execution.execute("SELECT max(id) FROM agb.position_snapshot").fetchone()[0]
    for symbol, value in (("AAA", "1500.50"), ("BBB", "500.25")):
        execution.execute(
            "INSERT INTO agb.position (snapshot_id, symbol, qty, avg_entry_price, market_value_usd)"
            " VALUES (%s, %s, 10, 100, %s)",
            (snapshot, symbol, value),
        )
    execution.execute(
        "INSERT INTO agb.broker_state (observed_at, account_status, suspend_trade, no_shorting,"
        " trading_blocked, account_blocked, equity_usd, cash_usd, buying_power_usd, mode, actor)"
        " VALUES (%s, 'ACTIVE', false, true, false, false, 98000, 1000, 1000, 'paper',"
        " 'execution')",
        (NOW,),
    )

    reply = killswitch.issue(conn, WHO, NOW)
    assert "positions: 2 held, worth $2,000.75 at the 2026-10-01 snapshot" in reply
    assert "equity: about $98,000.00, read " in reply


def test_a_fresh_request_supersedes_the_earlier_token(conn):
    first = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    second = issued_token(killswitch.liquidate(conn, WHO, None, NOW + timedelta(minutes=1)))
    assert killswitch.outstanding(conn, NOW) == NOW + killswitch.TOKEN_VALID + timedelta(minutes=1)
    if first != second:
        reply = killswitch.liquidate(conn, WHO, first, NOW + timedelta(minutes=2))
        assert "wrong liquidation token" in reply


def test_the_outstanding_token_expires_but_is_never_shown(conn):
    assert killswitch.outstanding(conn, NOW) is None
    killswitch.liquidate(conn, WHO, None, NOW)
    expires = NOW + killswitch.TOKEN_VALID
    assert killswitch.outstanding(conn, NOW) == expires
    assert killswitch.outstanding(conn, expires - timedelta(seconds=1)) == expires
    assert killswitch.outstanding(conn, expires) is None


def test_the_token_typed_back_in_time_pauses_the_system(conn):
    typed = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    reply = killswitch.liquidate(conn, WHO, typed, NOW + timedelta(minutes=4))

    assert reply.startswith("LIQUIDATING — step 2 of 2")
    assert [cause for cause, *_ in killswitch.causes(conn)] == ["liquidation"]
    assert rows(conn, "SELECT kind FROM agb.liquidation ORDER BY id") == [
        ("token",),
        ("confirm",),
    ]
    assert killswitch.outstanding(conn, NOW) is None


def test_the_token_is_accepted_in_any_case(conn):
    typed = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    assert killswitch.confirm(conn, WHO, typed.upper(), NOW).startswith("LIQUIDATING")


def test_a_token_confirmed_while_the_cause_is_already_open_does_not_open_it_twice(conn):
    typed = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    killswitch.liquidate(conn, WHO, typed, NOW)
    again = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    killswitch.liquidate(conn, WHO, again, NOW)
    assert rows(conn, "SELECT cause, action FROM agb.halt_event ORDER BY id") == [
        ("liquidation", "set")
    ]


@pytest.mark.parametrize("typed", ["", None])
def test_nothing_typed_asks_for_a_token(conn, typed):
    assert killswitch.liquidate(conn, WHO, typed, NOW).startswith("LIQUIDATE — step 1")


def test_a_token_confirmed_with_none_outstanding_is_refused_and_recorded(conn):
    reply = killswitch.liquidate(conn, WHO, "abc123", NOW)
    assert reply == (
        "LIQUIDATE refused\nno liquidation token is outstanding: send /liquidate first\n"
        "Nothing has been done."
    )
    assert killswitch.causes(conn) == []
    assert rows(conn, "SELECT count(*) FROM agb.liquidation") == [(0,)]
    [(outcome, reason)] = rows(conn, "SELECT outcome, reason FROM agb.command")
    assert outcome == "refused" and "no liquidation token" in reason


def test_a_wrong_token_is_refused_and_spends_the_token(conn):
    right = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    wrong = "zzzzzz" if right != "zzzzzz" else "yyyyyy"

    reply = killswitch.liquidate(conn, WHO, wrong, NOW)
    assert "wrong liquidation token; it is spent" in reply
    assert killswitch.causes(conn) == []
    assert killswitch.outstanding(conn, NOW) is None
    # The right token no longer works: the refusal spent it.
    assert "no liquidation token is outstanding" in killswitch.liquidate(conn, WHO, right, NOW)
    assert rows(conn, "SELECT kind FROM agb.liquidation ORDER BY id") == [
        ("token",),
        ("refused",),
    ]


def test_an_expired_token_is_refused_and_spent(conn):
    typed = issued_token(killswitch.liquidate(conn, WHO, None, NOW))
    reply = killswitch.liquidate(conn, WHO, typed, NOW + killswitch.TOKEN_VALID)
    assert "the liquidation token expired at" in reply and "it is spent" in reply
    assert killswitch.causes(conn) == []
    assert killswitch.outstanding(conn, NOW) is None


def test_a_token_with_characters_outside_ascii_is_refused_not_a_crash(conn):
    killswitch.liquidate(conn, WHO, None, NOW)
    reply = killswitch.liquidate(conn, WHO, "ünïcödé", NOW)
    assert reply.startswith("LIQUIDATE refused")
    assert "wrong liquidation token" in reply
