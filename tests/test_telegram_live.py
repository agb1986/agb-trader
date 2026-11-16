"""Opt-in live check: a real alert delivered to the real chat through a real proxy, and its
`alert_event` carrying Telegram's message id; and a command answered in the real chat, in
parts where it runs past Telegram's limit, with its `command` row.

Skipped unless AGB_LIVE_TELEGRAM=1 and TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and HTTPS_PROXY
are set in the environment. It reads nothing else: no env file. It sends a few messages and
never reads the chat's messages, so it does not compete with a bot already polling the token.
The alert is recorded in a throwaway test database, like every other test's.
"""

import os
from datetime import UTC, datetime

import pytest

from agb import readout
from agb.services.bot import ALERT_HEADER, MESSAGE_MAX, Bot
from agb.telegram import Telegram

REQUIRED = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "HTTPS_PROXY")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_TELEGRAM") != "1" or not all(os.environ.get(k) for k in REQUIRED),
        reason="live Telegram check: set AGB_LIVE_TELEGRAM=1 and " + ", ".join(REQUIRED),
    ),
]


def test_a_real_alert_is_delivered_and_recorded(db):
    telegram = Telegram.from_env()
    assert telegram.get_me()["is_bot"]
    bot = Bot(telegram, os.environ["TELEGRAM_CHAT_ID"].strip())
    conn = db.connect("bot")
    at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    body = f"{ALERT_HEADER}\n\n  live check {at}\n    from tests/test_telegram_live.py"
    assert bot.raise_alert(conn, "live_check", "telegram", at, body)

    bot.deliver(conn)

    [(event, chat_id, message_id, detail)] = (
        db.connect("cli")
        .execute("SELECT event, chat_id, telegram_message_id, detail FROM agb.alert_event")
        .fetchall()
    )
    assert (event, detail) == ("delivered", None)
    assert chat_id == bot.chat_id
    assert message_id > 0


class Sent:
    """The real client, keeping the id of every message Telegram accepted."""

    def __init__(self, telegram: Telegram) -> None:
        self.telegram = telegram
        self.message_ids: list[int] = []

    def send_message(self, chat_id: str, text: str) -> int:
        message_id = self.telegram.send_message(chat_id, text)
        self.message_ids.append(message_id)
        return message_id

    def get_updates(self, offset: int | None) -> list[dict]:
        raise AssertionError("the live check never reads the chat")


def test_a_command_is_answered_in_the_real_chat_and_recorded(db):
    """The answer goes to the real chat as the bot sends it, without polling: each command is
    handed to the bot as if Telegram had delivered it. /log 100 runs over an audit log padded
    past Telegram's limit, so it goes out in parts."""
    telegram = Sent(Telegram.from_env())
    chat = os.environ["TELEGRAM_CHAT_ID"].strip()
    bot = Bot(telegram, chat)
    conn = db.connect("bot")
    for i in range(60):
        conn.execute(
            "INSERT INTO agb.command (actor, surface, raw_text, outcome, reason)"
            " VALUES ('telegram:0', 'telegram', %s, 'refused', 'not a command')",
            (f"live check padding {i:02d}, long enough to fill the log past the limit",),
        )
    assert len(readout.log(conn, 100)) > MESSAGE_MAX

    for text in ("/help", "/log 100"):
        now = datetime.now(UTC)
        message = {
            "message_id": None,
            "chat": {"id": chat},
            "from": {"id": 0},
            "date": int(now.timestamp()),
            "text": text,
        }
        bot.answer(conn, message, now)

    assert db.connect("cli").execute(
        "SELECT verb, outcome FROM agb.command WHERE verb IS NOT NULL ORDER BY id"
    ).fetchall() == [("help", "accepted"), ("log", "accepted")]
    # The bot logs a failed send rather than raising, so count what Telegram took: /help in
    # one message and the log in more than one.
    assert len(telegram.message_ids) > 2
    assert all(message_id > 0 for message_id in telegram.message_ids)
