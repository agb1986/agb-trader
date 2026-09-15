"""Opt-in live check: a real alert delivered to the real chat through a real proxy, and its
`alert_event` carrying Telegram's message id.

Skipped unless AGB_LIVE_TELEGRAM=1 and TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID and HTTPS_PROXY
are set in the environment. It reads nothing else: no env file. It sends one message and
never reads the chat's messages, so it does not compete with a bot already polling the token.
The alert is recorded in a throwaway test database, like every other test's.
"""

import os
from datetime import UTC, datetime

import pytest

from agb.services.bot import ALERT_HEADER, Bot
from agb.telegram import Telegram

REQUIRED = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "HTTPS_PROXY")

pytestmark = pytest.mark.skipif(
    os.environ.get("AGB_LIVE_TELEGRAM") != "1" or not all(os.environ.get(k) for k in REQUIRED),
    reason="live Telegram check: set AGB_LIVE_TELEGRAM=1 and " + ", ".join(REQUIRED),
)


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
