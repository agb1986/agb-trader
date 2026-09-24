"""The Telegram bot, with a fake Telegram: alerts and their delivery, the pre-flight, the
configured chat, and the bot's own heartbeat. The commands are in tests/test_bot_commands.py."""

import logging
import threading
from datetime import date, datetime, timedelta

import pytest
from psycopg import errors

from agb import heartbeat
from agb.health import Finding
from agb.nyse import NEW_YORK, NyseCalendar
from agb.roles import OWNER
from agb.services import bot as bot_service
from agb.services.bot import Bot, help_text
from agb.telegram import TelegramError

CHAT = "4242"
# 12:00 New York on an ordinary mid-month session.
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=NEW_YORK)
# 10:05 New York on 2026-09-30, the session before the October rebalance.
PREFLIGHT = datetime(2026, 9, 30, 10, 5, tzinfo=NEW_YORK)


class FakeTelegram:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.updates: list[dict] = []
        self.offsets: list[int | None] = []
        self.fail: str | None = None

    def send_message(self, chat_id: str, text: str) -> int:
        if self.fail:
            raise TelegramError(self.fail)
        self.sent.append((chat_id, text))
        return 1000 + len(self.sent)

    def get_updates(self, offset: int | None) -> list[dict]:
        self.offsets.append(offset)
        if self.fail:
            raise TelegramError(self.fail)
        return [u for u in self.updates if offset is None or u["update_id"] >= offset]


@pytest.fixture
def telegram():
    return FakeTelegram()


def beat_at(db, service, at):
    """A heartbeat last beaten at `at`. Only the owner can write one for another time."""
    db.connect(OWNER).execute(
        "INSERT INTO agb.heartbeat (service, started_at, beat_at) VALUES (%s, %s, %s)",
        (service, at, at),
    )


def expect(db, kind, period, due_at):
    db.connect("scheduler").execute(
        "INSERT INTO agb.expected_event (kind, period, due_at, created_by) "
        "VALUES (%s, %s, %s, 'scheduler')",
        (kind, period, due_at),
    )


def alerts(db):
    return (
        db.connect("cli")
        .execute("SELECT kind, subject, period FROM agb.alert ORDER BY id")
        .fetchall()
    )


def events(db):
    return (
        db.connect("cli")
        .execute(
            "SELECT a.kind, e.event, e.channel, e.chat_id, e.telegram_message_id, e.detail, "
            "e.actor FROM agb.alert_event e JOIN agb.alert a ON a.id = e.alert_id ORDER BY e.id"
        )
        .fetchall()
    )


@pytest.fixture
def rebalance_ahead(db):
    """The next rebalance is scheduled, so the horizon check is quiet."""
    expect(db, "submission", "2026-10", datetime(2026, 10, 1, 15, 45, tzinfo=NEW_YORK))


# Alerts.


def test_a_stale_heartbeat_and_an_overdue_event_each_raise_one_alert(db, telegram, rebalance_ahead):
    beat_at(db, "scheduler", NOW - timedelta(minutes=14))
    beat_at(db, "backup", NOW - timedelta(seconds=20))
    # The bot never reports itself: if it can report, it is running.
    beat_at(db, "bot", NOW - timedelta(hours=1))
    expect(db, "approval_request", "2026-10", NOW - timedelta(minutes=12))
    conn = db.connect("bot")

    bot = Bot(telegram, CHAT, lambda: NOW)
    bot(conn)
    bot(conn)
    # A restarted bot a minute later holds nothing in memory, and still adds nothing.
    Bot(FakeTelegram(), CHAT, lambda: NOW + timedelta(minutes=1))(conn)

    assert alerts(db) == [
        ("stale_heartbeat", "scheduler", "2026-09-15T15:46:00Z"),
        ("overdue", "approval_request", "2026-10"),
    ]
    assert telegram.sent == [
        (
            CHAT,
            "HEALTH ALERT — nothing has been done and nothing will be.\n\n"
            "  stale heartbeat: scheduler\n"
            "    last beat 14m 0s ago, at 2026-09-15 11:46 ET",
        ),
        (
            CHAT,
            "HEALTH ALERT — nothing has been done and nothing will be.\n\n"
            "  overdue: approval_request 2026-10\n"
            "    due 2026-09-15 11:48 ET, 12m 0s ago",
        ),
    ]
    assert events(db) == [
        ("stale_heartbeat", "delivered", "telegram", CHAT, 1001, None, "bot"),
        ("overdue", "delivered", "telegram", CHAT, 1002, None, "bot"),
    ]


def test_a_service_that_goes_stale_again_raises_a_new_alert(db, telegram, rebalance_ahead):
    beat_at(db, "scheduler", NOW - timedelta(minutes=10))
    conn = db.connect("bot")
    Bot(telegram, CHAT, lambda: NOW)(conn)
    db.connect(OWNER).execute(
        "UPDATE agb.heartbeat SET beat_at = %s WHERE service = 'scheduler'",
        (NOW + timedelta(minutes=2),),
    )
    Bot(telegram, CHAT, lambda: NOW + timedelta(minutes=5))(conn)
    assert alerts(db) == [("stale_heartbeat", "scheduler", "2026-09-15T15:50:00Z")]

    Bot(telegram, CHAT, lambda: NOW + timedelta(minutes=8))(conn)
    assert alerts(db)[1:] == [("stale_heartbeat", "scheduler", "2026-09-15T16:02:00Z")]
    assert len(telegram.sent) == 2


def test_the_stale_threshold_is_configurable(db, telegram, rebalance_ahead):
    beat_at(db, "scheduler", NOW - timedelta(minutes=3))
    conn = db.connect("bot")
    Bot(telegram, CHAT, lambda: NOW)(conn)
    assert alerts(db) == []
    Bot(telegram, CHAT, lambda: NOW, stale_after=timedelta(minutes=2))(conn)
    assert [kind for kind, _, _ in alerts(db)] == ["stale_heartbeat"]


def test_no_rebalance_within_horizon_alerts_once_until_one_is_scheduled(db, telegram):
    conn = db.connect("bot")
    Bot(telegram, CHAT, lambda: NOW)(conn)
    Bot(telegram, CHAT, lambda: NOW + timedelta(hours=1))(conn)
    assert alerts(db) == [("no_rebalance_within_horizon", "submission", "none")]
    [(_, text)] = telegram.sent
    # The horizon is 40 days by default.
    assert text.splitlines()[2:] == [
        "  no rebalance within horizon",
        "    none due before 2026-10-25; last scheduled none",
    ]


# Delivery.


def test_a_failed_delivery_is_recorded_and_retried_after_a_backoff(db, telegram, rebalance_ahead):
    expect(db, "approval_request", "2026-10", NOW - timedelta(minutes=1))
    conn, cli = db.connect("bot"), db.connect("cli")
    telegram.fail = "sendMessage: Bad Gateway"
    bot = Bot(telegram, CHAT, lambda: NOW)
    bot(conn)
    bot(conn)  # still backing off, so not tried again
    assert events(db) == [
        ("overdue", "failed", "telegram", CHAT, None, "sendMessage: Bad Gateway", "bot")
    ]
    assert cli.execute(
        "SELECT kind, failures, last_failure FROM agb.undelivered_alert"
    ).fetchall() == [("overdue", 1, "sendMessage: Bad Gateway")]

    telegram.fail = None
    bot.retry_after = timedelta(0)
    bot(conn)
    bot(conn)
    assert events(db)[1:] == [("overdue", "delivered", "telegram", CHAT, 1001, None, "bot")]
    assert cli.execute("SELECT * FROM agb.undelivered_alert").fetchall() == []
    assert len(telegram.sent) == 1


def test_the_backoff_doubles_up_to_the_cap(db, telegram, rebalance_ahead):
    expect(db, "approval_request", "2026-10", NOW - timedelta(minutes=1))
    conn = db.connect("bot")
    telegram.fail = "sendMessage: timed out"
    bot = Bot(telegram, CHAT, lambda: NOW, retry_after=timedelta(0))
    for _ in range(3):
        bot(conn)
    assert len(events(db)) == 3
    # With a minute's backoff doubling from the first failure, the fourth waits four minutes;
    # a cap of zero lets it through at once.
    bot.retry_after = timedelta(minutes=1)
    bot(conn)
    assert len(events(db)) == 3
    bot.retry_cap = timedelta(0)
    bot(conn)
    assert len(events(db)) == 4


def test_alert_event_rules(db, telegram, rebalance_ahead):
    expect(db, "approval_request", "2026-10", NOW - timedelta(minutes=1))
    Bot(telegram, CHAT, lambda: NOW)(db.connect("bot"))
    bot = db.connect("bot")
    (alert_id,) = bot.execute("SELECT id FROM agb.alert").fetchone()
    insert = (
        "INSERT INTO agb.alert_event (alert_id, event, channel, telegram_message_id, detail, "
        "actor) VALUES (%s, %s, 'telegram', %s, %s, 'bot')"
    )
    with pytest.raises(errors.CheckViolation):
        bot.execute(insert, (alert_id, "delivered", None, None))
    with pytest.raises(errors.CheckViolation):
        bot.execute(insert, (alert_id, "failed", None, None))
    with pytest.raises(errors.UniqueViolation):
        bot.execute(insert, (alert_id, "delivered", 99, None))
    with pytest.raises(errors.UniqueViolation):
        bot.execute(
            "INSERT INTO agb.alert (kind, subject, period, body, raised_by) "
            "VALUES ('overdue', 'approval_request', '2026-10', 'again', 'bot')"
        )


@pytest.mark.parametrize("role", ["pipeline", "cli"])
def test_only_the_alerting_services_raise_alerts(db, role):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute(
            "INSERT INTO agb.alert (kind, subject, period, body, raised_by) "
            "VALUES ('overdue', 'x', 'y', 'z', 'bot')"
        )


@pytest.mark.parametrize("role", ["scheduler", "execution", "bot", "ui"])
def test_each_alerting_service_can_raise_an_alert(db, role):
    db.connect(role).execute(
        "INSERT INTO agb.alert (kind, subject, period, body, raised_by) "
        "VALUES ('overdue', %s, 'y', 'z', %s)",
        (role, role),
    )
    assert db.connect("cli").execute("SELECT count(*) FROM agb.undelivered_alert").fetchone() == (
        1,
    )


# The configured chat.


def test_only_the_configured_chat_is_answered(db, telegram, rebalance_ahead, caplog):
    telegram.updates = [
        {"update_id": 7, "message": {"chat": {"id": 999}, "from": {"id": 999}, "text": "/status"}},
        {"update_id": 8, "message": {"chat": {"id": int(CHAT)}, "from": {"id": 1}, "text": "hi"}},
        {"update_id": 9, "edited_message": {"chat": {"id": int(CHAT)}, "text": "hello"}},
    ]
    conn = db.connect("bot")
    bot = Bot(telegram, CHAT, lambda: NOW)
    with caplog.at_level(logging.WARNING, logger="agb.services.bot"):
        bot(conn)
        bot(conn)
    assert telegram.sent == [(CHAT, help_text("not a command"))]
    assert "refused a message from chat 999, user 999" in caplog.text
    # Each read confirms the ones before it, so nothing is answered twice.
    assert telegram.offsets == [None, 10]
    # Both messages are on record, the other chat's included; the edit is not a message.
    rows = (
        db.connect("cli")
        .execute("SELECT actor, chat_id, verb, outcome, reason FROM agb.command ORDER BY id")
        .fetchall()
    )
    assert rows == [
        ("telegram:999", "999", "status", "refused", "not the configured chat"),
        ("telegram:1", CHAT, None, "refused", "not a command"),
    ]


def test_telegram_unreachable_does_not_stop_the_cycle(db, telegram, rebalance_ahead, caplog):
    expect(db, "approval_request", "2026-10", NOW - timedelta(minutes=1))
    telegram.fail = "getUpdates: timed out"
    with caplog.at_level(logging.WARNING, logger="agb.services.bot"):
        Bot(telegram, CHAT, lambda: NOW)(db.connect("bot"))
    assert "could not read messages: getUpdates: timed out" in caplog.text
    assert [event for _, event, *_ in events(db)] == ["failed"]


# The pre-flight.


def moc_cutoff(day):
    return NyseCalendar(day - timedelta(days=7), day + timedelta(days=7)).moc_cutoff(day)


def test_the_preflight_when_all_is_healthy(db, telegram):
    expect(db, "submission", "2026-10", moc_cutoff(date(2026, 10, 1)))
    beat_at(db, "scheduler", PREFLIGHT - timedelta(seconds=10))
    conn = db.connect("bot")
    bot = Bot(telegram, CHAT, lambda: PREFLIGHT)
    bot(conn)
    bot(conn)
    assert telegram.sent == [(CHAT, "PRE-FLIGHT 2026-10\nrebalance tomorrow, all services healthy")]
    assert alerts(db) == [("preflight", "rebalance", "2026-10")]
    assert events(db) == [("preflight", "delivered", "telegram", CHAT, 1001, None, "bot")]


def test_the_preflight_lists_everything_unhealthy(db, telegram):
    expect(db, "submission", "2026-10", moc_cutoff(date(2026, 10, 1)))
    expect(db, "data_pull", "2026-09-29", datetime(2026, 9, 30, 9, 30, tzinfo=NEW_YORK))
    beat_at(db, "scheduler", PREFLIGHT - timedelta(minutes=14))

    def halts(conn):
        return [Finding("halt", "gate_breach", "1", "halt open: gate_breach", "since 2026-09-29")]

    def broker(conn):
        return [Finding("broker", "suspend_trade", "1", "broker suspend_trade: on", "no halt open")]

    Bot(telegram, CHAT, lambda: PREFLIGHT, halts=halts, broker=broker)(db.connect("bot"))

    assert [kind for kind, _, _ in alerts(db)] == [
        "stale_heartbeat",
        "overdue",
        "broker",
        "preflight",
    ]
    assert telegram.sent[-1] == (
        CHAT,
        "PRE-FLIGHT 2026-10\n"
        "rebalance tomorrow, 4 things not healthy\n"
        "\n"
        "  stale heartbeat: scheduler\n"
        "    last beat 14m 0s ago, at 2026-09-30 09:51 ET\n"
        "\n"
        "  overdue: data_pull 2026-09-29\n"
        "    due 2026-09-30 09:30 ET, 35m 0s ago\n"
        "\n"
        "  halt open: gate_breach\n"
        "    since 2026-09-29\n"
        "\n"
        "  broker suspend_trade: on\n"
        "    no halt open",
    )


def test_the_preflight_reports_a_rebalance_that_was_never_scheduled(db, telegram):
    Bot(telegram, CHAT, lambda: PREFLIGHT)(db.connect("bot"))
    [preflight] = [text for _, text in telegram.sent if text.startswith("PRE-FLIGHT")]
    assert preflight.splitlines()[1] == "rebalance tomorrow, 2 things not healthy"
    assert "  rebalance 2026-10 not scheduled\n    no submission expected for 2026-10-01" in (
        preflight
    )
    assert "  no rebalance within horizon\n" in preflight


@pytest.mark.parametrize(
    "now, period",
    [
        (datetime(2026, 9, 30, 9, 59, tzinfo=NEW_YORK), None),  # before 10:00
        (datetime(2026, 9, 29, 12, 0, tzinfo=NEW_YORK), None),  # not the last session
        (datetime(2026, 10, 1, 12, 0, tzinfo=NEW_YORK), None),  # the rebalance session itself
        (datetime(2026, 9, 30, 15, 59, tzinfo=NEW_YORK), "2026-10"),
        # October ends on a Saturday, so Friday the 30th is the session before.
        (datetime(2026, 10, 30, 10, 0, tzinfo=NEW_YORK), "2026-11"),
        (datetime(2026, 10, 31, 10, 0, tzinfo=NEW_YORK), None),
        # New Year's Eve, the session before a January rebalance.
        (datetime(2026, 12, 31, 10, 0, tzinfo=NEW_YORK), "2027-01"),
    ],
)
def test_the_preflight_is_sent_only_on_the_session_before_a_rebalance(db, telegram, now, period):
    Bot(telegram, CHAT, lambda: now)(db.connect("bot"))
    raised = [p for kind, _, p in alerts(db) if kind == "preflight"]
    assert raised == ([period] if period else [])


# The service.


def test_the_bot_beats_its_heartbeat(db, telegram, rebalance_ahead, monkeypatch):
    for key, value in db.pg_env("bot").items():
        monkeypatch.setenv(key, value)
    bot = Bot(telegram, CHAT, lambda: NOW)
    stop = threading.Event()
    cycles = []

    def cycle(conn):
        bot(conn)
        cycles.append(1)
        if len(cycles) == 2:
            stop.set()

    heartbeat.serve("bot", 0.01, stop, cycle)
    assert db.connect("cli").execute("SELECT service FROM agb.heartbeat").fetchall() == [("bot",)]
    assert telegram.offsets == [None, None]


@pytest.mark.parametrize(
    "env, missing",
    [
        ({"TELEGRAM_BOT_TOKEN": "x", "HTTPS_PROXY": "http://vpn:8888"}, "TELEGRAM_CHAT_ID"),
        ({"TELEGRAM_CHAT_ID": CHAT, "HTTPS_PROXY": "http://vpn:8888"}, "TELEGRAM_BOT_TOKEN"),
        ({"TELEGRAM_BOT_TOKEN": "x", "TELEGRAM_CHAT_ID": CHAT}, "HTTPS_PROXY"),
    ],
)
def test_the_bot_will_not_start_without_its_settings(monkeypatch, env, missing):
    for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        monkeypatch.delenv(key, raising=False)
    for key in ("http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(bot_service, "serve", lambda *args: pytest.fail("the bot started"))
    with pytest.raises(SystemExit, match=missing):
        bot_service.main()
