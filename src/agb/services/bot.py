"""The Telegram bot service. Each cycle it beats its heartbeat, reads new messages, raises an
alert for each unhealthy finding, raises the pre-flight on the session before a rebalance,
and delivers every undelivered alert to the configured chat.

It holds no state beyond the Telegram update offset. Every message it sends is computed from
the database, and every delivery attempt is an `alert_event`. Alerts notify and never act.
"""

import logging
import os
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import psycopg

from agb import health
from agb.health import Finding
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.nyse import NEW_YORK, NyseCalendar, trading_date
from agb.telegram import Messenger, Telegram, TelegramError

log = logging.getLogger(__name__)

ACTOR = "bot"
CHANNEL = "telegram"
# The pre-flight goes out from this time, New York, on the session before each rebalance.
PREFLIGHT_AT = time(10, 0)
# A failed delivery is retried after this, doubling with each failure up to the cap.
RETRY_AFTER = timedelta(minutes=1)
RETRY_CAP = timedelta(hours=1)

ALERT_HEADER = "HEALTH ALERT — nothing has been done and nothing will be."
NO_COMMANDS = "No commands yet. Alerts and the pre-flight arrive in this chat."

Check = Callable[[psycopg.Connection], list[Finding]]


def alert_text(finding: Finding) -> str:
    return f"{ALERT_HEADER}\n\n{finding.lines()}"


def preflight_text(period: str, findings: list[Finding]) -> str:
    if not findings:
        return f"PRE-FLIGHT {period}\nrebalance tomorrow, all services healthy"
    count = f"{len(findings)} thing{'' if len(findings) == 1 else 's'} not healthy"
    items = "\n\n".join(finding.lines() for finding in findings)
    return f"PRE-FLIGHT {period}\nrebalance tomorrow, {count}\n\n{items}"


_RAISE = """
    INSERT INTO agb.alert (kind, subject, period, body, raised_by)
    VALUES (%(kind)s, %(subject)s, %(period)s, %(body)s, %(actor)s)
    ON CONFLICT (kind, subject, period) DO NOTHING
    RETURNING id
"""

# Undelivered alerts whose last failure is older than the backoff for their failure count.
_DUE = """
    SELECT id, kind, subject, period, body FROM agb.undelivered_alert
    WHERE failures = 0
       OR last_failed_at <= now() - least(%(first)s * power(2, least(failures - 1, 20)), %(cap)s)
    ORDER BY id
"""

_RECORD = """
    INSERT INTO agb.alert_event (alert_id, event, channel, chat_id, telegram_message_id,
                                 detail, actor)
    VALUES (%(alert_id)s, %(event)s, %(channel)s, %(chat_id)s, %(message_id)s,
            %(detail)s, %(actor)s)
"""


class Bot:
    """One bot cycle, run by `serve`. Telegram and the clock are injectable for tests.

    `halts` and `broker` report open halt causes and a broker mismatch for the pre-flight.
    """

    def __init__(
        self,
        telegram: Messenger,
        chat_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        *,
        stale_after: timedelta = health.STALE_AFTER,
        preflight_at: time = PREFLIGHT_AT,
        retry_after: timedelta = RETRY_AFTER,
        retry_cap: timedelta = RETRY_CAP,
        halts: Check = health.open_halt_causes,
        broker: Check = health.broker_mismatch,
    ) -> None:
        self.telegram = telegram
        self.chat_id = chat_id
        self.clock = clock
        self.stale_after = stale_after
        self.preflight_at = preflight_at
        self.retry_after = retry_after
        self.retry_cap = retry_cap
        self.halts = halts
        self.broker = broker
        self._offset: int | None = None
        self._calendar: NyseCalendar | None = None

    def __call__(self, conn: psycopg.Connection) -> None:
        now = self.clock()
        self.poll()
        for finding in self.findings(conn, now):
            self.raise_alert(
                conn, finding.kind, finding.subject, finding.period, alert_text(finding)
            )
        self.preflight(conn, now)
        self.deliver(conn)

    # Messages in.

    def poll(self) -> None:
        """Read new messages. Only the configured chat is ever answered."""
        try:
            updates = self.telegram.get_updates(self._offset)
        except TelegramError as exc:
            log.warning("could not read messages: %s", exc)
            return
        for update in updates:
            self._offset = max(self._offset or 0, update["update_id"] + 1)
            message = update.get("message")
            if message is None:
                continue
            if str(message.get("chat", {}).get("id")) == self.chat_id:
                self.answer(message)
            else:
                self.refuse(message)

    def answer(self, message: dict[str, Any]) -> None:
        # The commands, and a `command` row for every message, arrive with #25.
        try:
            self.telegram.send_message(self.chat_id, NO_COMMANDS)
        except TelegramError as exc:
            log.warning("could not answer: %s", exc)

    def refuse(self, message: dict[str, Any]) -> None:
        """A message from any other chat is never answered, only logged."""
        log.warning(
            "refused a message from chat %s, user %s",
            message.get("chat", {}).get("id"),
            message.get("from", {}).get("id"),
        )

    # Alerts.

    def findings(self, conn: psycopg.Connection, now: datetime) -> list[Finding]:
        return health.stale_heartbeats(conn, now, self.stale_after) + health.overdue(conn, now)

    def raise_alert(
        self, conn: psycopg.Connection, kind: str, subject: str, period: str, body: str
    ) -> bool:
        """Raise an alert once: an alert with the same key already raised adds nothing."""
        raised = conn.execute(
            _RAISE,
            {
                "kind": kind,
                "subject": subject,
                "period": period,
                "body": body,
                "actor": ACTOR,
            },
        ).fetchone()
        if raised:
            log.info("raised alert %s: %s %s %s", raised[0], kind, subject, period)
        return raised is not None

    def calendar(self, today: date) -> NyseCalendar:
        if self._calendar is None or not self._calendar.covers(today, today + timedelta(days=14)):
            self._calendar = NyseCalendar(today - timedelta(days=7), today + timedelta(days=90))
        return self._calendar

    def preflight(self, conn: psycopg.Connection, now: datetime) -> None:
        """On the session before a rebalance, from `preflight_at` New York, raise the
        pre-flight once: healthy, or a list of everything that is not."""
        today = trading_date(now)
        calendar = self.calendar(today)
        if not calendar.is_session(today) or now.astimezone(NEW_YORK).time() < self.preflight_at:
            return
        trade = calendar.next_session(today)
        if trade.month == today.month:
            return
        period = f"{trade:%Y-%m}"
        if conn.execute(
            "SELECT 1 FROM agb.alert WHERE kind = 'preflight' AND period = %s", (period,)
        ).fetchone():
            return
        findings = [
            *self.findings(conn, now),
            *health.rebalance_not_scheduled(conn, period, trade),
            *self.halts(conn),
            *self.broker(conn),
        ]
        self.raise_alert(conn, "preflight", "rebalance", period, preflight_text(period, findings))

    def deliver(self, conn: psycopg.Connection) -> None:
        """Send each undelivered alert that is not backing off, and record the attempt."""
        due = conn.execute(_DUE, {"first": self.retry_after, "cap": self.retry_cap}).fetchall()
        for alert_id, kind, subject, period, body in due:
            try:
                message_id = self.telegram.send_message(self.chat_id, body)
            except TelegramError as exc:
                self._record(conn, alert_id, "failed", detail=str(exc))
                log.warning("could not deliver alert %s (%s %s): %s", alert_id, kind, subject, exc)
                continue
            self._record(conn, alert_id, "delivered", message_id=message_id)
            log.info("delivered alert %s (%s %s %s)", alert_id, kind, subject, period)

    def _record(
        self,
        conn: psycopg.Connection,
        alert_id: int,
        event: str,
        *,
        message_id: int | None = None,
        detail: str | None = None,
    ) -> None:
        conn.execute(
            _RECORD,
            {
                "alert_id": alert_id,
                "event": event,
                "channel": CHANNEL,
                "chat_id": self.chat_id,
                "message_id": message_id,
                "detail": detail,
                "actor": ACTOR,
            },
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    try:
        if not chat_id:
            raise ValueError("TELEGRAM_CHAT_ID is not set")
        telegram = Telegram.from_env()
    except ValueError as exc:
        sys.exit(f"bot: {exc}")
    bot = Bot(telegram, chat_id, stale_after=health.stale_after_from_env())
    serve(ACTOR, interval_from_env(), stop_on_signals(), bot)


if __name__ == "__main__":
    main()
