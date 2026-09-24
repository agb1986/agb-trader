"""The Telegram bot service. Each cycle it beats its heartbeat, reads new messages, raises an
alert for each unhealthy finding, raises the pre-flight on the session before a rebalance,
and delivers every undelivered alert to the configured chat.

It holds no state beyond the Telegram update offset. Every message it sends is computed from
the database, and every delivery attempt is an `alert_event`. Alerts notify and never act.

Every message from the configured chat is a `command` row. `/pause` opens the `pause` halt
cause and `/resume` clears every open cause, one clear each; both are written here, so the
database latch holds the moment the reply goes out. The broker's `suspend_trade` is
execution's to set, since only execution holds the broker's keys: it picks up the accepted
command, and its confirmation arrives in this chat as an alert. The other commands are
refused until they are built.
"""

import logging
import os
import re
import sys
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

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
NO_COMMANDS = "Only /pause and /resume so far. Alerts and the pre-flight arrive in this chat."
NOT_YET = "not available yet"

Check = Callable[[psycopg.Connection], list[Finding]]


def alert_text(finding: Finding) -> str:
    return f"{ALERT_HEADER}\n\n{finding.lines()}"


def preflight_text(period: str, findings: list[Finding]) -> str:
    if not findings:
        return f"PRE-FLIGHT {period}\nrebalance tomorrow, all services healthy"
    count = f"{len(findings)} thing{'' if len(findings) == 1 else 's'} not healthy"
    items = "\n\n".join(finding.lines() for finding in findings)
    return f"PRE-FLIGHT {period}\nrebalance tomorrow, {count}\n\n{items}"


VERB = re.compile(r"[a-z][a-z_]*")
_COMMAND = """
    INSERT INTO agb.command (actor, surface, raw_text, verb, args, outcome, reason)
    VALUES (%(actor)s, %(surface)s, %(raw_text)s, %(verb)s, %(args)s, %(outcome)s, %(reason)s)
"""
_PAUSE_OPEN = "SELECT at, actor FROM agb.halt_open WHERE cause = 'pause'"
_OPEN_CAUSES = "SELECT cause FROM agb.halt_open ORDER BY at, cause"
_HALT = """
    INSERT INTO agb.halt_event (cause, action, actor, reason) VALUES (%s, %s, %s, %s)
"""


def parse(text: str) -> tuple[str | None, list[str]]:
    """A message's verb and arguments: `/pause@agb_bot now` is ("pause", ["now"]). Text that
    is not a command has no verb."""
    words = text.split()
    if not words or not words[0].startswith("/"):
        return None, words
    verb = words[0][1:].split("@", 1)[0].lower()
    return (verb if VERB.fullmatch(verb) else None), words[1:]


def operator(message: dict[str, Any]) -> str:
    """The actor a message is attributed to: the Telegram user who sent it."""
    return f"telegram:{message.get('from', {}).get('id', 'unknown')}"


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
        self.poll(conn)
        for finding in [*self.findings(conn, now), *self.broker(conn)]:
            self.raise_alert(
                conn, finding.kind, finding.subject, finding.period, alert_text(finding)
            )
        self.preflight(conn, now)
        self.deliver(conn)

    # Messages in.

    def poll(self, conn: psycopg.Connection) -> None:
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
                self.answer(conn, message)
            else:
                self.refuse(message)

    def answer(self, conn: psycopg.Connection, message: dict[str, Any]) -> None:
        """Act on a message from the configured chat, record it as a `command`, and reply."""
        text = message.get("text") or ""
        verb, args = parse(text)
        actor = operator(message)
        if verb == "pause":
            reply = self.pause(conn, actor, text)
        elif verb == "resume":
            reply = self.resume(conn, actor, text)
        else:
            self.record(conn, actor, text, verb, {"args": args}, "refused", NOT_YET)
            reply = NO_COMMANDS
        try:
            self.telegram.send_message(self.chat_id, reply)
        except TelegramError as exc:
            log.warning("could not answer: %s", exc)

    def record(
        self,
        conn: psycopg.Connection,
        actor: str,
        text: str,
        verb: str | None,
        args: dict[str, Any],
        outcome: str,
        reason: str | None = None,
    ) -> None:
        conn.execute(
            _COMMAND,
            {
                "actor": actor,
                "surface": CHANNEL,
                "raw_text": text,
                "verb": verb,
                "args": Jsonb(args),
                "outcome": outcome,
                "reason": reason,
            },
        )

    def pause(self, conn: psycopg.Connection, actor: str, text: str) -> str:
        """Open the `pause` cause, unless it is open already, and ask execution to set the
        broker's `suspend_trade`. Returns the reply."""
        with conn.transaction():
            already = conn.execute(_PAUSE_OPEN).fetchone()
            if already is None:
                conn.execute(_HALT, ("pause", "set", actor, "/pause"))
            self.record(
                conn, actor, text, "pause", {"already_open": already is not None}, "accepted"
            )
        if already is None:
            opened = "halt cause pause: open. No further order goes out."
        else:
            opened = (
                f"halt cause pause: already open since {health.et(already[0])}, by {already[1]}."
            )
        log.info("pause by %s", actor)
        return (
            f"PAUSED\n{opened}\nbroker suspend_trade: asked on. Execution confirms it here, "
            "with anything it cancelled."
        )

    def resume(self, conn: psycopg.Connection, actor: str, text: str) -> str:
        """Clear every open halt cause, one clear each, and ask execution to unset the
        broker's `suspend_trade`. Returns the reply, which lists what was cleared."""
        with conn.transaction():
            causes = [cause for (cause,) in conn.execute(_OPEN_CAUSES).fetchall()]
            for cause in causes:
                conn.execute(_HALT, (cause, "clear", actor, "/resume"))
            self.record(conn, actor, text, "resume", {"cleared": causes}, "accepted")
        log.info("resume by %s, cleared: %s", actor, ", ".join(causes) or "nothing")
        cleared = f"cleared: {', '.join(causes)}" if causes else "cleared: nothing was open"
        return (
            f"RESUMED\n{cleared}\nbroker suspend_trade: asked off. Execution confirms it "
            "here.\nA halted rebalance is not resubmitted."
        )

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
