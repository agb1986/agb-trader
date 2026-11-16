"""The Telegram bot service. Each cycle it beats its heartbeat, reads new messages and
answers the read-only commands, raises an alert for each unhealthy finding and for each
performance reading past the certified range, a fill
confirmation for each rebalance that filled and a gate-halt alert for each one the checks
halted, puts each gated rebalance to the human and reminds them before its cutoff, raises the
pre-flight on the session before a rebalance, and delivers every undelivered alert to the
configured chat. Each of those duties runs under its own handler: one that raises is logged,
raises one alert for its run of failures, and leaves the rest to run, so delivery is always
attempted.

It holds no state beyond a cached Telegram update offset and which duties are failing. Every
message it sends is computed from the database, every message it receives is a `command` row
whatever became of it, and every delivery attempt is an `alert_event`. Alerts notify and never act.
Each update it handles is a `telegram_update` row written with its command, so a restart neither
loses nor repeats one, and a command older than `STALE_COMMAND` is refused rather than acted on.

`/pause`, `/resume` and `/liquidate` are the kill switch, and `/approve` and `/reject`
answer the approval request. Neither is written here: the kill switch lives in
`agb.killswitch` and the approval loop in `agb.approval`, and the local UI calls the same
functions with a source of its own, so the two surfaces cannot drift apart. This service only
turns a message into that source — who sent it, which surface it came from, and the text as
typed — and sends back the reply it is given, which is why every one of these is a `command`
row whatever became of it. The other commands only read.
"""

import logging
import os
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from time import sleep as wait
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from agb import approval, health, jobs, killswitch, ladder, readout, schedule
from agb.cli import format_age
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
# How many extra attempts the poll gets inside one cycle, and how long it waits between them.
# The bot reaches Telegram only through the VPN's proxy, and the failure that dominates its log
# is a handshake that never completes: a second attempt opens a new connection, which is
# usually all it takes. One extra attempt only, because the rest of the cycle -- the alerts it
# raises and the deliveries it owes -- waits behind this read.
POLL_RETRIES = 1
POLL_BACKOFF = 1.0

ALERT_HEADER = "HEALTH ALERT — nothing has been done and nothing will be."
# The performance readings get their own header and a footer, because the question they
# provoke is "what now?" and the honest answer is "whatever you decide, and nothing else".
PERFORMANCE_HEADER = "PERFORMANCE ALERT — nothing has been done and nothing will be."
PERFORMANCE_FOOTER = (
    "/pause stops the next rebalance and /liquidate closes every position. "
    "Both are yours to choose; nothing in this message chooses for you, "
    "and no part of the system will act on it."
)
# A duty of the cycle that raised. One alert per run of failures: the cycle the run began in
# identifies it, so a duty that recovers and fails again raises a new one.
CYCLE_FAILURE = "cycle_failure"
# How much of a failure's message the alert carries.
FAILURE_MAX = 200
FILL_CONFIRMATION = "fill_confirmation"
GATE_HALT = "gate_halt"
# The unprompted rebalance messages look back over this many of the latest live baskets.
RECENT_BASKETS = 3
# Telegram refuses a message longer than this; a longer answer goes out in parts.
MESSAGE_MAX = 4096
# How long a liquidation token stays valid. The kill switch owns it; it is named here
# because this is the surface that tells the operator how long they have.
TOKEN_VALID = killswitch.TOKEN_VALID
# A command older than this when the bot reads it is refused, not acted on. Telegram keeps an
# unread message for a day, so a `/resume` typed before an outage would otherwise clear a halt
# raised since. Five minutes is several poll cycles and a retried poll, and far less than the
# time it takes for the state a command was meant for to change.
STALE_COMMAND = timedelta(minutes=5)

# The commands this bot answers, and what each shows, in the order /help lists them.
COMMANDS = {
    "status": ("/status", "mode, open halts, the account, the latest and next rebalance"),
    "positions": ("/positions", "the latest broker snapshot, name by name"),
    "pnl": ("/pnl [YYYY-MM]", "realised return from account equity, for a month or all time"),
    "pending": ("/pending", "the rebalance waiting for your answer, as the request shows it"),
    "approve": ("/approve <reference>", "approve the pending rebalance, e.g. /approve 2026-10"),
    "reject": ("/reject <reference> [reason]", "reject the pending rebalance: no trades"),
    "basket": ("/basket", "the full target of the current or most recent rebalance"),
    "why": ("/why <TICKER>", "why a name scored as it did: rank, safety score, rubric, sources"),
    "gate": ("/gate", "every check of the latest gate run, with what it measured"),
    "level": ("/level", "the autonomy level, what it means, and the progress towards the next"),
    "log": (f"/log [1-{readout.LOG_MAX}]", "the latest events, oldest first"),
    "pause": ("/pause", "open the pause halt cause and suspend trading at the broker"),
    "resume": ("/resume", "clear every open halt cause and lift the broker suspension"),
    "liquidate": ("/liquidate [token]", "close every position at market, in two steps"),
    "help": ("/help", "this list"),
}
# Commands the spec names that later tickets bring. They are refused, and recorded.
LATER: frozenset[str] = frozenset()
# Commands whose arguments `agb.approval` checks, against what is pending.
DECISIONS = frozenset({"approve", "reject"})
VERB = re.compile(r"^[a-z][a-z_]*$")
# A ticker as the lake holds it, which is what `/why` takes.
TICKER = re.compile(r"^[A-Za-z][A-Za-z0-9.\-]{0,9}$")

Check = Callable[[psycopg.Connection], list[Finding]]


def help_text(reason: str | None = None) -> str:
    items = [readout.Item(usage, shows) for usage, shows in COMMANDS.values()]
    return readout.message(reason or "COMMANDS", items)


@dataclass(frozen=True)
class Parsed:
    verb: str | None
    args: list[str]
    # Why the command is refused, or None when it is answered.
    refusal: str | None


def parse(text: str) -> Parsed:
    """Read a verb and its arguments from a message and check them, without running it."""
    words = text.split()
    if not words or not words[0].startswith("/"):
        return Parsed(None, words, "not a command")
    verb = words[0][1:].split("@", 1)[0].lower()
    args = words[1:]
    if not VERB.match(verb):
        return Parsed(None, args, "not a command")
    if verb == "start":
        verb = "help"
    if verb in LATER:
        return Parsed(verb, args, f"/{verb} is not available yet")
    if verb not in COMMANDS:
        return Parsed(verb, args, f"unknown command /{verb}")
    usage = COMMANDS[verb][0]
    if verb == "pnl":
        if len(args) > 1 or (args and not readout.PERIOD.match(args[0])):
            return Parsed(verb, args, f"usage: {usage}")
    elif verb == "log":
        if len(args) > 1 or (
            args and not (args[0].isdigit() and 1 <= int(args[0]) <= readout.LOG_MAX)
        ):
            return Parsed(verb, args, f"usage: {usage}")
    elif verb == "why":
        if len(args) != 1 or not TICKER.match(args[0]):
            return Parsed(verb, args, f"usage: {usage}")
    elif verb == "liquidate":
        if len(args) > 1:
            return Parsed(verb, args, f"usage: {usage}")
    elif args and verb not in DECISIONS:
        return Parsed(verb, args, f"usage: {usage}, no arguments")
    return Parsed(verb, args, None)


def split(text: str, limit: int = MESSAGE_MAX) -> list[str]:
    """Cut a message into parts Telegram accepts, between items where it can."""
    parts: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        while len(block) > limit:
            if current:
                parts.append(current)
                current = ""
            parts.append(block[:limit])
            block = block[limit:]
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) > limit:
            parts.append(current)
            current = block
        else:
            current = candidate
    if current:
        parts.append(current)
    return parts


def alert_text(finding: Finding) -> str:
    if finding.kind in health.PERFORMANCE:
        return f"{PERFORMANCE_HEADER}\n\n{finding.lines()}\n\n{PERFORMANCE_FOOTER}"
    return f"{ALERT_HEADER}\n\n{finding.lines()}"


def _cut(text: str, limit: int = FAILURE_MAX) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def preflight_text(period: str, findings: list[Finding]) -> str:
    if not findings:
        return f"PRE-FLIGHT {period}\nrebalance tomorrow, all services healthy"
    count = f"{len(findings)} thing{'' if len(findings) == 1 else 's'} not healthy"
    items = "\n\n".join(finding.lines() for finding in findings)
    return f"PRE-FLIGHT {period}\nrebalance tomorrow, {count}\n\n{items}"


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

_COMMAND = """
    INSERT INTO agb.command (actor, surface, raw_text, verb, args, outcome, reason, chat_id,
                             telegram_message_id)
    VALUES (%(actor)s, 'telegram', %(text)s, %(verb)s, %(args)s, %(outcome)s, %(reason)s,
            %(chat_id)s, %(message_id)s)
    RETURNING id
"""

# The latest live baskets that filled or were halted, for the unprompted messages.
_SETTLED = """
    SELECT id, reference, period, state FROM agb.basket_state
    WHERE kind = 'live' AND state IN ('filled', 'halted')
      AND id IN (SELECT id FROM agb.basket WHERE kind = 'live' ORDER BY id DESC LIMIT %s)
    ORDER BY id
"""
_RAISED = "SELECT 1 FROM agb.alert WHERE kind = %s AND subject = %s AND period = %s"

_RECORD = """
    INSERT INTO agb.alert_event (alert_id, event, channel, chat_id, telegram_message_id,
                                 detail, actor)
    VALUES (%(alert_id)s, %(event)s, %(channel)s, %(chat_id)s, %(message_id)s,
            %(detail)s, %(actor)s)
    RETURNING id
"""


# Claim an update before handling it. A row already there means it was handled.
_CLAIM = """
    INSERT INTO agb.telegram_update (update_id, chat_id, telegram_message_id, actor)
    VALUES (%(update_id)s, %(chat_id)s, %(message_id)s, %(actor)s)
    ON CONFLICT (update_id) DO NOTHING
    RETURNING update_id
"""
# Where to ask Telegram from on start: one past the newest update handled.
_OFFSET = "SELECT max(update_id) + 1 FROM agb.telegram_update"


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
        poll_retries: int = POLL_RETRIES,
        poll_backoff: float = POLL_BACKOFF,
        sleep: Callable[[float], None] = wait,
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
        self.poll_retries = max(0, poll_retries)
        self.poll_backoff = poll_backoff
        self._sleep = sleep
        self.halts = halts
        self.broker = broker
        self._offset: int | None = None
        self._calendar: NyseCalendar | None = None
        # Each duty that is failing, with the cycle its run of failures began in.
        self._failing: dict[str, datetime] = {}

    def __call__(self, conn: psycopg.Connection) -> None:
        now = self.clock()
        self._step(conn, "poll", now, lambda: self.poll(conn, now))
        self._step(conn, "findings", now, lambda: self.alerts(conn, now))
        self._step(conn, "rebalances", now, lambda: self.rebalances(conn))
        self._step(conn, "approvals", now, lambda: self.approvals(conn, now))
        self._step(conn, "preflight", now, lambda: self.preflight(conn, now))
        self._step(conn, "deliver", now, lambda: self.deliver(conn))

    def _step(
        self, conn: psycopg.Connection, step: str, now: datetime, duty: Callable[[], None]
    ) -> None:
        """Run one duty of the cycle under its own handler, so a duty that raises stops only
        itself: the failure is logged, the first failure of a run raises one alert, and the
        cycle goes on to the next duty, delivery included. A duty that succeeds ends its run.

        A connection that is gone is the exception, because no later duty could reach the
        database either: that error propagates, as it always has, and Compose restarts the
        service.
        """
        try:
            duty()
        except Exception as exc:
            if conn.closed or conn.broken:
                raise
            log.exception("the %s duty failed", step)
            if step not in self._failing:
                self._failing[step] = now
                self._raise_failure(conn, step, now, exc)
            return
        if self._failing.pop(step, None) is not None:
            log.info("the %s duty ran again", step)

    def _raise_failure(
        self, conn: psycopg.Connection, step: str, since: datetime, exc: Exception
    ) -> None:
        finding = Finding(
            CYCLE_FAILURE,
            step,
            since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"bot duty failed: {step}",
            f"{type(exc).__name__}: {_cut(str(exc))}; failing since {health.et(since)}, "
            "the rest of the cycle still runs",
        )
        try:
            self.raise_alert(
                conn, finding.kind, finding.subject, finding.period, alert_text(finding)
            )
        except Exception:
            log.exception("could not raise the alert for the failed %s duty", step)

    # Messages in.

    def poll(self, conn: psycopg.Connection, now: datetime | None = None) -> None:
        """Read new messages and handle each once. Only the configured chat is ever answered.

        The offset starts from the newest update on record, so a restart does not ask again
        for what was handled before it.
        """
        now = now or self.clock()
        if self._offset is None:
            (self._offset,) = conn.execute(_OFFSET).fetchone()
        updates = self._updates()
        if updates is None:
            return
        for update in updates:
            self._offset = max(self._offset or 0, update["update_id"] + 1)
            reply = self.handle(conn, update, now)
            if reply is not None:
                self._send(reply)

    def handle(self, conn: psycopg.Connection, update: dict[str, Any], now: datetime) -> str | None:
        """Handle one update, at most once, and return the reply it is owed, if any.

        The update's `telegram_update` row, its `command` row and whatever the command did are
        one transaction, so either all of it is on record or none of it is. An update already on
        record is skipped: that is what makes a replay after a crash harmless. The reply goes
        out after the commit, because a sent message cannot be rolled back.
        """
        message = update.get("message")
        chat = message.get("chat", {}).get("id") if message is not None else None
        with conn.transaction():
            claimed = conn.execute(
                _CLAIM,
                {
                    "update_id": update["update_id"],
                    "chat_id": str(chat) if chat is not None else None,
                    "message_id": message.get("message_id") if message is not None else None,
                    "actor": ACTOR,
                },
            ).fetchone()
            if claimed is None:
                log.info("update %s was handled before; skipped", update["update_id"])
                return None
            if message is None:
                return None
            if str(chat) != self.chat_id:
                self.refuse(conn, message)
                return None
            return self.reply_to(conn, message, now)

    def _updates(self) -> list[dict[str, Any]] | None:
        """New messages, or `None` when Telegram could not be reached.

        Asking is idempotent: the offset advances only on what comes back, so the same ask
        either returns the same messages or returns nothing. That makes it safe to try again
        inside the cycle instead of leaving the messages to the next one, which is worth doing
        because a cycle that polls nothing answers nobody.
        """
        for attempt in range(self.poll_retries + 1):
            try:
                return self.telegram.get_updates(self._offset)
            except TelegramError as exc:
                if attempt == self.poll_retries:
                    log.warning("could not read messages: %s", exc)
                    return None
                log.warning("could not read messages: %s; trying again", exc)
                self._sleep(self.poll_backoff)
        return None

    def answer(self, conn: psycopg.Connection, message: dict[str, Any], now: datetime) -> None:
        """Record the command, then send its reply."""
        self._send(self.reply_to(conn, message, now))

    def _send(self, reply: str) -> None:
        try:
            for part in split(reply):
                self.telegram.send_message(self.chat_id, part)
        except TelegramError as exc:
            log.warning("could not send the reply %r: %s", reply.splitlines()[0], exc)

    def reply_to(self, conn: psycopg.Connection, message: dict[str, Any], now: datetime) -> str:
        """Record the command and return its reply: the command's readout, or why it was
        refused and what is available. A message older than `STALE_COMMAND` is refused
        whatever it says."""
        parsed = parse(message.get("text") or "")
        stale = self.stale(message, now)
        if stale is not None:
            self._command(conn, message, parsed.verb, {"args": parsed.args}, stale)
            what = f"/{parsed.verb}" if parsed.verb else "a message"
            log.warning("refused %s from %s: %s", what, operator(message), stale)
            return (
                f"STALE — {what} refused\n{stale}\n"
                "Nothing has been done. Send it again if you still mean it."
            )
        if parsed.refusal is not None:
            self._command(conn, message, parsed.verb, {"args": parsed.args}, parsed.refusal)
            return help_text(parsed.refusal)
        if parsed.verb == "pause":
            return self.pause(conn, message)
        if parsed.verb == "resume":
            return self.resume(conn, message)
        if parsed.verb == "liquidate":
            return self.liquidate(conn, message, parsed.args, now)
        if parsed.verb in DECISIONS:
            return self.decide(conn, message, parsed, now)
        self._command(conn, message, parsed.verb, {"args": parsed.args})
        return self.run(conn, parsed.verb, parsed.args, now)

    @staticmethod
    def stale(message: dict[str, Any], now: datetime) -> str | None:
        """Why a message is too old to act on, or `None` when it is fresh. A message without a
        send time is refused too, since its age cannot be known."""
        sent_at = message.get("date")
        if sent_at is None:
            return "stale: Telegram gave no time it was sent, so its age is unknown."
        sent = datetime.fromtimestamp(sent_at, UTC)
        if now - sent <= STALE_COMMAND:
            return None
        return (
            f"stale: sent {health.ago(now, sent)}, at {health.et(sent)}; "
            f"nothing older than {format_age(STALE_COMMAND)} is acted on."
        )

    def run(self, conn: psycopg.Connection, verb: str, args: list[str], now: datetime) -> str:
        if verb == "status":
            return readout.status(conn, now)
        if verb == "positions":
            return readout.positions(conn)
        if verb == "pnl":
            return readout.pnl(conn, args[0] if args else None)
        if verb == "pending":
            return approval.pending_text(conn, now, self.calendar(trading_date(now)))
        if verb == "basket":
            return readout.basket(conn)
        if verb == "gate":
            return readout.gate(conn)
        if verb == "level":
            return ladder.level_text(conn)
        if verb == "why":
            return readout.why(conn, args[0])
        if verb == "log":
            return readout.log(conn, int(args[0]) if args else readout.LOG_DEFAULT)
        return help_text()

    def source(self, message: dict[str, Any]) -> approval.Source:
        """Where a message came from, as the shared write paths take it: the Telegram user who
        sent it, this surface, and the text as typed, with the chat and message it arrived in so
        the `command` row points back at it."""
        chat = message.get("chat", {}).get("id")
        return approval.Source(
            operator(message),
            CHANNEL,
            message.get("text") or "",
            str(chat) if chat is not None else None,
            message.get("message_id"),
        )

    def pause(self, conn: psycopg.Connection, message: dict[str, Any]) -> str:
        return killswitch.pause(conn, self.source(message))

    def resume(self, conn: psycopg.Connection, message: dict[str, Any]) -> str:
        return killswitch.resume(conn, self.source(message))

    def liquidate(
        self, conn: psycopg.Connection, message: dict[str, Any], args: list[str], now: datetime
    ) -> str:
        """Bare, issue a token with what liquidating would mean; with a token, confirm and
        pause, or refuse. Returns the reply."""
        return killswitch.liquidate(conn, self.source(message), args[0] if args else None, now)

    def decide(
        self, conn: psycopg.Connection, message: dict[str, Any], parsed: Parsed, now: datetime
    ) -> str:
        """Approve or reject the pending rebalance the message names. Returns the reply."""
        assert parsed.verb is not None
        source = self.source(message)
        reply = approval.decide(
            conn, parsed.verb, parsed.args, source, now, self.calendar(trading_date(now))
        )
        log.info("/%s by %s: %s", parsed.verb, source.actor, reply.splitlines()[0])
        return reply

    def refuse(self, conn: psycopg.Connection, message: dict[str, Any]) -> None:
        """A message from any other chat is never answered, only recorded and logged."""
        chat = message.get("chat", {}).get("id")
        parsed = parse(message.get("text") or "")
        self._command(conn, message, parsed.verb, {"args": parsed.args}, "not the configured chat")
        log.warning(
            "refused a message from chat %s, user %s", chat, message.get("from", {}).get("id")
        )

    def _command(
        self,
        conn: psycopg.Connection,
        message: dict[str, Any],
        verb: str | None,
        args: dict[str, Any],
        refusal: str | None = None,
    ) -> int:
        chat = message.get("chat", {}).get("id")
        (command_id,) = conn.execute(
            _COMMAND,
            {
                "actor": operator(message),
                "text": message.get("text") or "",
                "verb": verb,
                "args": Jsonb(args),
                "outcome": "accepted" if refusal is None else "refused",
                "reason": refusal,
                "chat_id": str(chat) if chat is not None else None,
                "message_id": message.get("message_id"),
            },
        ).fetchone()
        return command_id

    # Alerts.

    def alerts(self, conn: psycopg.Connection, now: datetime) -> None:
        """An alert for each unhealthy finding, broker mismatch and performance reading."""
        for finding in [*self.findings(conn, now), *self.broker(conn), *self.performance(conn)]:
            self.raise_alert(
                conn, finding.kind, finding.subject, finding.period, alert_text(finding)
            )

    def findings(self, conn: psycopg.Connection, now: datetime) -> list[Finding]:
        return [
            *health.stale_heartbeats(conn, now, self.stale_after),
            *health.overdue(conn, now),
            *health.position_mismatch(conn),
            *health.panel_bodies(conn),
        ]

    def performance(self, conn: psycopg.Connection) -> list[Finding]:
        """What the account has done, against what the promoted certification said a world
        like this one would do. These are alerts and not pre-flight items: the pre-flight
        asks whether the machinery will work tomorrow, which is a different question from
        whether the strategy is behaving as it was validated to."""
        return [
            *health.drawdown(conn),
            *health.losing_run(conn),
            *health.tracking(conn),
            *health.live_cost(conn),
            *health.certified_capital(conn),
        ]

    def rebalances(self, conn: psycopg.Connection) -> None:
        """A fill confirmation for each recent rebalance that filled, and a gate-halt alert
        for each one the gate or the broker checks halted, once each."""
        for basket_id, reference, period, state in conn.execute(
            _SETTLED, (RECENT_BASKETS,)
        ).fetchall():
            kind = FILL_CONFIRMATION if state == "filled" else GATE_HALT
            if conn.execute(_RAISED, (kind, reference, period)).fetchone():
                continue
            if state == "filled":
                body = readout.filled_text(conn, basket_id, reference)
            else:
                body = readout.halted_text(conn, basket_id, reference)
            if body is not None:
                self.raise_alert(conn, kind, reference, period, body)

    def approvals(self, conn: psycopg.Connection, now: datetime) -> None:
        """Put each gated rebalance to the human, and raise the reminder due for each one
        still pending. One the cutoff has passed is left for the scheduler to mark missed.
        At L2 nothing is asked: the notification goes out for what the gate approved."""
        calendar = self.calendar(trading_date(now))
        for reference in approval.notify(conn, now, calendar):
            log.info("rebalance %s: proceeding at L2, notification raised", reference)
        for basket in approval.waiting(conn):
            if basket.state == "proposed":
                if now >= approval.cutoff_of(calendar, basket.session_date):
                    continue
                if approval.put(conn, basket, now, calendar):
                    log.info("rebalance %s: sent for approval", basket.reference)
            elif approval.remind(conn, basket, now, calendar):
                log.info("rebalance %s: reminder raised", basket.reference)

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
                # A long alert goes out in parts; the delivery names the last part sent.
                for part in split(body):
                    message_id = self.telegram.send_message(self.chat_id, part)
            except TelegramError as exc:
                self._record(conn, alert_id, "failed", detail=str(exc))
                log.warning("could not deliver alert %s (%s %s): %s", alert_id, kind, subject, exc)
                continue
            event_id = self._record(conn, alert_id, "delivered", message_id=message_id)
            if kind == approval.APPROVAL_REQUEST:
                # The request reaching the chat is what the month expects of the bot.
                jobs.fulfil(conn, schedule.APPROVAL_REQUEST, period, "alert_event", event_id, ACTOR)
            log.info("delivered alert %s (%s %s %s)", alert_id, kind, subject, period)

    def _record(
        self,
        conn: psycopg.Connection,
        alert_id: int,
        event: str,
        *,
        message_id: int | None = None,
        detail: str | None = None,
    ) -> int:
        (event_id,) = conn.execute(
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
        ).fetchone()
        return event_id


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
