"""The Telegram bot service. Each cycle it beats its heartbeat, reads new messages and
answers the read-only commands, raises an alert for each unhealthy finding, a fill
confirmation for each rebalance that filled and a gate-halt alert for each one the checks
halted, puts each gated rebalance to the human and reminds them before its cutoff, raises the
pre-flight on the session before a rebalance, and delivers every undelivered alert to the
configured chat.

It holds no state beyond the Telegram update offset. Every message it sends is computed from
the database, every message it receives is a `command` row whatever became of it, and every
delivery attempt is an `alert_event`. Alerts notify and never act.

`/pause` opens the `pause` halt cause and `/resume` clears every open cause, one clear each;
both are written here, so the database latch holds the moment the reply goes out. The
broker's `suspend_trade` is execution's to set, since only execution holds the broker's keys:
it picks up the accepted command, and its confirmation arrives in this chat as an alert.
`/approve` and `/reject` answer the approval request, through `agb.approval`, which records
the command whatever became of it.

`/liquidate` is two steps. Bare, it says what closing every position would mean and issues a
short random token, valid for five minutes. With the token typed back in time, it opens the
`liquidation` halt cause in the same transaction as the confirmation, so the system is paused
before execution calls the broker's close-all; the per-symbol result arrives as an alert. A
wrong or expired token is refused, spends the token, and is not retried. The other commands
only read.
"""

import logging
import os
import re
import secrets
import string
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from agb import approval, health, jobs, ladder, readout, schedule
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
FILL_CONFIRMATION = "fill_confirmation"
GATE_HALT = "gate_halt"
# The unprompted rebalance messages look back over this many of the latest live baskets.
RECENT_BASKETS = 3
# Telegram refuses a message longer than this; a longer answer goes out in parts.
MESSAGE_MAX = 4096
# How long a liquidation token stays valid, and what one is made of.
TOKEN_VALID = timedelta(minutes=5)
TOKEN_LENGTH = 6
TOKEN_ALPHABET = string.ascii_lowercase + string.digits

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
    return f"{ALERT_HEADER}\n\n{finding.lines()}"


def preflight_text(period: str, findings: list[Finding]) -> str:
    if not findings:
        return f"PRE-FLIGHT {period}\nrebalance tomorrow, all services healthy"
    count = f"{len(findings)} thing{'' if len(findings) == 1 else 's'} not healthy"
    items = "\n\n".join(finding.lines() for finding in findings)
    return f"PRE-FLIGHT {period}\nrebalance tomorrow, {count}\n\n{items}"


_PAUSE_OPEN = "SELECT at, actor FROM agb.halt_open WHERE cause = 'pause'"
_LIQUIDATION_OPEN = "SELECT 1 FROM agb.halt_open WHERE cause = 'liquidation'"
_OPEN_CAUSES = "SELECT cause FROM agb.halt_open ORDER BY at, cause"
_HALT = """
    INSERT INTO agb.halt_event (cause, action, actor, reason) VALUES (%s, %s, %s, %s)
"""


# The latest token no confirmation or refusal has spent yet. Only the latest is ever valid: a
# fresh `/liquidate` supersedes any earlier one.
_TOKEN = """
    SELECT t.id, t.token, t.expires_at FROM agb.liquidation t
    WHERE t.kind = 'token'
      AND t.id = (SELECT max(id) FROM agb.liquidation WHERE kind = 'token')
      AND NOT EXISTS (SELECT 1 FROM agb.liquidation f WHERE f.follows = t.id)
"""
_LIQUIDATION = """
    INSERT INTO agb.liquidation (kind, follows, command_id, token, expires_at, detail, actor)
    VALUES (%(kind)s, %(follows)s, %(command_id)s, %(token)s, %(expires_at)s, %(detail)s,
            %(actor)s)
    RETURNING id
"""
# What liquidating would close: the latest positions snapshot, and the account's equity.
_HELD = """
    SELECT s.id, s.snapshot_date, count(p.id), coalesce(sum(p.market_value_usd), 0)
    FROM agb.position_snapshot s
    LEFT JOIN agb.position p ON p.snapshot_id = s.id
    WHERE s.id = (SELECT max(id) FROM agb.position_snapshot)
    GROUP BY s.id
"""
_EQUITY = "SELECT equity_usd, observed_at FROM agb.broker_state_now"


def token() -> str:
    return "".join(secrets.choice(TOKEN_ALPHABET) for _ in range(TOKEN_LENGTH))


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
        self.poll(conn, now)
        for finding in [*self.findings(conn, now), *self.broker(conn)]:
            self.raise_alert(
                conn, finding.kind, finding.subject, finding.period, alert_text(finding)
            )
        self.rebalances(conn)
        self.approvals(conn, now)
        self.preflight(conn, now)
        self.deliver(conn)

    # Messages in.

    def poll(self, conn: psycopg.Connection, now: datetime | None = None) -> None:
        """Read new messages and record each as a command. Only the configured chat is ever
        answered."""
        now = now or self.clock()
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
                self.answer(conn, message, now)
            else:
                self.refuse(conn, message)

    def answer(self, conn: psycopg.Connection, message: dict[str, Any], now: datetime) -> None:
        """Record the command, then answer it: the command's readout, or why it was refused
        and what is available."""
        parsed = parse(message.get("text") or "")
        if parsed.refusal is not None:
            self._command(conn, message, parsed.verb, {"args": parsed.args}, parsed.refusal)
            reply = help_text(parsed.refusal)
        elif parsed.verb == "pause":
            reply = self.pause(conn, message)
        elif parsed.verb == "resume":
            reply = self.resume(conn, message)
        elif parsed.verb == "liquidate":
            reply = self.liquidate(conn, message, parsed.args, now)
        elif parsed.verb in DECISIONS:
            reply = self.decide(conn, message, parsed, now)
        else:
            self._command(conn, message, parsed.verb, {"args": parsed.args})
            reply = self.run(conn, parsed.verb, parsed.args, now)
        try:
            for part in split(reply):
                self.telegram.send_message(self.chat_id, part)
        except TelegramError as exc:
            log.warning("could not answer /%s: %s", parsed.verb, exc)

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

    def pause(self, conn: psycopg.Connection, message: dict[str, Any]) -> str:
        """Open the `pause` cause, unless it is open already, and ask execution to set the
        broker's `suspend_trade`. Returns the reply."""
        actor = operator(message)
        with conn.transaction():
            already = conn.execute(_PAUSE_OPEN).fetchone()
            if already is None:
                conn.execute(_HALT, ("pause", "set", actor, "/pause"))
            self._command(conn, message, "pause", {"already_open": already is not None})
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

    def resume(self, conn: psycopg.Connection, message: dict[str, Any]) -> str:
        """Clear every open halt cause, one clear each, and ask execution to unset the
        broker's `suspend_trade`. Returns the reply, which lists what was cleared."""
        actor = operator(message)
        with conn.transaction():
            causes = [cause for (cause,) in conn.execute(_OPEN_CAUSES).fetchall()]
            for cause in causes:
                conn.execute(_HALT, (cause, "clear", actor, "/resume"))
            self._command(conn, message, "resume", {"cleared": causes})
        log.info("resume by %s, cleared: %s", actor, ", ".join(causes) or "nothing")
        cleared = f"cleared: {', '.join(causes)}" if causes else "cleared: nothing was open"
        return (
            f"RESUMED\n{cleared}\nbroker suspend_trade: asked off. Execution confirms it "
            "here.\nA halted rebalance is not resubmitted."
        )

    def liquidate(
        self, conn: psycopg.Connection, message: dict[str, Any], args: list[str], now: datetime
    ) -> str:
        """Bare, issue a token with what liquidating would mean; with a token, confirm and
        pause, or refuse. Returns the reply."""
        if args:
            return self._confirm(conn, message, args[0], now)
        return self._issue(conn, message, now)

    def _issue(self, conn: psycopg.Connection, message: dict[str, Any], now: datetime) -> str:
        actor = operator(message)
        snapshot = conn.execute(_HELD).fetchone()
        if snapshot is None:
            held = "positions: no snapshot yet, so the count is unknown"
        else:
            _, snapshot_date, count, value = snapshot
            held = (
                f"positions: {count} held, worth {readout.dollars(value)} at the "
                f"{snapshot_date} snapshot"
            )
        account = conn.execute(_EQUITY).fetchone()
        if account is None or account[0] is None:
            equity = "equity: not read from the broker yet"
        else:
            equity = f"equity: about {readout.dollars(account[0])}, read {health.et(account[1])}"
        expires_at = now + TOKEN_VALID
        summary = (
            f"{held}\n{equity}\n"
            "Open orders are cancelled first, then every position is closed at market.\n"
            "This cannot be undone.\n"
            "The system will not re-enter these names until the next scheduled rebalance."
        )
        issued = token()
        with conn.transaction():
            command_id = self._command(conn, message, "liquidate", {"step": "token"})
            conn.execute(
                _LIQUIDATION,
                {
                    "kind": "token",
                    "follows": None,
                    "command_id": command_id,
                    "token": issued,
                    "expires_at": expires_at,
                    "detail": summary,
                    "actor": actor,
                },
            )
        log.info("liquidation token issued to %s", actor)
        minutes = int(TOKEN_VALID.total_seconds() // 60)
        return (
            f"LIQUIDATE — step 1 of 2\n{summary}\n\n"
            f"To go ahead, send this within {minutes} minutes, by {health.et(expires_at)}:\n"
            f"/liquidate {issued}\n"
            "A wrong or late token is refused, and you start again from /liquidate."
        )

    def _confirm(
        self, conn: psycopg.Connection, message: dict[str, Any], typed: str, now: datetime
    ) -> str:
        actor = operator(message)
        with conn.transaction():
            outstanding = conn.execute(_TOKEN).fetchone()
            if outstanding is None:
                refusal = "no liquidation token is outstanding: send /liquidate first"
            elif now >= outstanding[2]:
                refusal = (
                    f"the liquidation token expired at {health.et(outstanding[2])}; it is spent, "
                    "send /liquidate to start again"
                )
            # Bytes, not str: secrets.compare_digest refuses a str with non-ASCII characters,
            # and what the operator typed is whatever they typed.
            elif not secrets.compare_digest(typed.lower().encode(), outstanding[1].encode()):
                refusal = "wrong liquidation token; it is spent, send /liquidate to start again"
            else:
                refusal = None
            args = {"step": "confirm" if refusal is None else "refused"}
            if outstanding is not None:
                args["token_row"] = outstanding[0]
            command_id = self._command(conn, message, "liquidate", args, refusal)
            if outstanding is not None:
                if refusal is None:
                    if conn.execute(_LIQUIDATION_OPEN).fetchone() is None:
                        conn.execute(_HALT, ("liquidation", "set", actor, "/liquidate"))
                conn.execute(
                    _LIQUIDATION,
                    {
                        "kind": "confirm" if refusal is None else "refused",
                        "follows": outstanding[0],
                        "command_id": command_id,
                        "token": None,
                        "expires_at": None,
                        "detail": refusal or "token confirmed in time; the system is paused",
                        "actor": actor,
                    },
                )
        if refusal is not None:
            log.warning("liquidation refused for %s: %s", actor, refusal)
            return f"LIQUIDATE refused\n{refusal}\nNothing has been done."
        log.info("liquidation confirmed by %s", actor)
        return (
            "LIQUIDATING — step 2 of 2\n"
            "halt cause liquidation: open. No further order goes out.\n"
            "Execution now cancels open orders and closes every position at market. The "
            "broker's answer arrives here symbol by symbol, failures included.\n"
            "The liquidation cause stays open until /resume."
        )

    def decide(
        self, conn: psycopg.Connection, message: dict[str, Any], parsed: Parsed, now: datetime
    ) -> str:
        """Approve or reject the pending rebalance the message names. Returns the reply."""
        assert parsed.verb is not None
        chat = message.get("chat", {}).get("id")
        source = approval.Source(
            operator(message),
            CHANNEL,
            message.get("text") or "",
            str(chat) if chat is not None else None,
            message.get("message_id"),
        )
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

    def findings(self, conn: psycopg.Connection, now: datetime) -> list[Finding]:
        return [
            *health.stale_heartbeats(conn, now, self.stale_after),
            *health.overdue(conn, now),
            *health.position_mismatch(conn),
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
