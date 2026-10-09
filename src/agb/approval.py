"""The approval loop: putting a gated basket to the human, the reminders, the human's answer,
the miss at the cutoff, and the rerun the operator may ask for afterwards
(docs/architecture/04-risk-and-approval.md and 05-telegram-bot.md).

A live basket that passes the gate is left proposed, and so is a paper one while paper mode
is set to ask the human (`agb.paper_approval`). The bot puts it to the human: it moves the
basket to pending and raises the approval request in one transaction, then raises a
two-line reminder at T-60 and T-20 before the session's MOC cutoff. `/approve <reference>`
or `/reject <reference> [reason]` moves it on, and the unique `(basket_id, seq)` on
`basket_event` settles a race between surfaces: the first answer wins and the other is told
who answered and when. Every answer is a `command` row, refused ones included, and the event
it caused names it.

If nothing has answered by the cutoff the scheduler marks the basket missed, as the system,
and raises the expiry notice. A miss is not a failure: it fulfils the month's submission,
opens no halt cause, and the next month proceeds as usual. The operator may then ask for
the same basket at the next session's close with `agb rerun <reference>`: a new basket,
`2026-10r1`, whose `deviation_of` names the missed one, which keeps its outcome. The CLI
records the request and the pipeline builds and gates the basket.

Everything here reads its state from the database and holds none, so the UI can call the
same functions later.
"""

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import psycopg
from psycopg.errors import CheckViolation, UniqueViolation
from psycopg.types.json import Jsonb

from agb import blend, jobs, readout, schedule
from agb.health import et
from agb.nyse import NEW_YORK, NyseCalendar, trading_date

APPROVAL_REQUEST = schedule.APPROVAL_REQUEST
APPROVAL_REMINDER = "approval_reminder"
APPROVAL_EXPIRED = "approval_expired"
# The reminders go out this long before the cutoff, the earlier first.
REMINDERS = (timedelta(minutes=60), timedelta(minutes=20))
# Every basket asks the human first until the autonomy ladder exists.
LEVEL = "L0"
SYSTEM = "system"
SCHEDULER = "scheduler"
BOT = "bot"
RERUN = "rerun"
# A basket's rerun references: the period, then r and a number.
_RERUN = re.compile(r"^(\d{4}-\d{2})(?:r(\d+))?$")


class RerunRefused(Exception):
    """A rerun the CLI will not ask for, and why."""


@dataclass(frozen=True)
class Waiting:
    """A live basket waiting for the human: pending, or proposed and passed by the gate."""

    id: int
    reference: str
    period: str
    session_date: date
    state: str
    seq: int
    since: datetime


@dataclass(frozen=True)
class Source:
    """Where an answer came from: who typed it, on which surface, and the text as typed."""

    actor: str
    surface: str
    text: str
    chat_id: str | None = None
    message_id: int | None = None


_WAITING = """
    SELECT s.id, s.reference, s.period, s.session_date, s.state, s.seq, s.since
    FROM agb.basket_state s
    WHERE s.kind = 'live'
      AND (s.state = 'pending'
           OR (s.state = 'proposed'
               AND EXISTS (SELECT 1 FROM agb.gate_run r WHERE r.basket_id = s.id)))
    ORDER BY s.session_date, s.id
"""
_FIND = """
    SELECT id, reference, period, session_date, state, seq, since FROM agb.basket_state
    WHERE kind = 'live' AND reference = %s
"""
_DECIDED = """
    SELECT to_state, actor, at FROM agb.basket_event
    WHERE basket_id = %s AND to_state IN ('approved', 'rejected', 'missed')
    ORDER BY seq DESC
    LIMIT 1
"""
_MOVE = """
    INSERT INTO agb.basket_event
        (basket_id, seq, from_state, to_state, actor, surface, reason, command_id)
    VALUES (%(basket)s, %(seq)s, %(from)s, %(to)s, %(actor)s, %(surface)s, %(reason)s,
            %(command)s)
    RETURNING id
"""
_COMMAND = """
    INSERT INTO agb.command (actor, surface, raw_text, verb, args, outcome, reason, chat_id,
                             telegram_message_id)
    VALUES (%(actor)s, %(surface)s, %(text)s, %(verb)s, %(args)s, %(outcome)s, %(reason)s,
            %(chat_id)s, %(message_id)s)
    RETURNING id
"""
_RAISE = """
    INSERT INTO agb.alert (kind, subject, period, body, raised_by)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (kind, subject, period) DO NOTHING
    RETURNING id
"""


def waiting(conn: psycopg.Connection) -> list[Waiting]:
    return [Waiting(*row) for row in conn.execute(_WAITING).fetchall()]


def pending(conn: psycopg.Connection) -> list[Waiting]:
    return [w for w in waiting(conn) if w.state == "pending"]


def find(conn: psycopg.Connection, reference: str) -> Waiting | None:
    row = conn.execute(_FIND, (reference,)).fetchone()
    return Waiting(*row) if row else None


def record(
    conn: psycopg.Connection,
    source: Source,
    verb: str,
    args: dict[str, Any],
    refusal: str | None = None,
) -> int:
    """Record a command, accepted or refused, and return its id."""
    (command_id,) = conn.execute(
        _COMMAND,
        {
            "actor": source.actor,
            "surface": source.surface,
            "text": source.text,
            "verb": verb,
            "args": Jsonb(args),
            "outcome": "accepted" if refusal is None else "refused",
            "reason": refusal,
            "chat_id": source.chat_id,
            "message_id": source.message_id,
        },
    ).fetchone()
    return command_id


# Times.


def cutoff_of(calendar: NyseCalendar, session: date) -> datetime:
    """A session's MOC cutoff, from a calendar of its own if the one in hand does not cover
    it, as for a basket left waiting from long ago."""
    if not calendar.covers(session, session):
        calendar = NyseCalendar(session - timedelta(days=7), session + timedelta(days=7))
    return calendar.moc_cutoff(session)


def clock(at: datetime) -> str:
    return at.astimezone(NEW_YORK).strftime("%H:%M ET")


def deadline(cutoff: datetime, now: datetime) -> str:
    """The cutoff as the operator reads it: the time alone on the day, the date too before."""
    same_day = cutoff.astimezone(NEW_YORK).date() == now.astimezone(NEW_YORK).date()
    return clock(cutoff) if same_day else et(cutoff)


def countdown(now: datetime, cutoff: datetime) -> str:
    minutes = int((cutoff - now).total_seconds() // 60)
    if minutes < 0:
        return "passed"
    days, rem = divmod(minutes, 24 * 60)
    hours, minutes = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


# The approval request.

_REQUEST = """
    SELECT b.reference, b.session_date, b.capital_usd, r.mode, r.checks,
           (SELECT count(*) FROM agb.gate_check c WHERE c.run_id = r.id AND c.passed)
    FROM agb.basket b
    JOIN agb.gate_run r ON r.basket_id = b.id
    WHERE b.id = %s
"""


def request_text(
    conn: psycopg.Connection, basket_id: int, now: datetime, calendar: NyseCalendar
) -> str:
    """The approval request: the diff first, then the aggregate numbers, the flags, the gate
    and the prompt, which states what no answer means. A name the panel scored is bought with
    its safety score beside it, and whatever the panel flagged about the basket's names is
    listed under FLAGS."""
    reference, session, capital, mode, checks, passed = conn.execute(
        _REQUEST, (basket_id,)
    ).fetchone()
    cutoff = cutoff_of(calendar, session)
    by = deadline(cutoff, now)
    diff = readout.diff(conn, basket_id)
    width = diff.width
    out = [f"OUT ({len(diff.sold)})" if diff.sold else "OUT (none)"]
    for symbol, rank, was in diff.sold:
        line = f"  x {symbol:<{width}} {readout.rank_text(rank)}"
        out.append(line + (f" (was {was})" if was is not None else ""))
    scores = blend.safety(conn, basket_id)
    in_ = [f"IN ({len(diff.bought)})" if diff.bought else "IN (none)"]
    for symbol, rank in diff.bought:
        score = blend.safety_text(scores, symbol)
        line = f"  + {symbol:<{width}} {readout.rank_text(rank)}"
        in_.append(line + (f"  ·  {score}" if score else ""))
    numbers = (
        f"{readout.turnover_text(diff.measured.get('turnover'))}  ·  "
        f"{readout.cash_text(diff.measured.get('cash'), capital)}"
    )
    parts = [
        f"REBALANCE {reference} — approval needed\n"
        f"{mode.upper()} · {LEVEL} · deadline {by} (in {countdown(now, cutoff)})",
        "\n".join(out),
        "\n".join(in_),
        f"HOLD {diff.held} unchanged",
        numbers,
        flags_text(blend.flags(conn, basket_id)),
        f"GATE  {passed} of {checks} passed  /gate",
        f"/approve {reference}   /reject {reference}   /basket\n"
        f"No answer by {by} means no trades this month.",
    ]
    return "\n\n".join(parts)


def flags_text(flags: list[tuple[str, str]]) -> str:
    """What the panel flagged about the names this basket holds, one line each. Each is a
    fact about a filing or a rubric score, never a recommendation: the human decides."""
    if not flags:
        return "FLAGS (none)"
    width = max(len(symbol) + 1 for symbol, _ in flags)
    lines = [f"FLAGS ({len(flags)})"]
    lines += [f"  ! {symbol:<{width}} {flag}" for symbol, flag in flags]
    return "\n".join(lines)


def reminder_text(reference: str, now: datetime, cutoff: datetime) -> str:
    minutes = int((cutoff - now).total_seconds() // 60)
    return (
        f"REBALANCE {reference} — still waiting for your answer, {minutes}m left\n"
        f"No answer by {clock(cutoff)} means no trades this month.  /pending"
    )


def expiry_text(reference: str, cutoff: datetime) -> str:
    return (
        f"EXPIRED — rebalance {reference} had no answer by {et(cutoff)}.\n\n"
        "Marked missed by system. No trades this month: nothing was sent to the broker and "
        "no halt is set.\n\n"
        f"To trade it at the next session's close instead: agb rerun {reference}"
    )


def form(conn: psycopg.Connection) -> str:
    """The commands that answer what is waiting, for a refusal to re-print."""
    lines = [f"/approve {w.reference}   /reject {w.reference} [reason]" for w in pending(conn)]
    return "\n".join(lines) if lines else "Nothing is waiting for an answer.  /status"


def pending_text(conn: psycopg.Connection, now: datetime, calendar: NyseCalendar) -> str:
    """`/pending`: the approval request of every basket waiting for an answer, as it stands."""
    baskets = pending(conn)
    if not baskets:
        return "PENDING — nothing is waiting for an answer.  /status"
    return "\n\n\n".join(request_text(conn, w.id, now, calendar) for w in baskets)


# Putting a basket to the human, and reminding them.


def put(conn: psycopg.Connection, basket: Waiting, now: datetime, calendar: NyseCalendar) -> bool:
    """Move a gated basket from proposed to pending and raise its approval request, in one
    transaction. Returns False if something else moved the basket first."""
    body = request_text(conn, basket.id, now, calendar)
    try:
        with conn.transaction():
            conn.execute(
                _MOVE,
                {
                    "basket": basket.id,
                    "seq": basket.seq + 1,
                    "from": "proposed",
                    "to": "pending",
                    "actor": BOT,
                    "surface": "telegram",
                    "reason": "sent for approval",
                    "command": None,
                },
            )
            conn.execute(_RAISE, (APPROVAL_REQUEST, basket.reference, basket.period, body, BOT))
    except (UniqueViolation, CheckViolation):
        return False
    return True


def reminder(now: datetime, cutoff: datetime) -> timedelta | None:
    """The latest reminder due before the cutoff, if one is. A basket put to the human after
    a reminder's time still gets it, saying how long is really left."""
    if now >= cutoff:
        return None
    return min((before for before in REMINDERS if cutoff - before <= now), default=None)


def remind(
    conn: psycopg.Connection, basket: Waiting, now: datetime, calendar: NyseCalendar
) -> bool:
    """Raise the reminder due for a pending basket, once. Returns whether it raised one."""
    cutoff = cutoff_of(calendar, basket.session_date)
    before = reminder(now, cutoff)
    if before is None:
        return False
    subject = f"{basket.reference} T-{int(before.total_seconds() // 60)}"
    body = reminder_text(basket.reference, now, cutoff)
    raised = conn.execute(_RAISE, (APPROVAL_REMINDER, subject, basket.period, body, BOT)).fetchone()
    return raised is not None


# The human's answer.


def _already(conn: psycopg.Connection, basket: Waiting) -> str:
    decided = conn.execute(_DECIDED, (basket.id,)).fetchone()
    if decided is not None:
        state, actor, at = decided
        return f"rebalance {basket.reference}: already {state} by {actor} at {et(at)}"
    if basket.state == "proposed":
        return f"rebalance {basket.reference} is not yet waiting for an answer"
    if basket.state == "halted":
        return f"rebalance {basket.reference} was halted by the gate  /gate"
    return f"rebalance {basket.reference} is {basket.state}, not waiting for an answer"


def _refuse(
    conn: psycopg.Connection, source: Source, verb: str, args: dict[str, Any], why: str
) -> str:
    record(conn, source, verb, args, why)
    return f"{why}\n\n{form(conn)}"


def decide(
    conn: psycopg.Connection,
    verb: str,
    words: list[str],
    source: Source,
    now: datetime,
    calendar: NyseCalendar,
) -> str:
    """Answer `/approve <reference>` or `/reject <reference> [reason]` and return the reply.
    The reference must name the basket that is waiting; anything else is refused, recorded,
    and answered with the right form."""
    to_state = {"approve": "approved", "reject": "rejected"}[verb]
    args: dict[str, Any] = {"args": words}
    if not words:
        return _refuse(conn, source, verb, args, f"/{verb} needs the rebalance's reference")
    reference = words[0]
    reason = " ".join(words[1:]) or None
    if verb == "approve" and reason is not None:
        return _refuse(conn, source, verb, args, "usage: /approve <reference>, nothing after it")
    basket = find(conn, reference)
    if basket is None:
        return _refuse(conn, source, verb, args, f"no rebalance {reference}")
    if basket.state != "pending":
        return _refuse(conn, source, verb, args, _already(conn, basket))
    cutoff = cutoff_of(calendar, basket.session_date)
    if now >= cutoff:
        why = f"rebalance {reference}: the deadline, {et(cutoff)}, has passed"
        return _refuse(conn, source, verb, args, why)

    accepted = {"reference": reference, "basket_id": basket.id}
    if reason is not None:
        accepted["reason"] = reason
    try:
        with conn.transaction():
            command_id = record(conn, source, verb, accepted)
            conn.execute(
                _MOVE,
                {
                    "basket": basket.id,
                    "seq": basket.seq + 1,
                    "from": "pending",
                    "to": to_state,
                    "actor": source.actor,
                    "surface": source.surface,
                    "reason": reason or f"/{verb} {reference}",
                    "command": command_id,
                },
            )
    except (UniqueViolation, CheckViolation):
        # Another surface answered first; the compare-and-set kept its answer.
        return _refuse(conn, source, verb, args, _already(conn, find(conn, reference) or basket))
    if to_state == "approved":
        return (
            f"APPROVED {reference} by {source.actor} at {et(now)}\n"
            f"Execution submits it before the {clock(cutoff)} cutoff.  /status"
        )
    return (
        f"REJECTED {reference} by {source.actor} at {et(now)}\n"
        f"No trades this month.{f' reason: {reason}' if reason else ''}"
    )


# The miss at the cutoff.


def sweep(conn: psycopg.Connection, now: datetime, calendar: NyseCalendar) -> list[str]:
    """Mark missed, as the system, every basket still waiting for the human at its cutoff,
    raise the expiry notice and fulfil the month's submission. No halt cause opens. Returns
    the references it marked."""
    missed = []
    for basket in waiting(conn):
        cutoff = cutoff_of(calendar, basket.session_date)
        if now < cutoff:
            continue
        try:
            with conn.transaction():
                (event_id,) = conn.execute(
                    _MOVE,
                    {
                        "basket": basket.id,
                        "seq": basket.seq + 1,
                        "from": basket.state,
                        "to": "missed",
                        "actor": SYSTEM,
                        "surface": SCHEDULER,
                        "reason": f"no answer by the cutoff, {et(cutoff)}",
                        "command": None,
                    },
                ).fetchone()
                conn.execute(
                    _RAISE,
                    (
                        APPROVAL_EXPIRED,
                        basket.reference,
                        basket.period,
                        expiry_text(basket.reference, cutoff),
                        SCHEDULER,
                    ),
                )
                jobs.fulfil(
                    conn, schedule.SUBMISSION, basket.period, "basket_event", event_id, SCHEDULER
                )
        except (UniqueViolation, CheckViolation):
            continue
        missed.append(basket.reference)
    return missed


# The rerun.

_RERUNS = """
    SELECT reference FROM agb.basket WHERE period = %(period)s AND kind = 'live'
    UNION ALL
    SELECT reference FROM agb.rerun_pending WHERE reference LIKE %(period)s || '%%'
"""
_RERUN_OF = """
    SELECT reference FROM agb.basket WHERE deviation_of = %(id)s
    UNION ALL
    SELECT reference FROM agb.rerun_pending WHERE basket_id = %(id)s
"""


def next_reference(conn: psycopg.Connection, period: str) -> str:
    taken = [
        int(match.group(2) or 0)
        for (reference,) in conn.execute(_RERUNS, {"period": period}).fetchall()
        if (match := _RERUN.match(reference)) and match.group(1) == period
    ]
    return f"{period}r{max(taken, default=0) + 1}"


def rerun_session(original: date, now: datetime, calendar: NyseCalendar) -> date:
    """The first session after the missed one whose cutoff has not passed."""
    session = calendar.next_session(max(original, trading_date(now) - timedelta(days=1)))
    while calendar.moc_cutoff(session) <= now:
        session = calendar.next_session(session)
    return session


def request_rerun(
    conn: psycopg.Connection,
    reference: str,
    source: Source,
    now: datetime,
    calendar: NyseCalendar,
) -> str:
    """Ask for a missed basket to trade again at the next session's close. The request is a
    `rerun` command, which the pipeline builds the new basket from; a refusal is recorded
    too, and raised as RerunRefused."""
    args: dict[str, Any] = {"reference": reference}
    basket = find(conn, reference)
    if basket is None:
        why = f"no rebalance {reference}"
    elif basket.state != "missed":
        why = f"rebalance {reference} is {basket.state}; only a missed rebalance is rerun"
    elif taken := conn.execute(_RERUN_OF, {"id": basket.id}).fetchone():
        why = f"rebalance {reference} is already rerun as {taken[0]}"
    else:
        why = None
    if why is not None or basket is None:
        record(conn, source, RERUN, args, why)
        raise RerunRefused(why)
    new = next_reference(conn, basket.period)
    session = rerun_session(basket.session_date, now, calendar)
    cutoff = cutoff_of(calendar, session)
    args |= {"basket_id": basket.id, "rerun": new, "session_date": session.isoformat()}
    record(conn, source, RERUN, args)
    return (
        f"rerun {new} asked for: rebalance {reference} again, at the close of {session}.\n"
        "The pipeline builds and gates it, and the approval request follows in Telegram; "
        f"no answer by {et(cutoff)} means it is missed too."
    )


# Whether paper mode asks the human.

_PAPER_APPROVAL = "SELECT required, since, actor, reason FROM agb.paper_approval"
_SET_PAPER_APPROVAL = """
    INSERT INTO agb.paper_approval_event (required, actor, reason) VALUES (%s, %s, %s)
"""


def paper_approval_text(conn: psycopg.Connection) -> str:
    required, since, actor, reason = conn.execute(_PAPER_APPROVAL).fetchone()
    asks = "asks the human first" if required else "runs unattended"
    why = f": {reason}" if reason else ""
    return f"paper mode {asks}, since {et(since)}, set by {actor}{why}"


def set_paper_approval(
    conn: psycopg.Connection, required: bool, actor: str, reason: str | None = None
) -> str:
    """Set whether a paper basket that passes the gate waits for the human, from the next
    gate run on. Returns the setting as it now stands."""
    conn.execute(_SET_PAPER_APPROVAL, (required, actor, reason or None))
    return paper_approval_text(conn)
