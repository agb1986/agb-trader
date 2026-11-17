"""Acknowledging an alert: the operator saying they have seen it
(docs/architecture/05-telegram-bot.md and 06-local-ui.md).

`/ack ALERT_ID` in Telegram and the Acknowledge form on the UI's Alerts page both call
`acknowledge`, so the two surfaces do the same thing and give the same reply. An
acknowledgement is one `alert_event` row, event 'acknowledged', that names the operator who
gave it, the surface and, for Telegram, the chat. Every attempt is also an `agb.command` row,
refused ones included, as it is for the other writes.

An alert is acknowledged once. The first acknowledgement stands: a second one, from either
surface or from a race between the two, writes nothing and is refused with a reply that says who
acknowledged it and when. The unique index `alert_event_acknowledged_once` settles the race in
the database, and the same migration binds the actor to the role that writes the row.

Everything here reads its state from the database and holds none.
"""

from datetime import datetime

import psycopg

from agb import approval
from agb.health import et
from agb.rows import one

VERB = "ack"
# The largest value of the `bigint` an alert id is.
MAX_ID = 2**63 - 1
USAGE = "Usage: /ack <alert id>, the number the alert is listed under."

_ALERT = "SELECT kind, subject, period FROM agb.alert WHERE id = %s"
# Inserting nothing means the alert already has its acknowledgement, which the unique index
# allows once.
_ACKNOWLEDGE = """
    INSERT INTO agb.alert_event (alert_id, event, channel, chat_id, actor, at)
    VALUES (%(alert)s, 'acknowledged', %(channel)s, %(chat_id)s, %(actor)s, %(at)s)
    ON CONFLICT (alert_id) WHERE event = 'acknowledged' DO NOTHING
"""
_ACKNOWLEDGED = """
    SELECT actor, channel, at FROM agb.alert_event
    WHERE alert_id = %s AND event = 'acknowledged'
"""


def alert_id(word: str | None) -> int | None:
    """The alert id as typed, or None when it is not a whole number. Digits only, so a sign,
    a decimal point or a thousands separator is not an id, and neither is a number too large
    for the column."""
    if word is None or not word.isascii() or not word.isdigit():
        return None
    ident = int(word)
    return ident if ident <= MAX_ID else None


def acknowledge(
    conn: psycopg.Connection, source: approval.Source, word: str | None, now: datetime
) -> str:
    """Acknowledge the alert whose id was typed, and return the reply to show.

    The outcomes, each recorded as a command: acknowledged; refused because the id is not a
    number; refused because no alert has it; refused because it was already acknowledged. Only
    the first writes an `alert_event`."""
    ident = alert_id(word)
    if ident is None:
        refusal = f"{USAGE} {word!r} is not an alert id." if word else USAGE
        approval.record(conn, source, VERB, {"alert": word}, refusal)
        return refusal
    args = {"alert": ident}
    with conn.transaction():
        found = conn.execute(_ALERT, (ident,)).fetchone()
        if found is None:
            refusal = f"There is no alert {ident}; nothing was acknowledged."
            approval.record(conn, source, VERB, args, refusal)
            return refusal
        kind, subject, period = found
        written = conn.execute(
            _ACKNOWLEDGE,
            {
                "alert": ident,
                "channel": source.surface,
                "chat_id": source.chat_id,
                "actor": source.actor,
                "at": now,
            },
        )
        if written.rowcount == 0:
            actor, channel, at = one(conn.execute(_ACKNOWLEDGED, (ident,)))
            refusal = (
                f"Alert {ident} ({kind}, {subject} {period}) was already acknowledged by "
                f"{actor} from {channel} at {et(at)}. Nothing changed."
            )
            approval.record(conn, source, VERB, args, refusal)
            return refusal
        approval.record(conn, source, VERB, args)
    return f"Alert {ident} ({kind}, {subject} {period}) acknowledged by {source.actor}."
