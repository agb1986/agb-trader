"""The kill switch: pause, resume and the two-step liquidation, written once for every surface
that can throw it (docs/architecture/04-risk-and-approval.md and 06-local-ui.md).

Telegram and the local UI are two views onto one database, so the switch itself cannot live in
either service: a second copy of it would be a second set of semantics, and the one thing an
operator must be able to trust under pressure is that the lever behaves the same whichever hand
is on it. The functions here take the command's source — who threw the switch, from which
surface, and the text they typed — and return the reply, which both surfaces show verbatim.

`pause` opens the `pause` halt cause unless it is open already; `resume` clears every open
cause, one clear each, and says what it cleared, or clears only the causes it is told to when a
surface lets the operator choose (the local UI does; the bot's `/resume` takes none and clears
all). Neither touches the broker: `suspend_trade` is
execution's to set, since only execution holds the broker's keys, and its confirmation arrives
as an alert.

`liquidate` is two steps. Without a token it says what closing every position would mean and
issues a short random one, valid for five minutes. With the token typed back in time it opens
the `liquidation` halt cause in the same transaction as the confirmation, so the system is
paused before execution calls the broker's close-all. A wrong or expired token is refused,
spends the token, and is not retried. Every step is a `command` row, refused ones included.
"""

import logging
import secrets
import string
from collections.abc import Collection
from datetime import datetime, timedelta
from typing import Any

import psycopg

from agb import approval, health, readout

log = logging.getLogger(__name__)

# How long a liquidation token stays valid, and what one is made of.
TOKEN_VALID = timedelta(minutes=5)
TOKEN_LENGTH = 6
TOKEN_ALPHABET = string.ascii_lowercase + string.digits

_PAUSE_OPEN = "SELECT at, actor FROM agb.halt_open WHERE cause = 'pause'"
_LIQUIDATION_OPEN = "SELECT 1 FROM agb.halt_open WHERE cause = 'liquidation'"
_OPEN_CAUSES = "SELECT cause FROM agb.halt_open ORDER BY at, cause"
_CAUSES = "SELECT cause, at, actor, reason FROM agb.halt_open ORDER BY at, cause"
_HALT = """
    INSERT INTO agb.halt_event (cause, action, actor, reason) VALUES (%s, %s, %s, %s)
"""

# The latest token no confirmation or refusal has spent yet. Only the latest is ever valid: a
# fresh liquidation request supersedes any earlier one.
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


def causes(conn: psycopg.Connection) -> list[tuple[str, datetime, str, str | None]]:
    """Every open halt cause, oldest first, as a surface shows the latch before touching it."""
    return conn.execute(_CAUSES).fetchall()


def outstanding(conn: psycopg.Connection, now: datetime) -> datetime | None:
    """When the live liquidation token expires, or None if none is live. The token itself is not
    returned: a surface that re-displayed it would hand the second step to whoever reloaded the
    page, and the second step exists to be typed."""
    row = conn.execute(_TOKEN).fetchone()
    return row[2] if row is not None and now < row[2] else None


def pause(conn: psycopg.Connection, source: approval.Source) -> str:
    """Open the `pause` cause, unless it is open already, and ask execution to set the broker's
    `suspend_trade`. Returns the reply."""
    with conn.transaction():
        already = conn.execute(_PAUSE_OPEN).fetchone()
        if already is None:
            conn.execute(_HALT, ("pause", "set", source.actor, "/pause"))
        approval.record(conn, source, "pause", {"already_open": already is not None})
    if already is None:
        opened = "halt cause pause: open. No further order goes out."
    else:
        opened = f"halt cause pause: already open since {health.et(already[0])}, by {already[1]}."
    log.info("pause by %s", source.actor)
    return (
        f"PAUSED\n{opened}\nbroker suspend_trade: asked on. Execution confirms it here, "
        "with anything it cancelled."
    )


def resume(
    conn: psycopg.Connection, source: approval.Source, only: Collection[str] | None = None
) -> str:
    """Clear the open halt causes, one clear each, and ask execution to unset the broker's
    `suspend_trade`. Returns the reply, which lists what was cleared.

    Without `only` every open cause is cleared. With it, only the open causes it names are: a
    named cause that is not open any more is left out and said so, and an empty `only` is
    refused and recorded, since a resume that names nothing to clear is not one. The broker's
    `suspend_trade` stays on while the `pause` cause is."""
    with conn.transaction():
        open_causes = [cause for (cause,) in conn.execute(_OPEN_CAUSES).fetchall()]
        if only is None:
            cleared, gone = open_causes, []
        else:
            cleared = [cause for cause in open_causes if cause in only]
            gone = sorted(set(only) - set(open_causes))
        if only is not None and not only:
            refusal = "no cause was chosen to clear"
            approval.record(conn, source, "resume", {"cleared": []}, refusal)
        else:
            refusal = None
            for cause in cleared:
                conn.execute(_HALT, (cause, "clear", source.actor, "/resume"))
            args: dict[str, Any] = {"cleared": cleared}
            if only is not None:
                args["left_open"] = [cause for cause in open_causes if cause not in cleared]
            approval.record(conn, source, "resume", args)
    if refusal is not None:
        log.warning("resume refused for %s: %s", source.actor, refusal)
        return f"RESUME refused\n{refusal}\nNothing has been done."
    log.info("resume by %s, cleared: %s", source.actor, ", ".join(cleared) or "nothing")
    what = f"cleared: {', '.join(cleared)}" if cleared else "cleared: nothing was open"
    lines = ["RESUMED", what]
    if gone:
        lines.append(f"not open any more, so not cleared: {', '.join(gone)}")
    left = [cause for cause in open_causes if cause not in cleared]
    if only is not None and left:
        lines.append(f"still open: {', '.join(left)}")
    if "pause" in left:
        lines.append("broker suspend_trade: stays on while pause is open.")
    else:
        lines.append("broker suspend_trade: asked off. Execution confirms it here.")
    lines.append("A halted rebalance is not resubmitted.")
    return "\n".join(lines)


def liquidate(
    conn: psycopg.Connection, source: approval.Source, typed: str | None, now: datetime
) -> str:
    """Without a token, issue one with what liquidating would mean; with a token, confirm and
    pause, or refuse. Returns the reply."""
    if typed:
        return confirm(conn, source, typed, now)
    return issue(conn, source, now)


def issue(conn: psycopg.Connection, source: approval.Source, now: datetime) -> str:
    """Step one: what closing every position would mean, and the token that goes with it."""
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
        command_id = approval.record(conn, source, "liquidate", {"step": "token"})
        conn.execute(
            _LIQUIDATION,
            {
                "kind": "token",
                "follows": None,
                "command_id": command_id,
                "token": issued,
                "expires_at": expires_at,
                "detail": summary,
                "actor": source.actor,
            },
        )
    log.info("liquidation token issued to %s", source.actor)
    minutes = int(TOKEN_VALID.total_seconds() // 60)
    return (
        f"LIQUIDATE — step 1 of 2\n{summary}\n\n"
        f"To go ahead, send this within {minutes} minutes, by {health.et(expires_at)}:\n"
        f"/liquidate {issued}\n"
        "A wrong or late token is refused, and you start again from /liquidate."
    )


def confirm(conn: psycopg.Connection, source: approval.Source, typed: str, now: datetime) -> str:
    """Step two: the token typed back. In time and right, the `liquidation` cause opens in the
    same transaction as the confirmation; anything else spends the token and is refused."""
    with conn.transaction():
        token_row = conn.execute(_TOKEN).fetchone()
        if token_row is None:
            refusal = "no liquidation token is outstanding: send /liquidate first"
        elif now >= token_row[2]:
            refusal = (
                f"the liquidation token expired at {health.et(token_row[2])}; it is spent, "
                "send /liquidate to start again"
            )
        # Bytes, not str: secrets.compare_digest refuses a str with non-ASCII characters,
        # and what the operator typed is whatever they typed.
        elif not secrets.compare_digest(typed.lower().encode(), token_row[1].encode()):
            refusal = "wrong liquidation token; it is spent, send /liquidate to start again"
        else:
            refusal = None
        args = {"step": "confirm" if refusal is None else "refused"}
        if token_row is not None:
            args["token_row"] = token_row[0]
        command_id = approval.record(conn, source, "liquidate", args, refusal)
        if token_row is not None:
            if refusal is None:
                if conn.execute(_LIQUIDATION_OPEN).fetchone() is None:
                    conn.execute(_HALT, ("liquidation", "set", source.actor, "/liquidate"))
            conn.execute(
                _LIQUIDATION,
                {
                    "kind": "confirm" if refusal is None else "refused",
                    "follows": token_row[0],
                    "command_id": command_id,
                    "token": None,
                    "expires_at": None,
                    "detail": refusal or "token confirmed in time; the system is paused",
                    "actor": source.actor,
                },
            )
    if refusal is not None:
        log.warning("liquidation refused for %s: %s", source.actor, refusal)
        return f"LIQUIDATE refused\n{refusal}\nNothing has been done."
    log.info("liquidation confirmed by %s", source.actor)
    return (
        "LIQUIDATING — step 2 of 2\n"
        "halt cause liquidation: open. No further order goes out.\n"
        "Execution now cancels open orders and closes every position at market. The "
        "broker's answer arrives here symbol by symbol, failures included.\n"
        "The liquidation cause stays open until /resume."
    )
