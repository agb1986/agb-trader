"""The autonomy ladder and the trading mode (docs/architecture/04-risk-and-approval.md).

Three levels. At L0 every live rebalance asks the human and waits. At L1 a rebalance asks
only when the panel flagged a name or the turnover is unusual; a clean basket proceeds after
a countdown unless it is rejected inside the window. At L2 the approval message is a
notification with no prompt. The pre-trade gate and the kill-switch are not part of the
ladder: they apply at every level, permanently.

Loosening is manual, tightening is not. An up-move happens only through `agb level` or
`agb mode`, is refused unless the eligibility view for the level above is satisfied, and
carries the human's attestation. A rejection or a gate breach at L1 or L2 drops the system
back to L0 by itself, recorded as a `ladder_event` with actor `system`. Eligibility is a set
of views over the recorded history, never a stored counter, so nothing here keeps a count.

`/level` shows the level and the progress towards the next one and offers no way to move up.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg

from agb import approval

LEVELS = ("L0", "L1", "L2")
MODES = ("paper", "live")
LEVEL = "level"
MODE = "mode"
SYSTEM = "system"
# What each level does, for `/level` and the CLI.
MEANING = {
    "L0": "every live rebalance asks you and waits for your answer",
    "L1": "a flagged or unusually large rebalance asks you; a clean one proceeds after the"
    " countdown unless you reject it",
    "L2": "a rebalance proceeds and tells you; there is no prompt",
}


class Refused(Exception):
    """A move the ladder will not make, and why. Recorded as a refused command."""


@dataclass(frozen=True)
class Level:
    """The level the system is at, and the event that set it."""

    level: str
    since: datetime
    actor: str
    reason: str
    attestation: str | None


@dataclass(frozen=True)
class Criterion:
    """One eligibility criterion as its view measures it."""

    seq: int
    name: str
    measured: str
    threshold: str
    met: bool

    def line(self) -> str:
        mark = "met" if self.met else "unmet"
        return f"{self.name}: {self.measured} (needs {self.threshold}) — {mark}"


@dataclass(frozen=True)
class Readiness:
    """Whether a target is eligible, and the criteria that are not yet met."""

    target: str
    ready: bool
    outstanding: list[str]


_LEVEL = "SELECT level, since, actor, reason, attestation FROM agb.current_level"
_MODE = "SELECT mode, since, actor FROM agb.current_mode"
_STATUS = "SELECT seq, name, measured, threshold, met FROM agb.ladder_status ORDER BY seq"
_READY = "SELECT target, ready, coalesce(outstanding, '{}') FROM agb.ladder_ready"
_LIVE_STATUS = "SELECT seq, name, measured, threshold, met FROM agb.live_status ORDER BY seq"
_LIVE_READY = "SELECT ready, criteria_met, coalesce(outstanding, '{}') FROM agb.live_ready"
_RAISE = """
    INSERT INTO agb.ladder_event (level, actor, reason, attestation, command_id)
    VALUES (%(level)s, %(actor)s, %(reason)s, %(attestation)s, %(command_id)s)
"""
_DROP = """
    INSERT INTO agb.ladder_event (level, actor, reason, basket_id)
    VALUES ('L0', 'system', %(reason)s, %(basket_id)s)
"""
_SET_MODE = """
    INSERT INTO agb.mode_event (mode, actor, reason, attestation, command_id)
    VALUES (%(mode)s, %(actor)s, %(reason)s, %(attestation)s, %(command_id)s)
"""
# The first live rebalance since the level was set that the human rejected or the gate
# halted. Either drops the system back to L0; at L0 there is nothing to drop.
_BREACH = """
    SELECT r.basket_id, r.reference, r.state
    FROM agb.current_level l
    JOIN agb.ladder_rebalance r
        ON r.mode = 'live' AND r.state IN ('rejected', 'halted') AND r.since > l.since
    WHERE l.level <> 'L0'
    ORDER BY r.basket_id
    LIMIT 1
"""


def current(conn: psycopg.Connection) -> Level:
    """The level the system is at. There is always one: the migration records L0."""
    row = conn.execute(_LEVEL).fetchone()
    assert row is not None, "agb.ladder_event starts with an L0 event"
    return Level(*row)


def mode(conn: psycopg.Connection) -> str:
    """The trading mode, `paper` or `live`."""
    row = conn.execute(_MODE).fetchone()
    assert row is not None, "agb.mode_event starts with a paper event"
    return row[0]


def status(conn: psycopg.Connection) -> list[Criterion]:
    """The criteria for the level above, measured. Empty at L2, which is the top."""
    return [Criterion(*row) for row in conn.execute(_STATUS).fetchall()]


def ready(conn: psycopg.Connection) -> Readiness | None:
    """Whether the level above is within reach, or None at L2."""
    row = conn.execute(_READY).fetchone()
    return None if row is None else Readiness(*row)


def live_status(conn: psycopg.Connection) -> list[Criterion]:
    """The paper-to-live criteria, each measured from the recorded history."""
    return [Criterion(*row) for row in conn.execute(_LIVE_STATUS).fetchall()]


def live_ready(conn: psycopg.Connection) -> Readiness:
    """Whether paper has earned live, and which criteria are outstanding."""
    row = conn.execute(_LIVE_READY).fetchone()
    assert row is not None, "agb.live_status always measures its criteria"
    return Readiness("live", row[0], row[2])


def approval_required(conn: psycopg.Connection) -> bool:
    """Whether a rebalance needs the human's answer before it may be approved. True below
    L2, where a clean basket still waits out its countdown; at L2 the message only tells."""
    return current(conn).level != "L2"


def raise_to(
    conn: psycopg.Connection, level: str, attestation: str | None, source: approval.Source
) -> str:
    """Move the system to `level`. An up-move is refused unless the eligibility view for
    that level is satisfied and the human attests to it; the move is a `level` command and a
    `ladder_event` naming it. A move to a tighter level is always allowed. A refusal is
    recorded and raised as Refused."""
    args: dict[str, Any] = {"level": level, "attestation": attestation}
    now = current(conn)
    why = _level_refusal(conn, level, attestation, now)
    if why is not None:
        approval.record(conn, source, LEVEL, args, why)
        raise Refused(why)
    up = LEVELS.index(level) > LEVELS.index(now.level)
    reason = f"{'raised' if up else 'lowered'} from {now.level} by {source.actor}"
    with conn.transaction():
        command_id = approval.record(conn, source, LEVEL, args)
        conn.execute(
            _RAISE,
            {
                "level": level,
                "actor": source.actor,
                "reason": reason,
                "attestation": attestation,
                "command_id": command_id,
            },
        )
    return f"the level is {level}: {MEANING[level]}."


def _level_refusal(
    conn: psycopg.Connection, level: str, attestation: str | None, now: Level
) -> str | None:
    if level not in LEVELS:
        return f"{level} is not a level; the levels are {', '.join(LEVELS)}"
    if level == now.level:
        return f"the level is already {level}, set {now.since:%Y-%m-%d} by {now.actor}"
    if level != "L0" and not attestation:
        return f"a move to {level} needs your attestation that it is warranted"
    if LEVELS.index(level) < LEVELS.index(now.level):
        return None
    if LEVELS.index(level) > LEVELS.index(now.level) + 1:
        step = LEVELS[LEVELS.index(now.level) + 1]
        return f"the ladder is climbed one level at a time; {step} comes after {now.level}"
    eligible = ready(conn)
    if eligible is None or eligible.target != level:
        return f"nothing measures eligibility for {level} from {now.level}"
    if not eligible.ready:
        return f"{level} is not earned yet: {', '.join(eligible.outstanding)} outstanding"
    return None


def set_mode(
    conn: psycopg.Connection, to: str, attestation: str | None, source: approval.Source
) -> str:
    """Move the trading mode. Live is refused until every paper-to-live criterion is met and
    the human attests to it; paper is always allowed. The move is a `mode` command and a
    `mode_event` naming it, and a refusal is recorded and raised as Refused."""
    args: dict[str, Any] = {"mode": to, "attestation": attestation}
    was = mode(conn)
    why = _mode_refusal(conn, to, attestation, was)
    if why is not None:
        approval.record(conn, source, MODE, args, why)
        raise Refused(why)
    with conn.transaction():
        command_id = approval.record(conn, source, MODE, args)
        conn.execute(
            _SET_MODE,
            {
                "mode": to,
                "actor": source.actor,
                "reason": f"moved from {was} by {source.actor}",
                "attestation": attestation,
                "command_id": command_id,
            },
        )
    if to == "paper":
        return "the mode is paper: nothing reaches the broker."
    return f"the mode is live, at {current(conn).level}: {MEANING[current(conn).level]}."


def _mode_refusal(
    conn: psycopg.Connection, to: str, attestation: str | None, was: str
) -> str | None:
    if to not in MODES:
        return f"{to} is not a mode; the modes are {', '.join(MODES)}"
    if to == was:
        return f"the mode is already {to}"
    if to == "paper":
        return None
    if not attestation:
        return "a move to live needs your attestation that the paper record is sound"
    eligible = live_ready(conn)
    if not eligible.ready:
        return (
            "live is not earned yet: "
            f"{', '.join(eligible.outstanding)} outstanding in the paper record"
        )
    return None


def demote(conn: psycopg.Connection) -> str | None:
    """Drop to L0 if a live rebalance has been rejected or halted since the level was set,
    recorded as the system. Returns what it recorded, or None when nothing has breached.
    Tightening needs no attestation and no human."""
    row = conn.execute(_BREACH).fetchone()
    if row is None:
        return None
    basket_id, reference, state = row
    what = "the human rejected" if state == "rejected" else "the gate halted"
    reason = f"dropped to L0 because {what} the live rebalance {reference}"
    conn.execute(_DROP, {"reason": reason, "basket_id": basket_id})
    return reason


def level_text(conn: psycopg.Connection) -> str:
    """`/level`: the level, what it means, and the progress towards the next one. It offers
    no way to move up, because an up-move is a deliberate step taken at the terminal."""
    now = current(conn)
    lines = [
        f"Level {now.level} since {now.since:%Y-%m-%d}, set by {now.actor}.",
        MEANING[now.level],
    ]
    if now.attestation:
        lines.append(f"Attested: {now.attestation}")
    criteria = status(conn)
    if not criteria:
        lines.append("\nL2 is the top of the ladder.")
    else:
        eligible = ready(conn)
        target = eligible.target if eligible is not None else LEVELS[LEVELS.index(now.level) + 1]
        lines.append(f"\nTowards {target}:")
        lines += [f"  {c.line()}" for c in criteria]
        earned = eligible is not None and eligible.ready
        lines.append(
            f"\n{target} is earned. Raise it at the terminal with `agb level {target}`."
            if earned
            else f"\n{target} is not earned yet. The ladder loosens only at the terminal."
        )
    lines.append("The pre-trade gate and the kill-switch apply at every level.")
    return "\n".join(lines)


def live_text(conn: psycopg.Connection) -> str:
    """The paper-to-live view: every criterion as met or unmet, and what is outstanding. A
    criterion nobody has measured reads as unmet, which is what the three arithmetic checks
    and the panel pre-flight suite do until a pass is on record."""
    now = mode(conn)
    eligible = live_ready(conn)
    if now == "live":
        head = "Trading live."
    elif eligible.ready:
        head = "Paper has earned live. Switch it at the terminal with `agb mode live`."
    else:
        head = f"Paper, with {len(eligible.outstanding)} criteria outstanding."
    lines = [head, "", "Towards live:"]
    lines += [f"  {c.line()}" for c in live_status(conn)]
    if not eligible.ready:
        lines.append(f"\nOutstanding: {', '.join(eligible.outstanding)}.")
    return "\n".join(lines)
