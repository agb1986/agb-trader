"""What is unhealthy, read from the database: stale heartbeats, overdue expected events and
a rebalance that was never scheduled. The bot turns these into alerts and the pre-flight.

Each finding carries the dedupe key of the alert it raises, and two short lines of plain
text: what is wrong, then the facts behind it.
"""

import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import psycopg

from agb.cli import format_age
from agb.nyse import NEW_YORK

# A service whose heartbeat is older than this is stale: ten missed beats at the default
# 30-second interval.
STALE_AFTER = timedelta(minutes=5)


@dataclass(frozen=True)
class Finding:
    kind: str
    subject: str
    period: str
    what: str
    facts: str

    def lines(self) -> str:
        return f"  {self.what}\n    {self.facts}"


def stale_after_from_env() -> timedelta:
    seconds = os.environ.get("AGB_STALE_AFTER_SECONDS")
    return timedelta(seconds=float(seconds)) if seconds else STALE_AFTER


def et(at: datetime) -> str:
    return at.astimezone(NEW_YORK).strftime("%Y-%m-%d %H:%M ET")


def ago(now: datetime, at: datetime) -> str:
    return f"{format_age(now - at)} ago"


def stale_heartbeats(conn: psycopg.Connection, now: datetime, stale_after: timedelta):
    """Every other service whose last beat is older than `stale_after`. A service that has
    never beaten has no row, and is not reported here."""
    rows = conn.execute(
        "SELECT service, beat_at FROM agb.heartbeat "
        "WHERE beat_at < %s AND service <> current_user ORDER BY service",
        (now - stale_after,),
    ).fetchall()
    # One alert per stale episode: the last beat identifies it, so a service that recovers and
    # goes stale again raises a new one.
    return [
        Finding(
            "stale_heartbeat",
            service,
            beat_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"stale heartbeat: {service}",
            f"last beat {ago(now, beat_at)}, at {et(beat_at)}",
        )
        for service, beat_at in rows
    ]


def overdue(conn: psycopg.Connection, now: datetime) -> list[Finding]:
    """The overdue view as of `now`: past-due expected events, and no rebalance ahead."""
    rows = conn.execute(
        "SELECT kind, period, due_at, reason FROM agb.overdue_at(%s) ORDER BY due_at, kind",
        (now,),
    ).fetchall()
    findings = []
    for kind, period, due_at, reason in rows:
        if reason == "no_rebalance_within_horizon":
            # One alert until another rebalance is scheduled: the last one identifies it.
            (last,) = conn.execute(
                "SELECT max(period) FROM agb.expected_event WHERE kind = 'submission'"
            ).fetchone()
            findings.append(
                Finding(
                    "no_rebalance_within_horizon",
                    kind,
                    last or "none",
                    "no rebalance within horizon",
                    f"none due before {due_at.astimezone(NEW_YORK):%Y-%m-%d}; "
                    f"last scheduled {last or 'none'}",
                )
            )
        else:
            findings.append(
                Finding(
                    "overdue",
                    kind,
                    period,
                    f"overdue: {kind} {period}",
                    f"due {et(due_at)}, {ago(now, due_at)}",
                )
            )
    return findings


def rebalance_not_scheduled(conn: psycopg.Connection, period: str, trade: date) -> list[Finding]:
    """The rebalance trading on `trade` has no submission expected, so nothing would miss it."""
    scheduled = conn.execute(
        "SELECT 1 FROM agb.expected_event WHERE kind = 'submission' AND period = %s", (period,)
    ).fetchone()
    if scheduled:
        return []
    return [
        Finding(
            "rebalance_not_scheduled",
            "submission",
            period,
            f"rebalance {period} not scheduled",
            f"no submission expected for {trade:%Y-%m-%d}",
        )
    ]


def open_halt_causes(conn: psycopg.Connection) -> list[Finding]:
    """Open halt causes. The halt latch and its view come with the pre-trade gate (#21), whose
    columns are not yet defined; until then there is nothing to read and nothing is reported."""
    return []


def broker_mismatch(conn: psycopg.Connection) -> list[Finding]:
    """A broker `suspend_trade` that disagrees with the halt latch. The observed broker state
    comes with execution (#22); until then there is nothing to read and nothing is reported."""
    return []
