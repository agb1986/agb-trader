"""The `agb` operator CLI. It connects as the `cli` role through the PG* environment."""

import argparse
import os
import sys
from collections.abc import Iterable
from datetime import datetime, timedelta

import psycopg

from agb.roles import SERVICES

_HEARTBEATS = """
    SELECT service, beat_at, now() - beat_at AS age, version
    FROM agb.heartbeat
    ORDER BY service
"""


def format_age(age: timedelta) -> str:
    seconds = max(0, int(age.total_seconds()))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def format_status(rows: Iterable[tuple[str, datetime, timedelta, str | None]]) -> str:
    """One line per service: the known services first, in order, then any others seen."""
    beats = {service: (beat_at, age, version) for service, beat_at, age, version in rows}
    services = [*SERVICES, *sorted(beats.keys() - set(SERVICES))]
    lines = [f"{'SERVICE':<10}  {'LAST BEAT (UTC)':<19}  {'AGE':>7}  VERSION"]
    for service in services:
        if service in beats:
            beat_at, age, version = beats[service]
            when = beat_at.strftime("%Y-%m-%d %H:%M:%S")
            lines.append(f"{service:<10}  {when:<19}  {format_age(age):>7}  {version or '-'}")
        else:
            lines.append(f"{service:<10}  {'never':<19}  {'-':>7}  -")
    return "\n".join(lines)


def status(conn: psycopg.Connection) -> str:
    return format_status(conn.execute(_HEARTBEATS).fetchall())


def _status(args: argparse.Namespace) -> int:
    with psycopg.connect(application_name="agb-cli") as conn:
        print(status(conn))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="agb", description="agb-trader operator CLI")
    parser.add_argument("--version", action="version", version=os.environ.get("AGB_VERSION", "dev"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="each service's last heartbeat and its age").set_defaults(
        run=_status
    )
    args = parser.parse_args(argv)
    try:
        return args.run(args)
    except psycopg.Error as exc:
        print(f"agb: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
