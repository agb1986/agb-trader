"""The `agb` operator CLI. It connects as the `cli` role through the PG* environment."""

import argparse
import os
import sys
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import psycopg

from agb import panel
from agb.nyse import NyseCalendar, trading_date
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


def clock() -> datetime:
    return datetime.now(UTC)


def _status(args: argparse.Namespace) -> int:
    with psycopg.connect(application_name="agb-cli") as conn:
        print(status(conn))
    return 0


_PANEL_CONFIGS = """
    SELECT id, created_at, shortlist_n, w, analyst_model, prosecutor_model, defender_model,
           judge_model, rubric_version, scrubber_version, actor
    FROM agb.panel_config
    ORDER BY id
"""


def format_panel_configs(rows: Iterable[tuple]) -> str:
    lines = [
        f"{'ID':>4}  {'CREATED (UTC)':<16}  {'N':>3}  {'W':>5}  RUBRIC  SCRUBBER  MODELS  ACTOR"
    ]
    for id_, created, n, w, *models, rubric, scrubber, actor in rows:
        when = created.strftime("%Y-%m-%d %H:%M")
        lines.append(
            f"{id_:>4}  {when:<16}  {n:>3}  {float(w):>5.2f}  {rubric:<6}  {scrubber:<8}  "
            f"{'/'.join(models)}  {actor}"
        )
    return "\n".join(lines)


def _panel_config_create(args: argparse.Namespace) -> int:
    models = {
        role: model for role in panel.ROLES if (model := getattr(args, f"{role}_model")) is not None
    }
    with psycopg.connect(application_name="agb-cli") as conn:
        config_id = panel.create_config(conn, args.actor, args.n, args.w, models)
    print(f"panel_config {config_id} created")
    return 0


def _panel_config_list(args: argparse.Namespace) -> int:
    with psycopg.connect(application_name="agb-cli") as conn:
        print(format_panel_configs(conn.execute(_PANEL_CONFIGS).fetchall()))
    return 0


# agb.approval reads agb.health, which reads this module's format_age, so the two commands
# that use it import it where they run.


def _paper_approval(args: argparse.Namespace) -> int:
    from agb import approval

    with psycopg.connect(application_name="agb-cli") as conn:
        if args.setting == "show":
            print(approval.paper_approval_text(conn))
        else:
            required = args.setting == "require"
            print(approval.set_paper_approval(conn, required, args.actor, args.reason))
    return 0


def _rerun(args: argparse.Namespace) -> int:
    from agb import approval

    now = clock()
    today = trading_date(now)
    calendar = NyseCalendar(today - timedelta(days=7), today + timedelta(days=30))
    source = approval.Source(args.actor, "cli", f"agb rerun {args.reference}")
    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            print(approval.request_rerun(conn, args.reference, source, now, calendar))
        except approval.RerunRefused as exc:
            # The refusal is recorded; leaving the block commits it.
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="agb", description="agb-trader operator CLI")
    parser.add_argument("--version", action="version", version=os.environ.get("AGB_VERSION", "dev"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="each service's last heartbeat and its age").set_defaults(
        run=_status
    )
    configs = commands.add_parser(
        "panel-config", help="the panel's immutable configurations"
    ).add_subparsers(dest="action", required=True)
    create = configs.add_parser(
        "create",
        help="a new panel_config for this code's prompts, rubric and scrubber",
        description="Configurations are never changed: a new one takes effect from the next "
        "panel run. The prompt hashes, rubric version and scrubber version are this code's.",
    )
    create.add_argument("--n", type=int, default=50, help="shortlist size N (default 50)")
    create.add_argument("--w", type=float, default=0.2, help="blend weight w (default 0.2)")
    defaults = panel.default_models()
    for role in panel.ROLES:
        create.add_argument(f"--{role}-model", metavar="MODEL", help=f"default {defaults[role]}")
    create.add_argument(
        "--actor",
        default=os.environ.get("USER") or "operator",
        help="who is creating it (default $USER)",
    )
    create.set_defaults(run=_panel_config_create)
    configs.add_parser("list", help="every panel_config, oldest first").set_defaults(
        run=_panel_config_list
    )
    actor = {
        "default": os.environ.get("USER") or "operator",
        "help": "who is asking (default $USER)",
    }
    paper = commands.add_parser(
        "paper-approval",
        help="whether a paper rebalance waits for the human, as live mode does",
        description="Paper mode runs unattended by default: a basket that passes the gate is "
        "approved by the system. 'require' makes it wait for /approve, as in live mode, to "
        "rehearse the loop; it applies from the next gate run.",
    )
    paper.add_argument("setting", choices=["show", "require", "unattended"])
    paper.add_argument("--reason", help="why, kept with the setting")
    paper.add_argument("--actor", **actor)
    paper.set_defaults(run=_paper_approval)
    rerun = commands.add_parser(
        "rerun",
        help="trade a missed rebalance at the next session's close",
        description="Asks for a new basket, REFERENCE r1 and on, whose deviation_of names the "
        "missed one, which keeps its outcome. The pipeline builds and gates it, and it waits "
        "for /approve like any other.",
    )
    rerun.add_argument("reference", help="the missed rebalance, e.g. 2026-10")
    rerun.add_argument("--actor", **actor)
    rerun.set_defaults(run=_rerun)
    args = parser.parse_args(argv)
    try:
        return args.run(args)
    except psycopg.Error as exc:
        print(f"agb: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
