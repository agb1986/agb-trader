"""The `agb` operator CLI. It connects as the `cli` role through the PG* environment."""

import argparse
import os
import shlex
import sys
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path

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


# What each job kind actually takes, claim to completion. A reclaimed job is measured from
# its last claim, because that is the attempt that finished. Jobs that failed or are still
# running have no duration and are left out.
_RUNTIMES = """
    WITH span AS (
        SELECT j.id,
               j.kind,
               max(e.at) FILTER (WHERE e.event IN ('claimed', 'reclaimed')) AS started,
               max(e.at) FILTER (WHERE e.event = 'completed') AS finished
        FROM agb.job j
        JOIN agb.job_event e ON e.job_id = j.id
        GROUP BY j.id, j.kind
    )
    SELECT kind,
           count(*) AS runs,
           min(finished - started) AS fastest,
           percentile_disc(0.5) WITHIN GROUP (ORDER BY finished - started) AS median,
           max(finished - started) AS slowest
    FROM span
    WHERE started IS NOT NULL AND finished IS NOT NULL AND finished >= started
    GROUP BY kind
    ORDER BY kind
"""


def format_runtimes(rows: Iterable[tuple[str, int, timedelta, timedelta, timedelta]]) -> str:
    """One line per job kind, for setting poll intervals and lease durations from measurement."""
    lines = [f"{'KIND':<16}  {'RUNS':>5}  {'FASTEST':>8}  {'MEDIAN':>8}  {'SLOWEST':>8}"]
    counted = 0
    for kind, runs, fastest, median, slowest in rows:
        counted += 1
        lines.append(
            f"{kind:<16}  {runs:>5}  {format_age(fastest):>8}  "
            f"{format_age(median):>8}  {format_age(slowest):>8}"
        )
    if not counted:
        lines.append("no completed job has a claim and a completion to measure between")
    return "\n".join(lines)


def runtimes(conn: psycopg.Connection) -> str:
    return format_runtimes(conn.execute(_RUNTIMES).fetchall())


def _runtimes(args: argparse.Namespace) -> int:
    with psycopg.connect(application_name="agb-cli") as conn:
        print(runtimes(conn))
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


# agb.approval reads agb.health, which reads this module's format_age, so the commands that
# use it, directly or through agb.registry, import it where they run.


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


def _models_dir() -> Path:
    from agb import registry

    return Path(os.environ.get(registry.MODELS_ENV, registry.DEFAULT_MODELS_DIR))


def _import_certification(args: argparse.Namespace) -> int:
    from psycopg import errors

    from agb.research import artifact

    directory = _models_dir() / args.model_id
    try:
        with psycopg.connect(application_name="agb-cli") as conn:
            row_id = artifact.import_artifact(conn, directory)
    except artifact.CertificationError as exc:
        print(f"agb: {exc}", file=sys.stderr)
        return 1
    except errors.InsufficientPrivilege:
        print(
            "agb: only the research role may insert a certification; use PGUSER=research",
            file=sys.stderr,
        )
        return 1
    print(f"model_certification {row_id}: {directory}")
    return 0


def _promote(args: argparse.Namespace) -> int:
    from agb import registry

    raw = f"agb promote {args.model_id}"
    if args.waive_for_paper:
        if not args.reason:
            print(
                "agb: --waive-for-paper needs --reason: the waiver promotes a model that"
                " failed certification, and the reason is what the audit log keeps",
                file=sys.stderr,
            )
            return 2
        raw += f" --waive-for-paper --reason {shlex.quote(args.reason)}"
    elif args.reason:
        print("agb: --reason is for --waive-for-paper only", file=sys.stderr)
        return 2
    source = registry.Source(args.actor, "cli", raw)
    waive = args.reason if args.waive_for_paper else None
    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            print(
                registry.promote(conn, args.model_id, source, _models_dir(), waive_for_paper=waive)
            )
        except registry.Refused as exc:
            # The refusal is recorded; leaving the block commits it.
            print(f"agb: refused: {exc}", file=sys.stderr)
            # Which database was asked. A certification lives in the one it was imported
            # into, so the same command run against another says the model is uncertified.
            print(f"agb: asked {_where(conn)}", file=sys.stderr)
            return 1
    return 0


def _where(conn: psycopg.Connection) -> str:
    """The database the connection reached, as the refusals name it."""
    info = conn.info
    return f"database {info.dbname} on {info.host or 'a local socket'} as {info.user}"


def _void(args: argparse.Namespace) -> int:
    from agb import registry

    source = registry.Source(
        args.actor, "cli", f"agb void {args.model_id} --reason {shlex.quote(args.reason)}"
    )
    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            print(registry.void(conn, args.model_id, args.reason, source))
        except registry.Refused as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
    return 0


def _void_stale(args: argparse.Namespace) -> int:
    from agb import cost, registry

    source = registry.Source(args.actor, "cli", "agb void-stale")
    with psycopg.connect(application_name="agb-cli") as conn:
        voided = registry.void_stale(conn, source)
    if voided:
        print(f"voided under cost model {cost.CURRENT.version}: {', '.join(voided)}")
    else:
        print(f"nothing to void: none is under a cost model before {cost.CURRENT.version}")
    return 0


def _reconcile(args: argparse.Namespace) -> int:
    from psycopg import errors

    from agb import recovery
    from agb.alpaca import Alpaca

    if not Alpaca.configured():
        print("agb: refused: ALPACA_API_KEY and ALPACA_SECRET_KEY are not set", file=sys.stderr)
        return 1
    # Autocommit, so every order's event is committed as the broker answers for it: a
    # reconcile interrupted halfway keeps what it has already learned.
    try:
        with psycopg.connect(application_name="agb-cli", autocommit=True) as conn:
            report = recovery.recover(conn, Alpaca.from_env(), args.reference, args.actor)
    except recovery.RecoveryError as exc:
        print(f"agb: {exc}", file=sys.stderr)
        return 1
    except errors.InsufficientPrivilege:
        print(
            "agb: only the execution role may reconcile orders; use PGUSER=execution",
            file=sys.stderr,
        )
        return 1
    for walked in report.walked:
        print(
            f"{walked.client_order_id}  {walked.side} {walked.qty} {walked.symbol}: "
            f"{walked.outcome} - {walked.detail}"
        )
    print(f"basket {report.reference}: {report.detail}")
    print("appended as events; nothing was sent, cancelled or changed")
    return 0


def _panel_dir() -> Path:
    from agb import panel_store

    return Path(os.environ.get(panel_store.DIR_ENV, panel_store.DEFAULT_DIR))


def _record(conn: psycopg.Connection, config_id: int, result, actor: str) -> int:
    """Print a check's result and append it to the configuration's record."""
    from agb import preflight

    print(result)
    detail = preflight.detail_text(result)
    if detail:
        print(detail)
    row_id = preflight.record(conn, config_id, result, actor)
    print(f"panel_preflight {row_id} recorded for panel_config {config_id}")
    return 0 if result.passed else 1


def _panel_preflight_measure(args: argparse.Namespace) -> int:
    from agb import preflight

    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            config_id = preflight.config_of(conn, args.run_id)
            result = getattr(preflight, args.check_name)(conn, args.run_id)
        except preflight.PreflightError as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
        return _record(conn, config_id, result, args.actor)


def _panel_preflight_recall(args: argparse.Namespace) -> int:
    from psycopg import errors

    from agb import analyst, edgar, preflight
    from agb.panel_store import PanelStore

    store = PanelStore(_panel_dir())
    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            runs = preflight.recall_runs(
                conn, args.config, edgar.Edgar.from_env(), analyst.AnthropicModel(), store
            )
            result = preflight.recall(conn, store, runs)
        except errors.InsufficientPrivilege:
            print(
                "agb: only the pipeline role may run the panel; use PGUSER=pipeline",
                file=sys.stderr,
            )
            return 1
        except preflight.PreflightError as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
        return _record(conn, args.config, result, args.actor)


_RECALL_RUNS = """
    SELECT detail -> 'runs'
    FROM agb.panel_preflight
    WHERE panel_config_id = %s AND name = 'disclosure_recall'
    ORDER BY id DESC
    LIMIT 1
"""


def _panel_preflight_fixtures(args: argparse.Namespace) -> int:
    from datetime import date

    from agb import preflight
    from agb.panel_store import PanelStore

    with psycopg.connect(application_name="agb-cli") as conn:
        row = conn.execute(_RECALL_RUNS, (args.config,)).fetchone()
        runs = {date.fromisoformat(k): v for k, v in (row[0] if row else {}).items()}
        if not runs:
            print(
                f"agb: panel_config {args.config} has no recall run; each fixture's standing is "
                "read from the runs that measured it, so run `agb panel-preflight recall` first",
                file=sys.stderr,
            )
            return 1
        print(preflight.fixtures_text(conn, PanelStore(_panel_dir()), runs))
    return 0


def _panel_preflight_spot_check(args: argparse.Namespace) -> int:
    from agb import preflight

    if args.symbol and not args.verdict:
        print("agb: refused: --symbol needs a --verdict", file=sys.stderr)
        return 1
    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            config_id = preflight.config_of(conn, args.run_id)
            if args.symbol:
                reviewed = preflight.review(
                    conn, args.run_id, args.symbol, args.verdict, args.note, args.actor
                )
                print(f"{reviewed.symbol}: {args.verdict} recorded")
            reviews = preflight.sample(conn, args.run_id)
            pending = preflight.pending(reviews)
            if pending is not None:
                print()
                print(preflight.review_text(conn, args.run_id, pending, reviews))
                return 0
            result = preflight.spot_check(conn, args.run_id)
        except preflight.PreflightError as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
        if preflight.recorded(conn, config_id, result.name, args.run_id):
            print(result)
            return 0 if result.passed else 1
        return _record(conn, config_id, result, args.actor)


def _panel_preflight_show(args: argparse.Namespace) -> int:
    from agb import preflight

    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            print(preflight.status_text(conn, args.config))
        except preflight.PreflightError as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
    return 0


def _level(args: argparse.Namespace) -> int:
    from agb import approval, ladder

    with psycopg.connect(application_name="agb-cli") as conn:
        if args.level is None:
            print(ladder.level_text(conn))
            return 0
        text = f"agb level {args.level}" + (
            f" --attest {shlex.quote(args.attest)}" if args.attest else ""
        )
        source = approval.Source(args.actor, "cli", text)
        try:
            print(ladder.raise_to(conn, args.level, args.attest, source))
        except ladder.Refused as exc:
            # The refusal is recorded; leaving the block commits it.
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
    return 0


def _mode(args: argparse.Namespace) -> int:
    from agb import approval, ladder

    with psycopg.connect(application_name="agb-cli") as conn:
        if args.mode is None:
            print(ladder.live_text(conn))
            return 0
        text = f"agb mode {args.mode}" + (
            f" --attest {shlex.quote(args.attest)}" if args.attest else ""
        )
        source = approval.Source(args.actor, "cli", text)
        try:
            print(ladder.set_mode(conn, args.mode, args.attest, source))
        except ladder.Refused as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
    return 0


def _corporate(args: argparse.Namespace) -> int:
    from datetime import date
    from decimal import Decimal, InvalidOperation

    from agb import approval, corporate

    with psycopg.connect(application_name="agb-cli") as conn:
        if args.action == "show":
            events = corporate.for_period(conn, args.period)
            if not events:
                print(f"no forced events recorded for {args.period}")
                return 0
            print("\n".join(corporate.line(event) for event in events))
            return 0
        text = " ".join(
            shlex.quote(part)
            for part in [
                "agb",
                "corporate",
                "record",
                args.kind,
                args.symbol,
                "--period",
                args.period,
                "--effective",
                args.effective,
                "--shares",
                args.shares,
                "--detail",
                args.detail,
                *(["--price", args.price] if args.price else []),
                *(["--into", args.into] if args.into else []),
                *(["--into-shares", args.into_shares] if args.into_shares else []),
            ]
        )
        source = approval.Source(args.actor, "cli", text)
        try:
            event = corporate.Event(
                period=args.period,
                symbol=args.symbol,
                kind=args.kind,
                effective_date=date.fromisoformat(args.effective),
                shares=Decimal(args.shares),
                detail=args.detail,
                price_usd=None if args.price is None else Decimal(args.price),
                into_symbol=args.into,
                into_shares=None if args.into_shares is None else Decimal(args.into_shares),
            )
            row_id = corporate.record(conn, event, source)
        except (corporate.CorporateError, InvalidOperation, ValueError) as exc:
            # A refusal that reached the database is recorded; leaving the block commits it.
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
    print(f"forced_event {row_id}: {corporate.line(event)}")
    return 0


def _lake_dir() -> Path:
    from agb import lake

    return Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR))


def _paper_check(args: argparse.Namespace) -> int:
    from agb import lake, mechanics

    with psycopg.connect(application_name="agb-cli") as conn:
        try:
            if args.check_name == "fill_tolerance":
                result = mechanics.fill_tolerance(conn, lake.connect(_lake_dir()))
            else:
                result = getattr(mechanics, args.check_name)(conn)
        except mechanics.MechanicsError as exc:
            print(f"agb: refused: {exc}", file=sys.stderr)
            return 1
        print(result)
        detail = mechanics.detail_text(result)
        if detail:
            print(detail)
        row_id = mechanics.record(conn, result, args.actor)
    print(f"paper_check {row_id}: {result.name} recorded")
    return 0 if result.passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="agb", description="agb-trader operator CLI")
    parser.add_argument("--version", action="version", version=os.environ.get("AGB_VERSION", "dev"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="each service's last heartbeat and its age").set_defaults(
        run=_status
    )
    commands.add_parser(
        "runtimes",
        help="how long each job kind has taken, claim to completion",
        description="Reads the measured durations out of the job event log, which is what "
        "the poll intervals and lease durations in src/agb/schedule.py are set from.",
    ).set_defaults(run=_runtimes)
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
    certification = commands.add_parser(
        "import-certification",
        help="insert a certified model's row from its artifact under AGB_MODELS_DIR",
        description="The research profile certifies on the workstation and the artifact is "
        "copied to the models directory; this records it. Only the research role may insert, "
        "so run it with PGUSER=research. The artifact is verified first: an altered or "
        "unlisted file, a ledger whose trial count disagrees with the certification's, and a "
        "model already certified are each refused, and nothing is written.",
    )
    certification.add_argument("model_id", help="the artifact's directory under AGB_MODELS_DIR")
    certification.set_defaults(run=_import_certification)
    promote = commands.add_parser(
        "promote",
        help="make a certified model the one the pipeline ranks with",
        description="Refuses a model with no passing certification, a voided or stale one, and "
        "one whose artifact under AGB_MODELS_DIR is missing, altered or ships a trial count "
        "other than the certification declared. Every refusal is recorded. "
        "--waive-for-paper --reason <why> promotes a model whose only problem is that it "
        "failed its gates, in paper mode alone, so that the stack can be exercised while "
        "nothing has passed; a move to live is then refused until a model that passed is "
        "promoted.",
    )
    promote.add_argument(
        "model_id", help="the certified model, as `agb.model_certification` has it"
    )
    promote.add_argument(
        "--waive-for-paper",
        action="store_true",
        help="promote a model that failed its gates, in paper mode only; needs --reason",
    )
    promote.add_argument("--reason", help="why the waiver is being taken, for the audit log")
    promote.add_argument("--actor", **actor)
    promote.set_defaults(run=_promote)
    void = commands.add_parser(
        "void",
        help="void a model's certification for good",
        description="A voided certification cannot be promoted, and if it is the live model "
        "every rebalance fails until another is promoted.",
    )
    void.add_argument("model_id", help="the certified model")
    void.add_argument("--reason", required=True, help="why, kept with the voiding")
    void.add_argument("--actor", **actor)
    void.set_defaults(run=_void)
    stale = commands.add_parser(
        "void-stale",
        help="void every certification made under an earlier cost model",
        description="Any change to the cost model invalidates the certifications made under "
        "it. The deploy runs this once the new code is up; promotion runs it first.",
    )
    stale.add_argument("--actor", **actor)
    stale.set_defaults(run=_void_stale)
    reconcile = commands.add_parser(
        "reconcile",
        help="ask the broker what it knows about every order of a basket, by client_order_id",
        description="After a submission died mid-flight. It walks every planned order of the "
        "basket and reports each one as filled, still open at the broker, written but never "
        "reached the broker, or never written. What it learns is appended as order events and "
        "one reconcile_run row: no history row is changed, and no order is sent or cancelled. "
        "Deciding what to do next is yours, as is clearing the halt with /resume. Only the "
        "execution role holds the broker's keys, so run it with `make reconcile`.",
    )
    reconcile.add_argument("reference", help="the basket to walk, e.g. 2026-10")
    reconcile.add_argument("--actor", **actor)
    reconcile.set_defaults(run=_reconcile)
    preflight_commands = commands.add_parser(
        "panel-preflight",
        help="the panel pre-flight suite: four checks, recorded per panel_config",
        description="Whether the panel can be trusted under one configuration: retrieval "
        "fidelity, known-disclosure recall, the operator's spot check and the dispersion "
        "gate. Results belong to the panel_config they were measured under, so a new "
        "configuration starts with none of them and has to be pre-flighted again.",
    ).add_subparsers(dest="action", required=True)
    for name, helptext in (
        ("fidelity", "every quote of a run found verbatim in the document it cites"),
        ("dispersion", "the spread of a run's scores, against the gate's floor"),
    ):
        measure = preflight_commands.add_parser(name, help=helptext)
        measure.add_argument("run_id", type=int, metavar="RUN", help="the panel_run to measure")
        measure.add_argument("--actor", **actor)
        measure.set_defaults(run=_panel_preflight_measure, check_name=name)
    recall = preflight_commands.add_parser(
        "recall",
        help="the committed fixture set of known disclosures, by dimension",
        description="Runs the panel over agb.disclosures as of each fixture's date and reports "
        "what it found. This spends model calls and inserts panel runs, so it runs as the "
        "pipeline role: PGUSER=pipeline agb panel-preflight recall N.",
    )
    recall.add_argument("config", type=int, metavar="CONFIG", help="the panel_config to measure")
    recall.add_argument("--actor", **actor)
    recall.set_defaults(run=_panel_preflight_recall)
    fixtures = preflight_commands.add_parser(
        "fixtures",
        help="each fixture's standing in the last recall run",
        description="A fixture whose phrase is not in the text the Analyst was given is "
        "unusable, not a miss: fix or drop the entry in agb.disclosures.",
    )
    fixtures.add_argument("config", type=int, metavar="CONFIG", help="the panel_config to read")
    fixtures.set_defaults(run=_panel_preflight_fixtures)
    spot = preflight_commands.add_parser(
        "spot-check",
        help="read about 20 of a run's names and record a verdict on each",
        description="Presents the next name awaiting a verdict, with the panel's score, its "
        "reasons and the quotes it read. Record one with --symbol and --verdict; the check is "
        "recorded once every name in the sample has a verdict.",
    )
    spot.add_argument("run_id", type=int, metavar="RUN", help="the panel_run under review")
    spot.add_argument("--symbol", help="the name being judged (default: show the next one)")
    spot.add_argument(
        "--verdict", choices=("agree", "disagree"), help="whether the panel's reading holds up"
    )
    spot.add_argument("--note", help="why, which a 'disagree' requires")
    spot.add_argument("--actor", **actor)
    spot.set_defaults(run=_panel_preflight_spot_check)
    show = preflight_commands.add_parser(
        "show", help="the four checks under one panel_config, newest result each"
    )
    show.add_argument("config", type=int, metavar="CONFIG", help="the panel_config to read")
    show.set_defaults(run=_panel_preflight_show)
    level = commands.add_parser(
        "level",
        help="the autonomy level, and the one deliberate way to move it",
        description="With no argument, the level in force and the progress towards the next "
        "one. With a level, moves to it: an up-move is refused unless the eligibility view "
        "is satisfied and needs --attest, a statement that the move is warranted, which is "
        "recorded with it. Moving down needs neither. The ladder tightens by itself: a "
        "rejection or a gate breach above L0 drops to L0 without anyone asking.",
    )
    level.add_argument("level", nargs="?", choices=("L0", "L1", "L2"), help="the level to move to")
    level.add_argument("--attest", help="why the move is warranted, recorded with it")
    level.add_argument("--actor", **actor)
    level.set_defaults(run=_level)
    mode = commands.add_parser(
        "mode",
        help="paper or live, and the criteria between them",
        description="With no argument, every paper-to-live criterion as met or unmet. With a "
        "mode, switches to it: live is refused until every criterion is met and needs "
        "--attest; paper is always allowed.",
    )
    mode.add_argument("mode", nargs="?", choices=("paper", "live"), help="the mode to trade in")
    mode.add_argument("--attest", help="why going live is warranted, recorded with it")
    mode.add_argument("--actor", **actor)
    mode.set_defaults(run=_mode)
    corporate_commands = commands.add_parser(
        "corporate",
        help="record a forced corporate event, and read the ones a rebalance carries",
        description="The four events the market forces on a holding: a cash acquisition, a "
        "stock-for-stock merger, a delisting and a halt spanning a rebalance. Nothing here is "
        "inferred, because a name that has quietly stopped printing looks exactly like a "
        "halted one. The record is what lets the pre-trade gate accept a basket of fewer than "
        "twenty names and what keeps the snapshot mismatch alert quiet for the share change "
        "the event caused, so record it once the broker's statement or the exchange notice is "
        "in hand.",
    ).add_subparsers(dest="action", required=True)
    corporate_record = corporate_commands.add_parser(
        "record",
        help="record one forced event against the rebalance it bears on",
        description="File the event against the rebalance period it bears on, not the period "
        "it happened in: a delisting in October that the November rebalance has to sell out "
        "of is a November event with an October effective date. A cash acquisition needs its "
        "deal price and a delisting its last listed print; a stock-for-stock merger needs the "
        "acquirer and the shares it paid; a halt settles at no price. A second event of the "
        "same kind for the same name and period is refused.",
    )
    corporate_record.add_argument(
        "kind",
        choices=[str(kind) for kind in ("cash_acquisition", "stock_merger", "delisting", "halt")],
        help="which of the four it is",
    )
    corporate_record.add_argument("symbol", help="the name the event happened to")
    corporate_record.add_argument(
        "--period", required=True, metavar="YYYY-MM", help="the rebalance it bears on"
    )
    corporate_record.add_argument(
        "--effective", required=True, metavar="DATE", help="the session it took effect on"
    )
    corporate_record.add_argument(
        "--shares", required=True, help="the holding the event found, before it"
    )
    corporate_record.add_argument(
        "--detail", required=True, help="where the fact came from, kept for the audit"
    )
    corporate_record.add_argument(
        "--price", help="the deal price, or the last listed print of a delisting"
    )
    corporate_record.add_argument("--into", metavar="SYMBOL", help="the acquirer, for a merger")
    corporate_record.add_argument("--into-shares", help="the acquirer shares the merger paid")
    corporate_record.add_argument("--actor", **actor)
    corporate_record.set_defaults(run=_corporate)
    corporate_show = corporate_commands.add_parser(
        "show", help="the forced events recorded against one rebalance period"
    )
    corporate_show.add_argument("period", metavar="YYYY-MM", help="the rebalance to read")
    corporate_show.set_defaults(run=_corporate)
    paper_checks = commands.add_parser(
        "paper-check",
        help="the paper record's arithmetic checks, three of the paper-to-live criteria",
        description="Measures one check over the settled paper rebalances and records the "
        "result. A check with too little to measure is refused rather than recorded, which "
        "leaves its criterion unmet until paper has produced the evidence.",
    ).add_subparsers(dest="action", required=True)
    for command, name, helptext in (
        ("fill-tolerance", "fill_tolerance", "every paper fill against the official close"),
        ("fee-arithmetic", "fee_arithmetic", "each fill's fees against the schedule"),
        ("partial-fills", "partial_fills", "short fills, and whether each one alerted"),
    ):
        check = paper_checks.add_parser(command, help=helptext)
        check.add_argument("--actor", **actor)
        check.set_defaults(run=_paper_check, check_name=name)
    args = parser.parse_args(argv)
    try:
        return args.run(args)
    except psycopg.Error as exc:
        print(f"agb: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
