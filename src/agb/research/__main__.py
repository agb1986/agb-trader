"""`python -m agb.research`: the research profile's commands.

    sync         pull the server's lake, read-only, over the operator's SSH
    panel        build the dataset from the lake and save it under the research directory
    cpcv         combinatorial purged cross-validation of a LambdaRank configuration
    walkforward  the annual expanding-window test of a configuration
    scan         a vectorbt scan of one signal, costed with the live cost model
    backtest     the NautilusTrader validation backtest of one signal, checking the scan
    certify      run the validation gates over a configuration and write the artifact;
                 exits 1 when the gates fail, after writing it
    import       verify a certification artifact and insert its model_certification row
    trials       list the trial ledger
    reproduce    rerun a trial from its row, under the code version it ran, and compare
                 the results; a certification, or a row nested in one, too. Exits 1
                 when they differ.

A command given an option it cannot run with exits 2 with one line on stderr, as a usage
error does. Everything reads and writes the research directory AGB_RESEARCH_DIR names, which
`make research` and the research container set; it has no default.

`sync` needs nothing but the standard library, so it runs on the host outside the image.

`import` is the profile's one write to the server's database, and the server's Postgres is
reachable only from the `core` Compose network, so it runs on the server as
`./agb-stack import <model id>`, which is the same code: `agb import-certification` and this
command both call `agb.research.artifact.import_artifact`. Running it here needs a reachable
database and the `research` role's PG* environment, which the workstation has for its own
database only. `make promote MODEL=<model id>` does the whole handoff from the workstation:
it ships the artifact, runs that import on the server and promotes the model there."""

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import NoReturn

from agb import lake, rebalance


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m agb.research", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    p = commands.add_parser("sync", help="pull the server's lake, read-only")
    p.add_argument("--env-file", type=Path, default=Path("env/research.env"))
    p.add_argument("--lake", type=Path, default=_lake_dir())
    p.add_argument("--dry-run", action="store_true", help="list what would transfer")

    p = commands.add_parser("panel", help="build the dataset and save it")
    p.add_argument("--start", type=date.fromisoformat, default=lake.SINCE)
    p.add_argument("--end", type=date.fromisoformat, default=None)
    p.add_argument("--out", type=Path, default=None)

    for name, help_ in (
        ("cpcv", "combinatorial purged cross-validation"),
        ("walkforward", "the annual expanding-window test"),
    ):
        p = commands.add_parser(name, help=help_)
        _panel_argument(p)
        _model_arguments(p)
        if name == "cpcv":
            p.add_argument("--groups", type=int, default=16)
            p.add_argument("--test-groups", type=int, default=2)
            p.add_argument("--embargo-months", type=int, default=1)
        else:
            p.add_argument("--first-test-year", type=int, default=None)
            p.add_argument("--last-test-year", type=int, default=None)

    p = commands.add_parser("scan", help="a vectorbt scan of one signal")
    _panel_argument(p)
    p.add_argument("--signal", default="mom_12_1")
    p.add_argument("--size", type=int, default=rebalance.SIZE)
    p.add_argument("--hold-through", type=int, default=rebalance.HOLD_THROUGH)
    p.add_argument("--cash", type=float, default=1_000_000.0)

    p = commands.add_parser("backtest", help="the NautilusTrader validation backtest")
    _panel_argument(p)
    p.add_argument("--signal", default="mom_12_1")
    p.add_argument("--size", type=int, default=rebalance.SIZE)
    p.add_argument("--hold-through", type=int, default=rebalance.HOLD_THROUGH)
    p.add_argument("--cash", type=float, default=1_000_000.0)
    p.add_argument("--cost-multiple", type=float, default=1.0, help="scale the cost model")
    p.add_argument(
        "--start",
        action="append",
        default=[],
        metavar="DATE",
        help="also run from the first month on or after DATE; repeatable",
    )

    p = commands.add_parser("certify", help="the validation gates, and the artifact")
    _panel_argument(p)
    _model_arguments(p)
    p.add_argument("--model-id", required=True, help="the artifact's name; never reused")
    p.add_argument("--groups", type=int, default=16)
    p.add_argument("--test-groups", type=int, default=2)
    p.add_argument("--embargo-months", type=int, default=1)
    p.add_argument("--size", type=int, default=rebalance.SIZE)
    p.add_argument("--hold-through", type=int, default=rebalance.HOLD_THROUGH)

    p = commands.add_parser("import", help="insert a certification artifact's row")
    p.add_argument("directory", type=Path, help="the artifact directory")

    commands.add_parser("trials", help="list the trial ledger")

    p = commands.add_parser("reproduce", help="rerun a trial and compare")
    p.add_argument("trial_id")
    _panel_argument(p)

    args = parser.parse_args(argv)
    if args.command == "sync":
        from agb.research import sync

        try:
            sync.run(args.env_file, args.lake, dry_run=args.dry_run)
        except sync.SyncError as error:
            sys.exit(f"sync: {error}")
        return
    if args.command == "panel":
        _build_panel(args.start, args.end, args.out or _research_dir() / "panel.parquet")
        return
    if args.command == "trials":
        _list_trials()
        return
    if args.command == "import":
        _import(args.directory)
        return
    run = _run(args)
    print(f"trial {run.trial_id}")
    print(json.dumps(run.results, indent=2, sort_keys=True))
    # A failed certification is still written, because it is a record, but the command failed:
    # a script or an operator reading the status must not take the artifact for a promotable one.
    if args.command == "certify" and not run.results["passed"]:
        sys.exit(1)


def _run(args):
    from agb.research import backtest, cpcv, reproduce, scan, train, walkforward
    from agb.research.trials import FILE, Ledger

    ledger = Ledger(_research_dir() / FILE)
    panel = _panel(args.panel)
    if args.command == "reproduce":
        try:
            result = reproduce.reproduce(ledger, args.trial_id, panel, lake_dir=_lake_dir())
        except reproduce.ReproduceError as error:
            sys.exit(f"reproduce: {error}")
        except scan.ScanError as error:
            _usage_error("reproduce", error)
        verdict = "match" if result.matches else "DIFFER"
        print(f"trial {args.trial_id} rerun as {result.rerun.trial_id}: results {verdict}")
        for line in result.differences[:_SHOWN]:
            print(f"  {line}")
        if len(result.differences) > _SHOWN:
            print(f"  and {len(result.differences) - _SHOWN} more")
        sys.exit(0 if result.matches else 1)
    if args.command == "scan":
        options = scan.Options(
            signal=args.signal,
            size=args.size,
            hold_through=args.hold_through,
            init_cash=args.cash,
        )
        try:
            return scan.run(panel, options, ledger, lake_dir=_lake_dir())
        except scan.ScanError as error:
            _usage_error("scan", error)
    if args.command == "backtest":
        options = backtest.Options(
            signal=args.signal,
            size=args.size,
            hold_through=args.hold_through,
            init_cash=args.cash,
            cost_multiple=args.cost_multiple,
            starts=tuple(args.start),
        )
        try:
            return backtest.run(panel, options, ledger, lake_dir=_lake_dir())
        except backtest.BacktestError as error:
            sys.exit(f"backtest: {error}")
        except scan.ScanError as error:
            _usage_error("backtest", error)
    config = _config(args, train)
    if args.command == "cpcv":
        options = _validation(args, cpcv)
        # The cache keys on the code version, so a run with no version (`dev`) neither reads
        # nor fills it. A versioned run fills the entry a certification of the same
        # configuration over the same panel reads, which then refits only the shipped model.
        from agb.research.cache import Cache
        from agb.research.trials import UNVERSIONED, code_version

        cache = Cache.under(_research_dir()) if code_version() != UNVERSIONED else None
        return cpcv.run(panel, config, options, ledger, cache=cache)
    if args.command == "certify":
        from agb.research import certify

        validation = _validation(args, cpcv)
        try:
            options = certify.Options(
                model_id=args.model_id, size=args.size, hold_through=args.hold_through
            )
            certification = certify.run(
                panel,
                config,
                validation,
                options,
                ledger,
                research_dir=_research_dir(),
                lake_dir=_lake_dir(),
            )
        except (ValueError, certify.CertificationError, backtest.BacktestError) as error:
            sys.exit(f"certify: {error}")
        except scan.ScanError as error:
            _usage_error("certify", error)
        verdict = "PASSED" if certification.passed else "FAILED"
        print(f"certification {verdict}: {certify.CERTIFIES}; artifact {certification.directory}")
        from agb.research.trials import Run

        return Run(certification.trial_id, certification.manifest)
    options = walkforward.Options(
        first_test_year=args.first_test_year, last_test_year=args.last_test_year
    )
    return walkforward.run(panel, config, options, ledger)


# What libpq needs before a connection can go anywhere but a local socket. The research
# service sets none of them, and its uid has no local account either, so an unguarded connect
# fails as `local user with ID 1000 does not exist`, which describes a Unix account rather
# than the real problem.
_DATABASE_ENV = ("PGHOST", "PGHOSTADDR", "PGSERVICE", "PGDATABASE", "DATABASE_URL")


def _import(directory: Path) -> None:
    import psycopg

    from agb.research import artifact

    if not os.environ.keys() & set(_DATABASE_ENV):
        sys.exit(
            "import: no database is configured here, and the research service runs with"
            " network_mode: none, so it can never reach one; the import runs on the server as"
            f" `./agb-stack import {directory.name}`, or `make promote MODEL={directory.name}`"
            " from the workstation, which ships the artifact and runs it there"
        )
    try:
        with psycopg.connect() as conn:
            row_id = artifact.import_artifact(conn, directory)
    except artifact.CertificationError as error:
        sys.exit(f"import: {error}")
    except psycopg.OperationalError as error:
        sys.exit(
            f"import: the database this profile is pointed at refused the connection: {error}"
            f" The import runs on the server as `./agb-stack import {directory.name}`."
        )
    print(f"model_certification {row_id}: {directory}")


def _panel_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--panel",
        type=Path,
        default=None,
        help="a saved panel; the default is the research directory's, built if absent",
    )


# How many of a reproduction's differences are printed; the rest are counted.
_SHOWN = 20


def _usage_error(command: str, error: BaseException | str) -> NoReturn:
    """Exit 2, as argparse does for an option it refuses, with one line on stderr and no
    traceback: the command was asked for something it cannot do, and nothing ran."""
    print(f"{command}: {' '.join(str(error).split())}", file=sys.stderr)
    sys.exit(2)


def _validation(args, cpcv):
    """The cross-validation options, or a usage error when they cannot split the panel."""
    try:
        return cpcv.Options(
            groups=args.groups, test_groups=args.test_groups, embargo_months=args.embargo_months
        )
    except ValueError as error:
        _usage_error(args.command, error)


def _model_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--features", default=None, help="comma-separated subset of features")
    parser.add_argument("--rounds", type=int, default=None)
    parser.add_argument("--learning-rate", type=float, default=None)
    parser.add_argument("--leaves", type=int, default=None)
    parser.add_argument("--min-leaf", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)


def _config(args, train):
    values = {}
    if args.rounds is not None:
        values["num_boost_round"] = args.rounds
    if args.learning_rate is not None:
        values["learning_rate"] = args.learning_rate
    if args.leaves is not None:
        values["num_leaves"] = args.leaves
    if args.min_leaf is not None:
        values["min_data_in_leaf"] = args.min_leaf
    if args.seed is not None:
        values["seed"] = args.seed
    config = train.Config(**values)
    if args.features:
        config = config.with_features(tuple(name.strip() for name in args.features.split(",")))
    return config


def _panel(path: Path | None):
    from agb.research.panel import Panel

    path = path or _research_dir() / "panel.parquet"
    if not path.exists():
        # Building one reads the whole lake and takes a while; say so, rather than leave the
        # command silent until it is done.
        print(
            f"panel: none saved at {path}; building it from the lake at {_lake_dir()}",
            file=sys.stderr,
            flush=True,
        )
        _build_panel(lake.SINCE, None, path)
    return Panel.load(path)


def _build_panel(start: date, end: date | None, out: Path) -> None:
    from agb.research import panel

    built = panel.build(_lake_dir(), start, end)
    built.save(out)
    months = built.months
    span = f"{months[0].as_of} to {months[-1].as_of}" if months else "no months"
    print(f"panel: {len(built.frame)} rows, {len(months)} months, {span}, saved to {out}")


def _list_trials() -> None:
    from agb.research.trials import FILE, Ledger

    rows = Ledger(_research_dir() / FILE).rows()
    if not rows:
        print("no trials yet")
        return
    for row in rows:
        started = row["started_at"].strftime("%Y-%m-%d %H:%M")
        mark = f" reproduces={row['reproduces']}" if row.get("reproduces") else ""
        print(
            f"{row['trial_id']}  {started}  {row['kind']:<12} {row['status']:<10}"
            f" refits={row['refits']:<4} partitions={row['partition_hash'][:12]}{mark}"
        )


def _lake_dir() -> Path:
    return Path(os.environ.get("AGB_LAKE_DIR", str(lake.DEFAULT_DIR)))


def _research_dir() -> Path:
    """The research directory AGB_RESEARCH_DIR names. There is no default: `/research` is the
    container's mount and nothing on the host, and a ledger quietly started somewhere else is
    a ledger whose trials no certification counts."""
    value = os.environ.get("AGB_RESEARCH_DIR")
    if not value:
        _usage_error(
            "research",
            "AGB_RESEARCH_DIR is not set; `make research` and the research container set it to"
            " the research directory, which holds the trial ledger and the panel",
        )
    return Path(value)


if __name__ == "__main__":
    main()
