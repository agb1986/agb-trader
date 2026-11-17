"""Certification: the validation gates run over one model, and what a run leaves behind.

A certification run is one trial in the ledger, like a scan or a backtest, and it nests two
more: the combinatorial purged cross-validation that fits and scores the model on every
path, and the validation backtest that trades each path's out-of-sample scores through the
live rebalance code at the certified capital and one times the modelled cost. The paths are
then replayed at twice the cost, which is a constant, not a trial, and the gates in
`agb.research.gates` read the three sets of monthly returns. The overfitting gate also reads
every other backtest the ledger holds over the same panel, because a model is overfit
relative to the configurations that were tried against it: each earlier backtest over the
same months, partition and cost-model version is a candidate, as its paths' mean monthly
net return, and the run's own series is one more. The matrix has one column per
configuration, not per row: a backtest nested inside a certification is that
certification's configuration, a rerun of a configuration that already has a column adds
none, and a run of this certification's own configuration is this run. Every series left
out is reported with its reason, and a configuration that should have had a column and has
none fails the gate, as does a matrix with fewer than two distinct columns, where the
probability of backtest overfitting is undefined.

The declared trial count that the deflated Sharpe ratio charges for is the number of
distinct configurations the ledger says were tried, `trials.RULE`, read off the ledger
after the nested trials have closed and never from anything the run itself says. It is not
the ledger's row count, which is also recorded, as `ledger_rows`. Two things make a row that
is not another configuration tried, and counting them charged the model for work nobody did:
a certification writes three rows, its own and the cross-validation and backtest it nests,
which is one configuration evaluated once; and the same configuration rerun after a crash,
or rechecked by `reproduce`, writes a row every time. So the count folds each row into the
run it names as its parent and takes distinct configurations rather than rows. A
configuration that only ever failed or was abandoned still counts: it was started, and
whoever chose what to certify saw how it went. The run refuses a ledger whose rows no longer
chain, since a row taken out of it is a trial the count would quietly lose. Screening scans
over raw signals are counted, since they are searches whose outcome informed what was
certified; the training window, the panel's partition digest and the cost-model version are
part of a configuration's identity, so rebuilding the panel and trying the same model again
is another try, not the same reading over again.

The count is lower under this rule than under the row count, which lowers the bar the
deflated Sharpe ratio has to clear, and that is a reason to distrust it rather than a
benefit: it is right only to the extent that each row it drops really is one evaluation's
mechanics rather than a search. What the number cannot cover at all, and no ledger rule
could, is choices made without a run: which features exist, which were kept, the
hyperparameters, how the panel and the cost model are built, and how many times any of that
was adjusted after seeing a result. Those are decisions taken by judgement, they leave no
row, and the deflated Sharpe ratio is not charged for them. A pre-registered budget for the
runs a model is allowed, and `ledger_rows` next to the declared count in every manifest, are
what keeps the number honest; the gate's arithmetic cannot. The final model is fitted on the
whole panel inside the certification's own trial, as one more refit of it, so it adds no
row.
Whatever the gates decide, the run leaves an artifact directory,
`<research dir>/certifications/<model id>/`, that is never overwritten:

    manifest.json    every field the `model_certification` row carries, the sha256 of each
                     other file, and the sha256 over those digests
    model.txt        the LightGBM booster fitted on every panel month, which is what the
                     live pipeline ranks with once the certification is promoted
    trials.parquet   the ledger as it stood at certification, so the declared count and the
                     rule it was counted under can be rechecked by anyone holding the artifact
    paths.json       each path's monthly net and gross returns at 1x cost and net at 2x,
                     and the benchmark's return over the same month, or null where the
                     lake has none
    report.md        the verdict in prose, stating what the gates certify

A run whose gates fail is recorded exactly like one that passes, with `passed` false, in
the ledger, the artifact and the database: a failed certification is a fact about the
model, not something that never happened. Promotion is a separate step that must require
`passed`.

The deflated Sharpe gates and the cost-inclusive Sharpe gate read returns in excess of SPY
(issue #167, `gates.EXCESS_DEFINITION`), and the manifest says so as `sharpe_basis`
`excess_spy`, with the total-return Sharpe beside it as `sharpe_total`. The benchmark's return
for a month comes from the validation backtest, marked on the same two sessions as the path's
own. If the lake lacks it for any month of any path, the benchmark is missing for the whole
run: the months are neither dropped nor counted as zero, the three gates fail with the cause
named, and the artifact is still written, because a failed certification is still a record
and the paper-only waiver needs one to waive.

The server does not take the verdict as written: importing the artifact, and loading the model
from the registry, runs the gates again over paths.json and trials.parquet and refuses any
difference (issue #211). The candidate columns, the input matrices and the verdict's fields in
the manifest are `agb.research.recompute`'s, which this module calls rather than copies, so the
certification and the recomputation cannot drift apart. The artifact digest covers the whole
manifest, and the trial count is always `trials.RULE`. Certifications written before either
change, lgbm-2026-10 among them, are refused on import and must be certified again.

Reading an artifact back and inserting its row is `agb.research.artifact`, which is separate
because the import has to run where the server's database is reachable and nothing there may
import this module's dependencies. The manifest carries `certified_path` so the row records
where the certification ran rather than where the artifact was read from.

These gates certify stage 1 of the ranker only, the classical LightGBM ranker. The LLM panel
and the blend are unbacktested by construction, and every output of this module says so."""

import json
import re
import shutil
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from agb import lake, rebalance
from agb.research import backtest, cpcv, gates, recompute, train
from agb.research.artifact import (
    CERTIFIED_PATH,
    KIND,
    MANIFEST,
    MODEL,
    CertificationError,
    artifact_sha256,
    file_sha256,
)
from agb.research.cache import Cache
from agb.research.panel import Panel
from agb.research.recompute import PATHS, Candidates
from agb.research.trials import FILE as LEDGER_FILE
from agb.research.trials import RULE as TRIAL_RULE
from agb.research.trials import UNVERSIONED, Ledger, LedgerError, Run, code_version

CERTIFIES = gates.CERTIFIES
CAPITAL = 100_000.0
DIRECTORY = "certifications"
REPORT = "report.md"
# The one pattern for a model id; scripts/deploy/promote.sh holds the same one for the same reason,
# and tests/test_compose.py checks that the two agree. It is matched against the whole string:
# `$` alone would let a trailing newline through.
MODEL_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


@dataclass(frozen=True)
class Options:
    """What the certification trades and where its gates fall. The basket is the live one,
    twenty names held through rank thirty, unless a test shrinks it."""

    model_id: str
    size: int = rebalance.SIZE
    hold_through: int = rebalance.HOLD_THROUGH
    thresholds: gates.Thresholds = field(default_factory=gates.Thresholds)

    def __post_init__(self) -> None:
        if not MODEL_ID.fullmatch(self.model_id):
            raise ValueError(f"model id {self.model_id!r}: letters, digits, dot, dash, underscore")


@dataclass(frozen=True)
class Certification:
    """A finished run: its ledger row, its artifact directory and the manifest inside it."""

    trial_id: str
    directory: Path
    manifest: dict[str, Any]

    @property
    def model_id(self) -> str:
        return self.manifest["model_id"]

    @property
    def passed(self) -> bool:
        return self.manifest["passed"]


def run(
    panel: Panel,
    config: train.Config,
    validation: cpcv.Options,
    options: Options,
    ledger: Ledger,
    *,
    research_dir: Path,
    lake_dir: Path = lake.DEFAULT_DIR,
    db: duckdb.DuckDBPyConnection | None = None,
    checkpoint: bool = True,
) -> Certification:
    """Fit, trade and gate `config` over the panel, and write the artifact.

    `checkpoint` keeps the cross-validation's scores and every path's backtest under the
    research directory, so an attempt that fell over, or one rerun over the same panel and
    configuration, reuses them rather than spending another day computing them again. The
    cache is keyed by everything that determines what it holds, so a hit is the same work and
    not merely a likely one, and a reused stage hands back the trial id of the run that filled
    it rather than appending a row, which is what keeps the verdict identical; see
    `agb.research.cache`. Turning it off makes the run recompute everything.
    """
    # The code version keys the cache and is recorded in the manifest, and `dev` names no
    # build: an entry written by one checkout would be served to another, and the artifact
    # could not say what code certified it.
    if code_version() == UNVERSIONED:
        raise CertificationError(
            f"the code version is {UNVERSIONED!r}, which names no build; set AGB_VERSION to the"
            " commit, as `make research` does"
        )
    directory = research_dir / DIRECTORY / options.model_id
    if directory.exists():
        raise CertificationError(f"{directory} exists; a certification is never overwritten")
    months = panel.months
    if not months:
        raise CertificationError("the panel has no months")
    # A ledger a row was taken out of declares a count it cannot vouch for, so refuse before
    # the run adds rows of its own to it.
    try:
        ledger.verify()
    except LedgerError as error:
        raise CertificationError(f"the trial ledger cannot be trusted: {error}") from None
    trading = backtest.Options(
        signal="score",
        size=options.size,
        hold_through=options.hold_through,
        init_cash=CAPITAL,
        cost_multiple=1.0,
    )
    # After the overwrite guard, so a refused certification writes nothing at all, and
    # outside the trial, so the cache directory is not a side effect of an open row.
    cache = Cache.under(research_dir) if checkpoint else None
    with ledger.trial(
        KIND,
        {KIND: _config(options), "model": config.to_dict(), "cpcv": asdict(validation)},
        features=config.features,
        train_start=months[0].as_of,
        train_end=months[-1].as_of,
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
    ) as trial:
        scores: dict[int, pd.Series] = {}
        fitted = cpcv.run(panel, config, validation, ledger, scores_out=scores, cache=cache)
        series = {str(p): scores[p] for p in sorted(scores)}
        traded = backtest.run(
            panel, trading, ledger, lake_dir=lake_dir, db=db, scores=series, cache=cache
        )
        twice = backtest.replay(
            panel,
            replace(trading, cost_multiple=2.0),
            series,
            lake_dir=lake_dir,
            db=db,
            cache=cache,
        )
        paths = traded.results["paths"]
        # The backtest leaves out the leading months the lake cannot price, so the window the
        # gates judge is the one it traded rather than every month the model was fitted over.
        # The trial's own window stays the panel's, which is what was fitted.
        labels = [month["as_of"] for month in paths[0]["monthly"]]
        net = recompute.matrix(paths, "net")
        gross = recompute.matrix(paths, "gross")
        net_2x = recompute.matrix(twice, "net")
        benchmark, benchmark_missing = recompute.benchmark(paths)
        others = candidates(
            ledger,
            panel,
            labels=labels,
            exclude={trial.trial_id, traded.trial_id},
            current=trial.trial_id,
        )
        matrix = np.column_stack([net.mean(axis=0), *others.series.values()])
        # `current` is this run's own row, which is still `running`: it is open until the run
        # ends, so what it nests is read as nested, and it counts itself.
        declared = ledger.declared(current=trial.trial_id)
        rows = ledger.count()
        verdict = gates.evaluate(
            net=net,
            gross=gross,
            net_2x=net_2x,
            candidates=matrix,
            trials=declared,
            months=labels,
            participation=[(p["participation_max"], p["participation_mean"]) for p in paths],
            thresholds=options.thresholds,
            missing=others.missing,
            benchmark=benchmark,
            benchmark_missing=benchmark_missing,
        )
        # What would trade: the configuration refitted on every month of the panel, the
        # leading ones the backtest could not trade included, since they still carry a label.
        booster = train.fit(panel.select(months), config, trial=trial)
        trial.results = {
            **verdict,
            "declared_trials_rule": TRIAL_RULE,
            "ledger_rows": rows,
            "model_id": options.model_id,
            "certified_capital": CAPITAL,
            "cpcv_trial_id": fitted.trial_id,
            "backtest_trial_id": traded.trial_id,
            "candidate_trials": list(others.series),
            "candidates_dropped": others.dropped,
            "cpcv": fitted.results["summary"],
            "backtest": traded.results["summary"],
            "paths": [
                {
                    "path": path["path"],
                    "monthly": [
                        {**month, "net_2x": other["net"]}
                        for month, other in zip(path["monthly"], again["monthly"], strict=True)
                    ],
                }
                for path, again in zip(paths, twice, strict=True)
            ],
        }
    return _write(
        directory,
        Run(trial.trial_id, trial.results),
        ledger,
        config,
        panel,
        months,
        options,
        booster,
    )


def candidates(
    ledger: Ledger,
    panel: Panel,
    *,
    labels: Sequence[str],
    exclude: set[str] = frozenset(),
    current: str | None = None,
) -> Candidates:
    """The overfitting gate's columns for a certification over `panel`, from the rows of
    `ledger`; see `recompute.candidates`, which the server runs over the shipped ledger to check
    the same columns."""
    return recompute.candidates(
        ledger.rows(),
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
        start=panel.months[0].as_of,
        end=panel.months[-1].as_of,
        labels=labels,
        exclude=exclude,
        current=current,
    )


# The artifact ---------------------------------------------------------------------------------


def _write(
    directory: Path,
    run: Run,
    ledger: Ledger,
    config: train.Config,
    panel: Panel,
    months,
    options: Options,
    booster,
) -> Certification:
    results = run.results
    directory.mkdir(parents=True)
    shutil.copyfile(ledger.path, directory / LEDGER_FILE)
    booster.save_model(str(directory / MODEL))
    # Sorted, as the manifest is: a digest over this file should say what the paths are and
    # not what order a dict happened to be built in, so that a run served from the
    # certification cache -- whose payloads come back through canonical JSON -- writes the
    # same bytes as the run that filled it.
    (directory / PATHS).write_text(json.dumps(results["paths"], indent=1, sort_keys=True) + "\n")
    manifest = {
        "model_id": options.model_id,
        "certifies": CERTIFIES,
        "features": list(config.features),
        "train_start": months[0].as_of.isoformat(),
        "train_end": months[-1].as_of.isoformat(),
        "declared_trials_rule": results["declared_trials_rule"],
        "ledger_rows": results["ledger_rows"],
        "cost_model_version": panel.cost_model_version,
        "certified_capital": CAPITAL,
        # The verdict, in exactly the fields the server recomputes and compares.
        **recompute.recorded(
            results, paths=len(results["paths"]), dropped=results["candidates_dropped"]
        ),
        "certified_at": datetime.now(UTC).isoformat(),
        CERTIFIED_PATH: str(directory.resolve()),
        "trial_id": run.trial_id,
        "cpcv_trial_id": results["cpcv_trial_id"],
        "backtest_trial_id": results["backtest_trial_id"],
        "partition_hash": panel.partition_hash,
        "code_version": code_version(),
        "model": config.to_dict(),
    }
    (directory / REPORT).write_text(report(manifest))
    files = {
        path.name: file_sha256(path)
        for path in sorted(directory.iterdir())
        if path.name != MANIFEST
    }
    manifest["files"] = files
    manifest["ledger_sha256"] = files[LEDGER_FILE]
    # Through JSON first, so the digest is over what a reader of the file will parse.
    manifest = json.loads(json.dumps(manifest))
    manifest["artifact_sha256"] = artifact_sha256(manifest)
    (directory / MANIFEST).write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    return Certification(run.trial_id, directory, manifest)


def report(manifest: dict[str, Any]) -> str:
    """The verdict in prose."""
    verdict = "PASSED" if manifest["passed"] else "FAILED"
    lines = [
        f"# Certification of {manifest['model_id']}: {verdict}",
        "",
        f"These gates certify {manifest['certifies']}. The LLM panel and the blend that ride",
        "on top of the ranker are unbacktested by construction, and nothing here is evidence",
        "that they work.",
        "",
        f"Trained on {len(manifest['features'])} features from {manifest['train_start']} to"
        f" {manifest['train_end']}, cost model {manifest['cost_model_version']}, certified"
        f" capital {manifest['certified_capital']:,.0f}, {manifest['declared_trials']} trials"
        f" declared from the {manifest['ledger_rows']} rows of the ledger shipped as"
        f" {LEDGER_FILE}, counted as {manifest['declared_trials_rule']}.",
        "",
        "## Gates, on returns net of 1x modelled cost",
        "",
        *_basis(manifest),
        "",
    ]
    for name in ("dsr", "pbo", "sharpe_net", "dsr_2x"):
        gate = manifest["gates"][name]
        state = "pass" if gate["passed"] else "FAIL"
        cause = f" ({gate['cause']})" if gate.get("cause") else ""
        value = "undefined" if gate.get("defined") is False else f"{gate['value']:.4f}"
        lines.append(f"- {name}: {value}, {gate['rule']} {gate['threshold']:.4f}: {state}" + cause)
    multiple = manifest["break_even_multiple"]
    total = manifest.get("sharpe_total", manifest["sharpe_net"])
    lines += [
        "",
        "## Diagnostics",
        "",
        f"- gross Sharpe {manifest['sharpe_gross']:.4f}, total-return Sharpe {total:.4f},"
        f" suspicion flag {'raised' if manifest['gross_sharpe_flag'] else 'clear'}",
        f"- break-even cost multiple {'none charged' if multiple is None else f'{multiple:.2f}'}",
        f"- participation max {manifest['participation_max']:.4f},"
        f" mean {manifest['participation_mean']:.4f}",
        f"- start dates {'stable' if manifest['gates']['start_date_stable'] else 'unstable'}: "
        + ", ".join(
            f"{start['start']} "
            + ("undefined" if start["sharpe_net"] is None else f"{start['sharpe_net']:.3f}")
            for start in manifest["gates"]["start_dates"]
        ),
        "",
        "## Path distribution",
        "",
    ]
    for name in ("max_drawdown", "longest_losing_run", "tracking_dispersion", "monthly_return"):
        values = manifest["path_quantiles"][name]
        lines.append(
            f"- {name}: "
            + ", ".join(f"{key} {values[key]:.4f}" for key in ("min", "q05", "q50", "q95", "max"))
        )
    return "\n".join(lines) + "\n"


# Helpers --------------------------------------------------------------------------------------


def _config(options: Options) -> dict[str, Any]:
    return {**asdict(options), "certified_capital": CAPITAL, "certifies": CERTIFIES}


def _basis(manifest: dict[str, Any]) -> list[str]:
    """What the Sharpe gates were measured on, in the report's words."""
    if manifest.get("sharpe_basis", gates.TOTAL) == gates.TOTAL:
        return ["The deflated Sharpe and Sharpe gates read total returns."]
    excess = manifest["excess"]
    lines = [
        f"The deflated Sharpe and Sharpe gates read returns in excess of"
        f" {excess['benchmark']}: the {excess['definition']}. The overfitting gate ranks"
        " the candidates on total returns.",
    ]
    if not excess["defined"]:
        lines += [
            "",
            f"Those three gates fail ({excess['cause']})."
            f" They record {gates.UNDEFINED_SHARPE:.1f} for the Sharpe and"
            f" {gates.UNDEFINED_DSR:.1f} for each DSR so the row holds a number, and the"
            " number is not a measurement.",
        ]
    return lines
