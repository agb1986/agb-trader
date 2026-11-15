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
    paths.json       each path's monthly net and gross returns at 1x cost and net at 2x
    report.md        the verdict in prose, stating what the gates certify

A run whose gates fail is recorded exactly like one that passes, with `passed` false, in
the ledger, the artifact and the database: a failed certification is a fact about the
model, not something that never happened. Promotion is a separate step that must require
`passed`.

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
from agb.research import backtest, cpcv, gates, train
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
from agb.research.trials import FILE as LEDGER_FILE
from agb.research.trials import RULE as TRIAL_RULE
from agb.research.trials import Ledger, LedgerError, Run, code_version, configuration, roots

CERTIFIES = gates.CERTIFIES
CAPITAL = 100_000.0
DIRECTORY = "certifications"
PATHS = "paths.json"
REPORT = "report.md"
MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
# The trial kinds whose results carry monthly net returns the overfitting gate can read.
CANDIDATE_KINDS = ("backtest",)


@dataclass(frozen=True)
class Options:
    """What the certification trades and where its gates fall. The basket is the live one,
    twenty names held through rank thirty, unless a test shrinks it."""

    model_id: str
    size: int = rebalance.SIZE
    hold_through: int = rebalance.HOLD_THROUGH
    thresholds: gates.Thresholds = field(default_factory=gates.Thresholds)

    def __post_init__(self) -> None:
        if not MODEL_ID.match(self.model_id):
            raise ValueError(f"model id {self.model_id!r}: letters, digits, dot, dash, underscore")


@dataclass(frozen=True)
class Candidates:
    """The overfitting gate's columns, one per configuration and keyed by the trial id of the
    row that supplied it, what was left out and why, and which of those leave a declared
    configuration without the column it should have, which fails the gate."""

    series: dict[str, np.ndarray]
    dropped: list[dict[str, str]]
    missing: list[str]


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
        net = _matrix(paths, "net")
        gross = _matrix(paths, "gross")
        net_2x = _matrix(twice, "net")
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
    """The monthly net return series the overfitting gate ranks this model against, one per
    configuration among the succeeded backtests over this panel's months, partition and
    cost-model version, keyed by the trial id of the run that supplied it. A backtest over
    several paths contributes the paths' mean month.

    A configuration is what `trials.configuration` says of the run at the root of a backtest's
    `parent` links (`trials.roots`; by interval for rows from before the chain): every
    certification trades its scores through a backtest whose own configuration is the same
    whatever the model, so the backtest's row alone cannot tell two models apart,
    and the certification around it can. `current` is the certification asking, and anything
    of its configuration is this run, which is the matrix's first column already; `exclude`
    names rows to leave out besides. The first usable series of a configuration is its column
    and later runs of it are reruns, which add none.

    A backtest's recorded window is the panel it was handed, while `labels` is the shorter
    window it could trade, so the series a candidate contributes is held to the traded length:
    two runs whose months differ are not two readings of the same thing. Every series left out
    is in `dropped` with its reason, and a configuration none of whose runs left a usable
    series is in `missing`, which fails the gate: it was tried, and a PBO without it is not
    the PBO of the search."""
    rows = ledger.rows()
    top = roots(rows, current=current)
    mine = next((configuration(r) for r in rows if r["trial_id"] == current), None)
    start, end = panel.months[0].as_of, panel.months[-1].as_of
    series: dict[str, np.ndarray] = {}
    columns: dict[tuple[object, ...], str] = {}
    unusable: dict[tuple[object, ...], list[str]] = {}
    dropped: list[dict[str, str]] = []

    def drop(row: dict[str, Any], reason: str) -> None:
        dropped.append({"trial_id": str(row["trial_id"]), "reason": reason})

    for row in rows:
        if row["kind"] not in CANDIDATE_KINDS or row["trial_id"] in exclude:
            continue
        if row["status"] != "succeeded":
            drop(row, f"status {row['status']}")
            continue
        if (
            row["partition_hash"] != panel.partition_hash
            or row["cost_model_version"] != panel.cost_model_version
            or row["train_start"] != start
            or row["train_end"] != end
        ):
            drop(row, "another panel, window or cost model")
            continue
        identity = configuration(top[str(row["trial_id"])])
        if identity == mine:
            drop(row, "this certification's own configuration")
            continue
        if identity in columns:
            drop(row, f"rerun of the configuration of {columns[identity]}")
            continue
        found, reason = _series(row, labels)
        if found is None:
            drop(row, reason)
            unusable.setdefault(identity, []).append(f"{row['trial_id']}: {reason}")
            continue
        columns[identity] = str(row["trial_id"])
        series[str(row["trial_id"])] = found
    missing = [
        "; ".join(reasons) for identity, reasons in unusable.items() if identity not in columns
    ]
    return Candidates(series, dropped, missing)


def _series(row: dict[str, Any], labels: Sequence[str]) -> tuple[np.ndarray | None, str]:
    """A candidate row's mean monthly net return series, or none and why it cannot be read."""
    if not row["results"]:
        return None, "no results"
    results = json.loads(row["results"])
    if "paths" in results:
        found = _matrix(results["paths"], "net").mean(axis=0)
    elif "monthly" in results:
        found = np.array([month["net"] for month in results["monthly"]], dtype="float64")
    else:
        return None, "no monthly returns"
    if len(found) != len(labels):
        return None, f"{len(found)} months, {len(labels)} traded"
    if not np.isfinite(found).all():
        return None, "a month that is not finite"
    return found, ""


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
        "passed": results["passed"],
        "features": list(config.features),
        "train_start": months[0].as_of.isoformat(),
        "train_end": months[-1].as_of.isoformat(),
        "declared_trials": results["declared_trials"],
        "declared_trials_rule": results["declared_trials_rule"],
        "ledger_rows": results["ledger_rows"],
        "dsr": results["dsr"],
        "pbo": results["pbo"],
        "sharpe_net": results["sharpe_net"],
        "dsr_2x": results["dsr_2x"],
        "break_even_multiple": results["break_even_multiple"],
        "sharpe_gross": results["sharpe_gross"],
        "gross_sharpe_flag": results["gross_sharpe_flag"],
        "participation_max": results["participation_max"],
        "participation_mean": results["participation_mean"],
        "cost_model_version": panel.cost_model_version,
        "certified_capital": CAPITAL,
        "path_quantiles": results["path_quantiles"],
        "gates": {
            **results["gates"],
            "thresholds": results["thresholds"],
            "start_dates": results["start_dates"],
            "start_date_stable": results["start_date_stable"],
            "paths": len(results["paths"]),
            "months": results["months"],
            "candidates": results["overfitting"]["candidates"],
            "candidates_dropped": results["candidates_dropped"],
        },
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
    manifest["artifact_sha256"] = artifact_sha256(files)
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
    ]
    for name in ("dsr", "pbo", "sharpe_net", "dsr_2x"):
        gate = manifest["gates"][name]
        state = "pass" if gate["passed"] else "FAIL"
        cause = f" ({gate['cause']})" if gate.get("cause") else ""
        lines.append(
            f"- {name}: {gate['value']:.4f}, {gate['rule']} {gate['threshold']:.4f}: {state}"
            + cause
        )
    multiple = manifest["break_even_multiple"]
    lines += [
        "",
        "## Diagnostics",
        "",
        f"- gross Sharpe {manifest['sharpe_gross']:.4f}, net Sharpe {manifest['sharpe_net']:.4f},"
        f" suspicion flag {'raised' if manifest['gross_sharpe_flag'] else 'clear'}",
        f"- break-even cost multiple {'none charged' if multiple is None else f'{multiple:.2f}'}",
        f"- participation max {manifest['participation_max']:.4f},"
        f" mean {manifest['participation_mean']:.4f}",
        f"- start dates {'stable' if manifest['gates']['start_date_stable'] else 'unstable'}: "
        + ", ".join(
            f"{start['start']} {start['sharpe_net']:.3f}"
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


def _matrix(paths: list[dict[str, Any]], key: str) -> np.ndarray:
    return np.array([[month[key] for month in path["monthly"]] for path in paths], dtype="float64")
