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
net return, and the run's own series is one more.

The declared trial count is the ledger's row count, read after the nested trials have
closed and never from anything the run itself says. Whatever the gates decide, the run
leaves an artifact directory, `<research dir>/certifications/<model id>/`, that is never
overwritten:

    manifest.json    every field the `model_certification` row carries, the sha256 of each
                     other file, and the sha256 over those digests
    trials.parquet   the ledger as it stood at certification, so the declared count can be
                     checked against it by anyone holding the artifact
    paths.json       each path's monthly net and gross returns at 1x cost and net at 2x
    report.md        the verdict in prose, stating what the gates certify

A run whose gates fail is recorded exactly like one that passes, with `passed` false, in
the ledger, the artifact and the database: a failed certification is a fact about the
model, not something that never happened. Promotion is a separate step that must require
`passed`.

`import_artifact` is the research profile's one write to the server's database. It runs on
the host with the `research` role's PG* environment (the research container has no network),
verifies every digest and the trial count against the shipped ledger, and inserts the row.
An altered file, an unlisted file, a ledger whose row count disagrees with the declared count
and a model already certified are each refused, and nothing is written.

These gates certify stage 1 of the ranker only, the classical LightGBM ranker. The LLM panel
and the blend are unbacktested by construction, and every output of this module says so."""

import hashlib
import json
import re
import shutil
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import psycopg
from psycopg import errors
from psycopg.types.json import Jsonb

from agb import lake, rebalance
from agb.research import backtest, cpcv, gates, train
from agb.research.panel import Panel
from agb.research.trials import FILE as LEDGER_FILE
from agb.research.trials import Ledger, LedgerError, Run, code_version

KIND = "certification"
CERTIFIES = gates.CERTIFIES
CAPITAL = 100_000.0
DIRECTORY = "certifications"
MANIFEST = "manifest.json"
PATHS = "paths.json"
REPORT = "report.md"
ACTOR = "research"
MODEL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
# The trial kinds whose results carry monthly net returns the overfitting gate can read.
CANDIDATE_KINDS = ("backtest",)
# What the manifest carries, in the order the row and the report show them.
FIELDS = (
    "model_id", "certifies", "passed", "features", "train_start", "train_end",
    "declared_trials", "dsr", "pbo", "sharpe_net", "dsr_2x", "break_even_multiple",
    "sharpe_gross", "gross_sharpe_flag", "participation_max", "participation_mean",
    "cost_model_version", "certified_capital", "path_quantiles", "gates", "certified_at",
)  # fmt: skip


class CertificationError(Exception):
    pass


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
) -> Certification:
    """Fit, trade and gate `config` over the panel, and write the artifact."""
    directory = research_dir / DIRECTORY / options.model_id
    if directory.exists():
        raise CertificationError(f"{directory} exists; a certification is never overwritten")
    months = panel.months
    if not months:
        raise CertificationError("the panel has no months")
    labels = [month.as_of.isoformat() for month in months]
    trading = backtest.Options(
        signal="score",
        size=options.size,
        hold_through=options.hold_through,
        init_cash=CAPITAL,
        cost_multiple=1.0,
    )
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
        fitted = cpcv.run(panel, config, validation, ledger, scores_out=scores)
        series = {str(p): scores[p] for p in sorted(scores)}
        traded = backtest.run(panel, trading, ledger, lake_dir=lake_dir, db=db, scores=series)
        twice = backtest.replay(
            panel, replace(trading, cost_multiple=2.0), series, lake_dir=lake_dir, db=db
        )
        paths = traded.results["paths"]
        net = _matrix(paths, "net")
        gross = _matrix(paths, "gross")
        net_2x = _matrix(twice, "net")
        others = candidates(ledger, panel, exclude={trial.trial_id, traded.trial_id})
        matrix = np.column_stack([net.mean(axis=0), *others.values()])
        declared = ledger.count()
        verdict = gates.evaluate(
            net=net,
            gross=gross,
            net_2x=net_2x,
            candidates=matrix,
            trials=declared,
            months=labels,
            participation=[(p["participation_max"], p["participation_mean"]) for p in paths],
            thresholds=options.thresholds,
        )
        trial.results = {
            **verdict,
            "model_id": options.model_id,
            "certified_capital": CAPITAL,
            "cpcv_trial_id": fitted.trial_id,
            "backtest_trial_id": traded.trial_id,
            "candidate_trials": list(others),
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
        directory, Run(trial.trial_id, trial.results), ledger, config, panel, months, options
    )


def candidates(
    ledger: Ledger, panel: Panel, *, exclude: set[str] = frozenset()
) -> dict[str, np.ndarray]:
    """The monthly net return series of every earlier succeeded backtest over this panel's
    months, partition and cost-model version, keyed by trial id: the configurations the
    overfitting gate ranks this model against. A backtest over several paths contributes the
    paths' mean month."""
    months = panel.months
    found: dict[str, np.ndarray] = {}
    for row in ledger.rows():
        if (
            row["kind"] not in CANDIDATE_KINDS
            or row["status"] != "succeeded"
            or row["trial_id"] in exclude
            or row["partition_hash"] != panel.partition_hash
            or row["cost_model_version"] != panel.cost_model_version
            or row["train_start"] != months[0].as_of
            or row["train_end"] != months[-1].as_of
            or not row["results"]
        ):
            continue
        results = json.loads(row["results"])
        if "paths" in results:
            series = _matrix(results["paths"], "net").mean(axis=0)
        elif "monthly" in results:
            series = np.array([month["net"] for month in results["monthly"]], dtype="float64")
        else:
            continue
        if len(series) == len(months) and np.isfinite(series).all():
            found[row["trial_id"]] = series
    return found


# The artifact ---------------------------------------------------------------------------------


def _write(
    directory: Path,
    run: Run,
    ledger: Ledger,
    config: train.Config,
    panel: Panel,
    months,
    options: Options,
) -> Certification:
    results = run.results
    directory.mkdir(parents=True)
    shutil.copyfile(ledger.path, directory / LEDGER_FILE)
    (directory / PATHS).write_text(json.dumps(results["paths"], indent=1) + "\n")
    manifest = {
        "model_id": options.model_id,
        "certifies": CERTIFIES,
        "passed": results["passed"],
        "features": list(config.features),
        "train_start": months[0].as_of.isoformat(),
        "train_end": months[-1].as_of.isoformat(),
        "declared_trials": results["declared_trials"],
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
        },
        "certified_at": datetime.now(UTC).isoformat(),
        "trial_id": run.trial_id,
        "cpcv_trial_id": results["cpcv_trial_id"],
        "backtest_trial_id": results["backtest_trial_id"],
        "partition_hash": panel.partition_hash,
        "code_version": code_version(),
        "model": config.to_dict(),
    }
    (directory / REPORT).write_text(report(manifest))
    files = {
        path.name: _sha256(path) for path in sorted(directory.iterdir()) if path.name != MANIFEST
    }
    manifest["files"] = files
    manifest["ledger_sha256"] = files[LEDGER_FILE]
    manifest["artifact_sha256"] = artifact_sha256(files)
    (directory / MANIFEST).write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    return Certification(run.trial_id, directory, manifest)


def artifact_sha256(files: dict[str, str]) -> str:
    """One digest over every file's name and digest, in name order."""
    hasher = hashlib.sha256()
    for name in sorted(files):
        hasher.update(f"{name}\0{files[name]}\n".encode())
    return hasher.hexdigest()


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
        f" declared from the ledger shipped as {LEDGER_FILE}.",
        "",
        "## Gates, on returns net of 1x modelled cost",
        "",
    ]
    for name in ("dsr", "pbo", "sharpe_net", "dsr_2x"):
        gate = manifest["gates"][name]
        state = "pass" if gate["passed"] else "FAIL"
        lines.append(
            f"- {name}: {gate['value']:.4f}, {gate['rule']} {gate['threshold']:.4f}: {state}"
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
    for name in ("max_drawdown", "longest_losing_run", "tracking_dispersion"):
        values = manifest["path_quantiles"][name]
        lines.append(
            f"- {name}: "
            + ", ".join(f"{key} {values[key]:.4f}" for key in ("min", "q05", "q50", "q95", "max"))
        )
    return "\n".join(lines) + "\n"


# The import -----------------------------------------------------------------------------------


def load(directory: Path) -> dict[str, Any]:
    """The manifest of the artifact at `directory`, after every check: each listed file is
    present and unaltered, nothing unlisted is there, the digest over the digests matches,
    and the shipped ledger holds exactly the declared number of trials, among them this
    certification's own row saying the same."""
    path = directory / MANIFEST
    if not path.is_file():
        raise CertificationError(f"{directory} holds no {MANIFEST}")
    manifest = json.loads(path.read_text())
    missing = [name for name in (*FIELDS, "files", "ledger_sha256", "artifact_sha256", "trial_id")
               if name not in manifest]  # fmt: skip
    if missing:
        raise CertificationError(f"{MANIFEST} lacks {', '.join(missing)}")
    files = manifest["files"]
    for name, digest in files.items():
        file = directory / name
        if not file.is_file():
            raise CertificationError(f"{name} is missing from {directory}")
        if _sha256(file) != digest:
            raise CertificationError(f"{name} has been altered since certification")
    unlisted = sorted(
        p.name for p in directory.iterdir() if p.name not in files and p.name != MANIFEST
    )
    if unlisted:
        raise CertificationError(f"{directory} holds unlisted files: {', '.join(unlisted)}")
    if files.get(LEDGER_FILE) != manifest["ledger_sha256"]:
        raise CertificationError(f"{LEDGER_FILE} is not the ledger the manifest names")
    if artifact_sha256(files) != manifest["artifact_sha256"]:
        raise CertificationError("the artifact digest does not match its files")
    ledger = Ledger(directory / LEDGER_FILE)
    count = ledger.count()
    if count != manifest["declared_trials"]:
        raise CertificationError(
            f"{LEDGER_FILE} holds {count} trials; the certification declared"
            f" {manifest['declared_trials']}"
        )
    try:
        row = ledger.get(manifest["trial_id"])
    except LedgerError as error:
        raise CertificationError(
            f"the shipped ledger lacks the certification's trial: {error}"
        ) from None
    results = json.loads(row["results"] or "null") or {}
    if row["kind"] != KIND or row["status"] != "succeeded":
        raise CertificationError(f"trial {row['trial_id']} is not a succeeded certification")
    if results.get("declared_trials") != count or results.get("passed") != manifest["passed"]:
        raise CertificationError("the manifest disagrees with the certification's own trial row")
    return manifest


def import_artifact(conn: psycopg.Connection, directory: Path, *, actor: str = ACTOR) -> int:
    """Verify the artifact at `directory` and insert its `model_certification` row, as the
    `research` role. Returns the row's id."""
    manifest = load(directory)
    columns = {
        "model_id": manifest["model_id"],
        "certifies": manifest["certifies"],
        "passed": manifest["passed"],
        "artifact_sha256": manifest["artifact_sha256"],
        "artifact_path": str(directory.resolve()),
        "ledger_sha256": manifest["ledger_sha256"],
        "features": Jsonb(manifest["features"]),
        "train_start": date.fromisoformat(manifest["train_start"]),
        "train_end": date.fromisoformat(manifest["train_end"]),
        "declared_trials": manifest["declared_trials"],
        "dsr": manifest["dsr"],
        "pbo": manifest["pbo"],
        "sharpe_net": manifest["sharpe_net"],
        "dsr_2x": manifest["dsr_2x"],
        "break_even_multiple": manifest["break_even_multiple"],
        "sharpe_gross": manifest["sharpe_gross"],
        "gross_sharpe_flag": manifest["gross_sharpe_flag"],
        "participation_max": manifest["participation_max"],
        "participation_mean": manifest["participation_mean"],
        "cost_model_version": manifest["cost_model_version"],
        "certified_capital": manifest["certified_capital"],
        "path_quantiles": Jsonb(manifest["path_quantiles"]),
        "gates": Jsonb(manifest["gates"]),
        "certified_at": datetime.fromisoformat(manifest["certified_at"]),
        "actor": actor,
    }
    names = ", ".join(columns)
    holes = ", ".join(f"%({name})s" for name in columns)
    try:
        with conn.transaction():
            row = conn.execute(
                f"INSERT INTO agb.model_certification ({names}) VALUES ({holes}) RETURNING id",
                columns,
            ).fetchone()
    except errors.UniqueViolation:
        raise CertificationError(
            f"model {manifest['model_id']!r} is already certified; a certification is never"
            " replaced"
        ) from None
    return row[0]


# Helpers --------------------------------------------------------------------------------------


def _config(options: Options) -> dict[str, Any]:
    return {**asdict(options), "certified_capital": CAPITAL, "certifies": CERTIFIES}


def _matrix(paths: list[dict[str, Any]], key: str) -> np.ndarray:
    return np.array([[month[key] for month in path["monthly"]] for path in paths], dtype="float64")


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
