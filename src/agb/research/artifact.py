"""A certification artifact: what it holds, how it is checked, and the one row it inserts.

`agb.research.certify` runs a certification and writes the artifact. This module is everything
needed to read one back and record it, and it is separate for one reason: the import is the
research profile's only write to the server's database, and it has to run where that database is
reachable. The server's Postgres is on the `core` Compose network and publishes no ports, so the
import runs inside a container on that network, from the app image, which carries the source but
not the research dependency group. `certify` imports `backtest`, `cpcv` and `train`, so it pulls
NautilusTrader, vectorbt and pandas and cannot be imported there at all. Nothing here may import
anything outside the standard library, `psycopg` and `agb.research.trials`, all of which the base
image installs. tests/test_artifact.py holds that line.

An artifact is append-only: `load` refuses one whose files have changed since certification, one
holding a file the manifest does not list, one whose shipped ledger's rows no longer chain, and
one whose shipped ledger disagrees with the trial count the certification declared. The declared
count is recounted off the shipped ledger under the rule the manifest says the certification
counted by, not under the rule in force now, so an artifact certified before the rule changed
still verifies against the number it declared.
`import_artifact` refuses a model that is already certified. A
failed certification imports exactly like a passing one, with `passed` false, because a failed
certification is a fact about the model rather than something that never happened."""

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import errors
from psycopg.types.json import Jsonb

from agb.research.trials import FILE as LEDGER_FILE
from agb.research.trials import Ledger, LedgerError

# The trial kind a certification run records, which `load` checks the shipped row against.
KIND = "certification"
# The role the import runs as. It is the only one granted INSERT on agb.model_certification.
ACTOR = "research"
MANIFEST = "manifest.json"
MODEL = "model.txt"
# What the manifest carries, in the order the row and the report show them.
FIELDS = (
    "model_id", "certifies", "passed", "features", "train_start", "train_end",
    "declared_trials", "dsr", "pbo", "sharpe_net", "dsr_2x", "break_even_multiple",
    "sharpe_gross", "gross_sharpe_flag", "participation_max", "participation_mean",
    "cost_model_version", "certified_capital", "path_quantiles", "gates", "certified_at",
)  # fmt: skip
# Where the certification ran, which the row keeps for provenance. The import records what the
# manifest says rather than where it read the artifact from, because on the server it reads a copy.
CERTIFIED_PATH = "certified_path"


class CertificationError(Exception):
    pass


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def artifact_sha256(files: dict[str, str]) -> str:
    """One digest over every file's name and digest, in name order."""
    hasher = hashlib.sha256()
    for name in sorted(files):
        hasher.update(f"{name}\0{files[name]}\n".encode())
    return hasher.hexdigest()


def load(directory: Path) -> dict[str, Any]:
    """The manifest of the artifact at `directory`, after every check: each listed file is
    present and unaltered, nothing unlisted is there, the digest over the digests matches,
    the booster is among the files, the shipped ledger's rows chain, and recounting it under the
    manifest's own counting rule gives exactly the declared number of trials, among them this
    certification's own row saying the same."""
    path = directory / MANIFEST
    if not path.is_file():
        raise CertificationError(f"{directory} holds no {MANIFEST}")
    manifest = json.loads(path.read_text())
    missing = [name for name in (*FIELDS, "files", "ledger_sha256", "artifact_sha256", "trial_id",
                                 CERTIFIED_PATH)
               if name not in manifest]  # fmt: skip
    if missing:
        raise CertificationError(f"{MANIFEST} lacks {', '.join(missing)}")
    files = manifest["files"]
    for name, digest in files.items():
        file = directory / name
        if not file.is_file():
            raise CertificationError(f"{name} is missing from {directory}")
        if file_sha256(file) != digest:
            raise CertificationError(f"{name} has been altered since certification")
    unlisted = sorted(
        p.name for p in directory.iterdir() if p.name not in files and p.name != MANIFEST
    )
    if unlisted:
        raise CertificationError(f"{directory} holds unlisted files: {', '.join(unlisted)}")
    if MODEL not in files:
        raise CertificationError(f"{directory} holds no {MODEL}")
    if files.get(LEDGER_FILE) != manifest["ledger_sha256"]:
        raise CertificationError(f"{LEDGER_FILE} is not the ledger the manifest names")
    if artifact_sha256(files) != manifest["artifact_sha256"]:
        raise CertificationError("the artifact digest does not match its files")
    ledger = Ledger(directory / LEDGER_FILE)
    try:
        ledger.verify()
    except LedgerError as error:
        raise CertificationError(f"{LEDGER_FILE} cannot be trusted: {error}") from None
    # Certifications up to lgbm-2026-10 declared the ledger's row count and said no rule, so a
    # manifest without one is read as having counted that way and still verifies.
    rule = manifest.get("declared_trials_rule", "rows")
    rows = ledger.count()
    if "ledger_rows" in manifest and rows != manifest["ledger_rows"]:
        raise CertificationError(
            f"{LEDGER_FILE} holds {rows} rows; the certification shipped {manifest['ledger_rows']}"
        )
    try:
        count = ledger.declared(rule=rule)
    except LedgerError as error:
        raise CertificationError(
            f"{MANIFEST} counted its trials by an unknown rule: {error}"
        ) from None
    if count != manifest["declared_trials"]:
        raise CertificationError(
            f"{LEDGER_FILE} holds {count} trials counted as {rule}; the certification declared"
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
        "artifact_path": manifest[CERTIFIED_PATH],
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
