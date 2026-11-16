"""A certification artifact: what it holds, how it is checked, and the one row it inserts.

`agb.research.certify` runs a certification and writes the artifact. This module is everything
needed to read one back and record it, and it is separate for one reason: the import is the
research profile's only write to the server's database, and it has to run where that database is
reachable. The server's Postgres is on the `core` Compose network and publishes no ports, so the
import runs inside a container on that network, from the app image, which carries the source but
not the research dependency group. `certify` imports `backtest`, `cpcv` and `train`, so it pulls
NautilusTrader, vectorbt and pandas and cannot be imported there at all. Nothing here may import
anything outside the standard library, `psycopg`, numpy and `agb.research.trials`, `gates` and
`recompute`, all of which the app image installs. tests/test_artifact.py holds that line.

An artifact is append-only, and the server takes nothing in it on trust that it can check
(issue #211). `load` refuses one whose files or manifest have changed since certification, one
holding a file the manifest does not list, one whose shipped ledger's rows no longer chain, one
that counted its trials by any rule but `trials.RULE`, and one whose verdict is not the verdict
the gates give when they are run again, here, over the paths and the ledger it ships; see
`agb.research.recompute` for what that does and does not catch. The registry calls `load` again
each time it loads a certified model, so the same checks stand between an artifact and the
live pipeline as between an artifact and the database.

Artifacts certified before those checks existed are refused, not read under the rules they
were certified by: lgbm-2026-10 names no counting rule, declared its ledger's row count and
digests its files without its manifest, so nothing in it can be checked against a verdict the
server computes. The refusal says so and says to re-certify.
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

from agb.research import gates, recompute
from agb.research.recompute import PATHS
from agb.research.trials import FILE as LEDGER_FILE
from agb.research.trials import RULE, Ledger, LedgerError, declared

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


def artifact_sha256(manifest: dict[str, Any]) -> str:
    """One digest over the whole manifest except this digest itself: canonical JSON, keys
    sorted and no whitespace. The manifest lists every other file with its own digest, so this
    one covers every byte of the artifact, the verdict and the fields that select what the
    verdict is recomputed from among them."""
    body = {key: value for key, value in manifest.items() if key != "artifact_sha256"}
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _files_sha256(files: dict[str, str]) -> str:
    """The digest certifications up to lgbm-2026-10 carry: one over every file's name and
    digest and nothing of the manifest. Kept only so `load` can name why it refuses one."""
    hasher = hashlib.sha256()
    for name in sorted(files):
        hasher.update(f"{name}\0{files[name]}\n".encode())
    return hasher.hexdigest()


# Every field `load` reads, so a manifest without one is refused by name rather than by a
# KeyError halfway through the checks.
REQUIRED = (
    *FIELDS, "declared_trials_rule", "ledger_rows", "sharpe_basis", "excess", "sharpe_total",
    "partition_hash", "code_version", "trial_id", "cpcv_trial_id", "backtest_trial_id",
    "files", "ledger_sha256", "artifact_sha256", CERTIFIED_PATH,
)  # fmt: skip
# How to get an artifact this code accepts out of one it refuses for when it was certified.
RECERTIFY = "re-certify the model under the current code and import the new artifact"


def load(directory: Path) -> dict[str, Any]:
    """The manifest of the artifact at `directory`, after every check, or `CertificationError`
    naming the first that fails:

    - the manifest counted its trials by `trials.RULE`; a manifest naming another rule, or none,
      is refused, since a count under a rule the certification chose is a count it chose
    - it carries every field this reads, and each listed file is present and unaltered, nothing
      unlisted is there, and the booster, the ledger and the paths are among the files
    - the digest over the manifest matches, so no field of it has changed since it was written
    - the shipped ledger's rows chain, there are as many as the certification shipped, and
      recounting them under `trials.RULE` gives the declared number of trials
    - the ledger holds the certification's own succeeded row, which agrees with the manifest on
      what was certified, over what, at which thresholds, and on the paths the artifact ships
    - running the gates again over those paths and that ledger gives the manifest's verdict,
      within `recompute.REL_TOL`; every field that differs is named

    Artifacts certified before the rule was fixed and the verdict recomputed, lgbm-2026-10
    among them, are refused at the first or third check with what to do about it."""
    path = directory / MANIFEST
    if not path.is_file():
        raise CertificationError(f"{directory} holds no {MANIFEST}")
    try:
        manifest = json.loads(path.read_text())
    except ValueError as error:
        raise CertificationError(f"{MANIFEST} is not JSON: {error}") from None
    if not isinstance(manifest, dict):
        raise CertificationError(f"{MANIFEST} is not a JSON object")
    rule = manifest.get("declared_trials_rule")
    if rule is None:
        raise CertificationError(
            f"{MANIFEST} names no trial count rule: the model was certified before the rule was"
            f" fixed at {RULE!r} (issue #211); {RECERTIFY}"
        )
    if rule != RULE:
        raise CertificationError(
            f"{MANIFEST} counted its trials as {rule!r}, and the only rule this code accepts is"
            f" {RULE!r}; {RECERTIFY}"
        )
    missing = [name for name in REQUIRED if name not in manifest]
    if missing:
        raise CertificationError(f"{MANIFEST} lacks {', '.join(missing)}; {RECERTIFY}")
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
    for name in (MODEL, LEDGER_FILE, PATHS):
        if name not in files:
            raise CertificationError(f"{directory} holds no {name}")
    if files[LEDGER_FILE] != manifest["ledger_sha256"]:
        raise CertificationError(f"{LEDGER_FILE} is not the ledger the manifest names")
    if artifact_sha256(manifest) != manifest["artifact_sha256"]:
        if _files_sha256(files) == manifest["artifact_sha256"]:
            raise CertificationError(
                f"the artifact digest covers its files and not {MANIFEST}: the model was"
                f" certified before the manifest was digested (issue #211); {RECERTIFY}"
            )
        raise CertificationError(f"{MANIFEST} has been altered since certification")
    ledger = Ledger(directory / LEDGER_FILE)
    try:
        ledger.verify()
    except LedgerError as error:
        raise CertificationError(f"{LEDGER_FILE} cannot be trusted: {error}") from None
    rows = ledger.rows()
    if len(rows) != manifest["ledger_rows"]:
        raise CertificationError(
            f"{LEDGER_FILE} holds {len(rows)} rows; the certification shipped"
            f" {manifest['ledger_rows']}"
        )
    try:
        count = declared(rows, rule=RULE)
    except LedgerError as error:
        raise CertificationError(f"{LEDGER_FILE} cannot be counted: {error}") from None
    if count != manifest["declared_trials"]:
        raise CertificationError(
            f"{LEDGER_FILE} holds {count} trials counted as {RULE}; the certification declared"
            f" {manifest['declared_trials']}"
        )
    row = next((r for r in rows if r["trial_id"] == manifest["trial_id"]), None)
    if row is None:
        raise CertificationError(
            f"the shipped ledger lacks the certification's trial {manifest['trial_id']!r}"
        )
    if row["kind"] != KIND or row["status"] != "succeeded":
        raise CertificationError(f"trial {row['trial_id']} is not a succeeded certification")
    if manifest["certifies"] != gates.CERTIFIES:
        raise CertificationError(
            f"{MANIFEST} certifies {manifest['certifies']!r}; this code certifies only"
            f" {gates.CERTIFIES!r}"
        )
    results = json.loads(row["results"] or "null") or {}
    disagree = _disagreements(manifest, row, results)
    if disagree:
        raise CertificationError(
            "the manifest disagrees with the certification's own trial row on "
            + ", ".join(disagree)
        )
    try:
        shipped = json.loads((directory / PATHS).read_text())
    except ValueError as error:
        raise CertificationError(f"{PATHS} is not JSON: {error}") from None
    if recompute.differences(results.get("paths"), shipped, PATHS):
        raise CertificationError(f"{PATHS} is not the paths the certification's trial row holds")
    try:
        found = recompute.verdict(manifest, shipped, rows)
    except (recompute.RecomputeError, KeyError, TypeError, ValueError) as error:
        raise CertificationError(f"the verdict cannot be recomputed: {error!r}") from None
    expected = {key: manifest[key] for key in found}
    differences = recompute.differences(expected, found)
    if differences:
        raise CertificationError(
            f"the verdict recomputed from {PATHS} and {LEDGER_FILE} differs from {MANIFEST} in"
            f" {len(differences)} field(s): " + "; ".join(differences)
        )
    return manifest


def _disagreements(
    manifest: dict[str, Any], row: dict[str, Any], results: dict[str, Any]
) -> list[str]:
    """The fields on which the manifest and the certification's ledger row say different things
    about what was certified. The row's columns are covered by the ledger's chain, so these are
    what bind the manifest's selectors to the run the ledger recorded."""
    found = []
    for name in (
        "declared_trials",
        "passed",
        "declared_trials_rule",
        "ledger_rows",
        "model_id",
        "certified_capital",
        "cpcv_trial_id",
        "backtest_trial_id",
    ):
        if results.get(name) != manifest[name]:
            found.append(name)
    if results.get("thresholds") != manifest["gates"].get("thresholds"):
        found.append("gates.thresholds")
    columns = {
        "features": json.loads(row["features"] or "null"),
        "train_start": row["train_start"].isoformat() if row["train_start"] else None,
        "train_end": row["train_end"].isoformat() if row["train_end"] else None,
        "partition_hash": row["partition_hash"],
        "cost_model_version": row["cost_model_version"],
        "code_version": row["code_version"],
    }  # fmt: skip
    found += [name for name, value in columns.items() if value != manifest[name]]
    return found


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
