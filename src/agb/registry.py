"""The certified-model registry's server side (docs/architecture/07-research-environment.md
and 08-state-and-audit.md): which certification is live, promoting one, voiding one, and
loading the live model for the pipeline to rank with.

The research profile certifies a model on the workstation and imports its
`model_certification` row. The artifact directory it wrote is copied to the server as it
stands, under `<models dir>/<model id>/` (AGB_MODELS_DIR, `/models` in the containers); the
row's `artifact_path` says where it was certified and is kept for provenance only. The
operator then promotes it with `agb promote <model id>`, which refuses, and records the
refusal as a refused command:

  * a model with no certification, or one that failed its gates;
  * a voided certification, or one made under a cost model other than the current one. A
    refused promotion changes nothing; one that goes ahead first voids every certification
    made under a cost model the current one supersedes, as `agb void-stale` does. One made
    under a cost model this code does not know yet, by newer research code, is refused but
    left for the release that brings it;
  * an artifact missing from the models directory, or one `agb.research.artifact.load`
    refuses: altered files or manifest, a trial count by any rule but `trials.RULE`, or a
    verdict the gates do not give when run again over the paths and ledger it ships; or one
    whose manifest digest or declared trial count is not the certification's;
  * the model already live.

A promotion can carry one waiver, and one only. `agb promote <model id> --waive-for-paper
--reason <why>` promotes a certification whose single problem is that it failed its gates,
so that the stack can be exercised end to end while nothing has passed: the sizing run
(docs/build/measured-limits.md) needs a real rebalance to measure, and a rebalance with
nothing promoted fails before it proposes a basket. The waiver is refused unless the trading
mode is paper, refused for a voided or stale certification, and refused for a certification
that passed, which needs no waiver; the artifact is verified as it always is. A `waived`
certification_event records it, and `agb mode live` is refused while a waived model is live
(agb.ladder), so a waiver can never carry into live trading.

The latest promotion is the live model (`agb.promoted_model`). The pipeline loads it at each
rebalance with `load_promoted`, which verifies the artifact again and refuses the same way:
nothing promoted, voided, stale, altered. It never falls back to another model. Every basket
names the certification and the artifact digest it was ranked with, and the gate halts one
whose digest is not the live certification's.

Verifying an artifact is `agb.research.artifact.load`, the same function the import runs, so
the import and the registry cannot disagree on what an artifact must satisfy (issue #211). That
module and the two it uses, `agb.research.recompute` and `agb.research.gates`, import nothing
the app image lacks; tests/test_artifact.py holds that line. Nothing else from agb.research is
imported here. LightGBM is imported when a model is loaded.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import psycopg
import pyarrow as pa

from agb import approval, cost, features
from agb.approval import Source
from agb.ranker import Ranked, order
from agb.research import artifact

# The artifact layout, re-exported for the readers of `models_dir`.
from agb.research.artifact import MANIFEST as MANIFEST
from agb.research.artifact import MODEL, CertificationError

if TYPE_CHECKING:
    import duckdb

    from agb.universe import Universe

MODELS_ENV = "AGB_MODELS_DIR"
DEFAULT_MODELS_DIR = Path("/models")

PROMOTE = "promote"
VOID = "void"
VOID_STALE = "void_stale"

# The trading mode a measurement waiver is confined to, as agb.current_mode has it.
PAPER = "paper"

# Each categorical feature's fixed vocabulary. The research panel holds them as pandas
# categoricals over the same vocabulary, and LightGBM trains on their codes.
_VOCABULARIES = {"sector": features.SECTORS}


class RegistryError(Exception):
    """A model the registry will not load, and why."""


class Refused(RegistryError):
    """A promotion or voiding refused, and recorded as a refused command."""


@dataclass(frozen=True)
class Certification:
    """A `model_certification` row as the registry reads it, with its voiding if any."""

    id: int
    model_id: str
    passed: bool
    artifact_sha256: str
    declared_trials: int
    cost_model_version: str
    features: list[str]
    voided_at: datetime | None
    voided_by: str | None
    voided_reason: str | None
    waived_at: datetime | None
    waived_by: str | None
    waived_reason: str | None

    def stale(self, current: str) -> str:
        return (
            f"model {self.model_id} was certified under cost model"
            f" {self.cost_model_version}; the cost model is now {current}"
        )

    def problem(self, current: str) -> str | None:
        """Why this certification cannot be live whatever its artifact, or None."""
        if self.voided_at is not None:
            return (
                f"model {self.model_id} was voided on {self.voided_at:%Y-%m-%d} by"
                f" {self.voided_by}: {self.voided_reason}"
            )
        if not self.passed:
            return f"model {self.model_id} failed certification"
        if self.cost_model_version != current:
            return self.stale(current)
        return None

    def waiver(self, mode: str, current: str) -> str | None:
        """The paper-only measurement waiver this certification runs under, or None. A
        waiver covers a failed gate and nothing else: it is no waiver in live mode, none for
        a voided or stale certification, and none for one that passed."""
        if self.waived_at is None or mode != PAPER or self.passed:
            return None
        if self.voided_at is not None or self.cost_model_version != current:
            return None
        return (
            f"model {self.model_id} failed certification and is live under the paper-only"
            f" measurement waiver recorded on {self.waived_at:%Y-%m-%d} by {self.waived_by}:"
            f" {self.waived_reason}"
        )


_CERTIFICATION = """
    SELECT c.id, c.model_id, c.passed, c.artifact_sha256, c.declared_trials,
           c.cost_model_version, c.features, v.at, v.actor, v.reason, w.at, w.actor, w.reason
    FROM agb.model_certification c
    LEFT JOIN agb.certification_event v ON v.certification_id = c.id AND v.event = 'voided'
    LEFT JOIN agb.certification_event w ON w.certification_id = c.id AND w.event = 'waived'
"""
_BY_MODEL = _CERTIFICATION + "WHERE c.model_id = %s"
_LIVE = _CERTIFICATION + "WHERE c.id = (SELECT certification_id FROM agb.promoted_model)"
_PROMOTED = "SELECT certification_id, model_id, promoted_by, promoted_at FROM agb.promoted_model"
_STALE = """
    SELECT c.id, c.model_id, c.cost_model_version
    FROM agb.model_certification c
    WHERE c.cost_model_version = ANY(%s)
      AND NOT EXISTS (
          SELECT 1 FROM agb.certification_event v
          WHERE v.certification_id = c.id AND v.event = 'voided'
      )
    ORDER BY c.id
"""
_VOID = """
    INSERT INTO agb.certification_event
        (certification_id, event, reason, cost_model_version, command_id, actor)
    VALUES (%(id)s, 'voided', %(reason)s, %(current)s, %(command_id)s, %(actor)s)
    ON CONFLICT (certification_id) WHERE event = 'voided' DO NOTHING
"""
_WAIVE = """
    INSERT INTO agb.certification_event
        (certification_id, event, reason, cost_model_version, command_id, actor)
    VALUES (%(id)s, 'waived', %(reason)s, %(current)s, %(command_id)s, %(actor)s)
    ON CONFLICT (certification_id) WHERE event = 'waived' DO NOTHING
"""
_PROMOTE = """
    INSERT INTO agb.promotion_event (certification_id, command_id, actor)
    VALUES (%s, %s, %s)
"""
_MODE = "SELECT mode FROM agb.current_mode"


def certification(conn: psycopg.Connection, model_id: str) -> Certification | None:
    row = conn.execute(_BY_MODEL, (model_id,)).fetchone()
    return Certification(*row) if row is not None else None


def live(conn: psycopg.Connection) -> Certification | None:
    """The live model's certification, voided or not, or None if nothing is promoted."""
    row = conn.execute(_LIVE).fetchone()
    return Certification(*row) if row is not None else None


# The artifact.


def verify(directory: Path, artifact_sha256: str, declared_trials: int) -> None:
    """Check that `directory` holds the certified artifact: `agb.research.artifact.load`
    accepts it, which recomputes its verdict, and its manifest digest and declared trial count
    are the ones the certification row recorded. The digest covers the whole manifest, so an
    artifact that passes is the one that was imported, field for field."""
    try:
        manifest = artifact.load(directory)
    except CertificationError as exc:
        raise RegistryError(f"the artifact in {directory} is refused: {exc}") from None
    except (OSError, pa.ArrowException) as exc:
        raise RegistryError(f"the artifact in {directory} cannot be read: {exc}") from None
    digest = manifest["artifact_sha256"]
    if digest != artifact_sha256:
        raise RegistryError(
            f"the artifact in {directory} has digest {digest[:12]}; the certification's is"
            f" {artifact_sha256[:12]}"
        )
    if manifest["declared_trials"] != declared_trials:
        raise RegistryError(
            f"the artifact in {directory} declares {manifest['declared_trials']} trials; the"
            f" certification declared {declared_trials}"
        )


# The ranker.


def matrix(ranked: pa.Table, names: Sequence[str]) -> np.ndarray:
    """The rows the booster reads, one per name in `ranked` and one column per feature in
    the certification's order: each numeric feature's percentile, and each categorical's
    code in its fixed vocabulary, as the research panel encodes it. Missing is NaN."""
    columns = []
    for name in names:
        values = ranked.column(name).to_pylist()
        if name in _VOCABULARIES:
            codes = {value: float(code) for code, value in enumerate(_VOCABULARIES[name])}
            columns.append([codes.get(value, math.nan) for value in values])
        else:
            columns.append([math.nan if value is None else float(value) for value in values])
    return np.array(columns, dtype="float64").reshape(len(names), ranked.num_rows).T


@dataclass(frozen=True)
class CertifiedRanker:
    """The stage-1 ranker of one certified, verified artifact. Its name is the model id, which
    the basket records as its ranker identity beside the certification and the digest.
    `waiver` is set when the model is live under a paper-only measurement waiver, so that a
    run which ranks with an uncertified model says so."""

    certification_id: int
    name: str
    artifact_sha256: str
    features: tuple[str, ...]
    booster: Any
    waiver: str | None = None

    def rank(self, universe: Universe, lake: duckdb.DuckDBPyConnection) -> list[Ranked]:
        ranked = features.compute(universe, db=lake).ranked
        tickers = ranked.column("ticker").to_pylist()
        scores: dict[str, float | None] = {}
        if tickers:
            predicted = self.booster.predict(matrix(ranked, self.features))
            scores = dict(zip(tickers, (float(s) for s in predicted), strict=True))
        return order(universe.tickers, scores)


def load(models_dir: Path, certification: Certification) -> CertifiedRanker:
    """Verify the certification's artifact under `models_dir` and load its booster."""
    unknown = [name for name in certification.features if name not in features.FEATURES]
    if unknown:
        raise RegistryError(
            f"model {certification.model_id} reads features this code does not compute:"
            f" {', '.join(unknown)}"
        )
    directory = models_dir / certification.model_id
    if not directory.is_dir():
        raise RegistryError(f"model {certification.model_id} has no artifact at {directory}")
    verify(directory, certification.artifact_sha256, certification.declared_trials)
    import lightgbm as lgb

    try:
        booster = lgb.Booster(model_file=str(directory / MODEL))
    except lgb.basic.LightGBMError as exc:
        raise RegistryError(f"{directory / MODEL} is not a LightGBM model: {exc}") from None
    if booster.feature_name() != list(certification.features):
        raise RegistryError(
            f"{directory / MODEL} reads {', '.join(booster.feature_name())}; the"
            f" certification names {', '.join(certification.features)}"
        )
    return CertifiedRanker(
        certification.id,
        certification.model_id,
        certification.artifact_sha256,
        tuple(certification.features),
        booster,
    )


def load_promoted(conn: psycopg.Connection, models_dir: Path) -> CertifiedRanker:
    """The live model, ready to rank, or RegistryError saying why there is none: nothing
    promoted, the promoted certification voided, failed or stale, or its artifact missing or
    not the certified one. A model promoted under a paper-only measurement waiver loads in
    paper mode, carrying the waiver; in live mode it is refused as the failed certification
    it is."""
    promoted = live(conn)
    if promoted is None:
        raise RegistryError("no model is promoted; `agb promote <model id>` promotes one")
    (mode,) = conn.execute(_MODE).fetchone()
    waiver = promoted.waiver(mode, cost.CURRENT.version)
    why = promoted.problem(cost.CURRENT.version)
    if why is not None and waiver is None:
        raise RegistryError(f"the promoted {why}")
    ranker = load(models_dir, promoted)
    return ranker if waiver is None else replace(ranker, waiver=waiver)


# The CLI's commands.


def promote(
    conn: psycopg.Connection,
    model_id: str,
    source: Source,
    models_dir: Path,
    *,
    waive_for_paper: str | None = None,
) -> str:
    """Make `model_id`'s certification the live model. The promotion is a `promote` command
    and a promotion_event naming it; a refusal is recorded too, and raised as Refused.
    The refusal is decided first, on the certification as it stands, so a refused promotion
    changes nothing but the refused command; only a promotion that goes ahead voids the stale
    certifications. `waive_for_paper` is the operator's reason for a
    paper-only measurement waiver: it is refused outside paper mode, and refused for any
    certification whose problem is anything but the failed gate."""
    args: dict[str, Any] = {"model_id": model_id}
    if waive_for_paper is not None:
        args["waive_for_paper"] = waive_for_paper
    candidate = certification(conn, model_id)
    was = conn.execute(_PROMOTED).fetchone()
    (mode,) = conn.execute(_MODE).fetchone()
    why = _refusal(
        model_id,
        candidate,
        was,
        models_dir,
        cost.CURRENT.version,
        mode=mode,
        waive_for_paper=waive_for_paper,
    )
    if why is not None or candidate is None:
        approval.record(conn, source, PROMOTE, args, why)
        raise Refused(why)
    void_stale(conn, source)
    args |= {"certification_id": candidate.id, "artifact_sha256": candidate.artifact_sha256}
    with conn.transaction():
        command_id = approval.record(conn, source, PROMOTE, args)
        if waive_for_paper is not None:
            conn.execute(
                _WAIVE,
                {
                    "id": candidate.id,
                    "reason": waive_for_paper,
                    "current": cost.CURRENT.version,
                    "command_id": command_id,
                    "actor": source.actor,
                },
            )
        conn.execute(_PROMOTE, (candidate.id, command_id, source.actor))
    replacing = f", replacing {was[1]}" if was is not None else ""
    identity = f"certification {candidate.id}, artifact {candidate.artifact_sha256[:12]}"
    if waive_for_paper is None:
        return (
            f"model {model_id} promoted{replacing}: {identity}. The next rebalance ranks with it."
        )
    return (
        f"model {model_id} promoted{replacing} under a paper-only measurement waiver:"
        f" {identity}. It failed certification, so the next rebalance ranks with an"
        " uncertified model, and a move to live is refused until a model that passed is"
        " promoted."
    )


def _refusal(
    model_id: str,
    candidate: Certification | None,
    was: tuple | None,
    models_dir: Path,
    current: str,
    *,
    mode: str = "live",
    waive_for_paper: str | None = None,
) -> str | None:
    if candidate is None:
        return f"no certification of model {model_id}"
    if waive_for_paper is not None:
        if mode != PAPER:
            return (
                f"a measurement waiver promotes a model that failed certification, so it is"
                f" refused in {mode} mode; it is for paper only"
            )
        if not waive_for_paper.strip():
            return "a measurement waiver needs a reason"
        if candidate.passed:
            return f"model {model_id} passed certification and needs no waiver"
        if candidate.voided_at is not None:
            return candidate.problem(current)
        if candidate.cost_model_version != current:
            return candidate.stale(current)
    elif (why := candidate.problem(current)) is not None:
        return why
    if was is not None and was[0] == candidate.id:
        return f"model {model_id} is already live, promoted {was[3]:%Y-%m-%d} by {was[2]}"
    try:
        load(models_dir, candidate)
    except RegistryError as exc:
        return str(exc)
    return None


def void(conn: psycopg.Connection, model_id: str, reason: str, source: Source) -> str:
    """Void `model_id`'s certification for good. The voiding is a `void` command and a
    certification_event naming it; a refusal is recorded too, and raised as Refused."""
    args: dict[str, Any] = {"model_id": model_id, "reason": reason}
    target = certification(conn, model_id)
    if target is None:
        why = f"no certification of model {model_id}"
    elif target.voided_at is not None:
        why = f"model {model_id} was already voided on {target.voided_at:%Y-%m-%d}"
    elif not reason.strip():
        why = "a voiding needs a reason"
    else:
        why = None
    if why is not None or target is None:
        approval.record(conn, source, VOID, args, why)
        raise Refused(why)
    with conn.transaction():
        command_id = approval.record(conn, source, VOID, args | {"certification_id": target.id})
        conn.execute(
            _VOID,
            {
                "id": target.id,
                "reason": reason,
                "current": cost.CURRENT.version,
                "command_id": command_id,
                "actor": source.actor,
            },
        )
    promoted = conn.execute(_PROMOTED).fetchone()
    if promoted is not None and promoted[0] == target.id:
        return (
            f"model {model_id} voided. It is the live model: rebalances fail until another"
            " is promoted."
        )
    return f"model {model_id} voided."


def superseded(current: str) -> list[str]:
    """The cost-model versions issued before `current`, oldest first."""
    versions = list(cost.VERSIONS)
    return versions[: versions.index(current)]


def void_stale(conn: psycopg.Connection, source: Source) -> list[str]:
    """Void every certification made under a cost model the one in force supersedes, and
    return their model ids. Any cost-model change invalidates a certification
    (docs/architecture/01), so the deploy runs this once the new code is up, and promotion
    runs it first. A certification under a version this code does not know is newer, not
    stale, and is left alone. Nothing stale records nothing."""
    current = cost.CURRENT.version
    older = superseded(current)
    stale = conn.execute(_STALE, (older,)).fetchall() if older else []
    if not stale:
        return []
    model_ids = [model_id for _, model_id, _ in stale]
    with conn.transaction():
        command_id = approval.record(
            conn, source, VOID_STALE, {"cost_model_version": current, "model_ids": model_ids}
        )
        for certification_id, _, version in stale:
            conn.execute(
                _VOID,
                {
                    "id": certification_id,
                    "reason": f"certified under cost model {version}, superseded by {current}",
                    "current": current,
                    "command_id": command_id,
                    "actor": source.actor,
                },
            )
    return model_ids
