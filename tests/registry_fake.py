"""A stand-in for the promoted model, for the tests that exercise the rebalance, the gate,
execution and the bot around the model rather than the model itself (tests/test_registry.py
loads and promotes a real certified artifact).

`Momentum` ranks by momentum over a short window, which the fake market's growth rates order
by name, and carries a certification id and an artifact digest as agb.registry's
CertifiedRanker does. `promote` records a passing certification of it, as the research import
does, and its promotion, as `agb promote` does, so the gate finds the basket's digest live."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from psycopg.types.json import Jsonb

from agb import cost
from agb.ranker import Ranked, order

if TYPE_CHECKING:
    import duckdb

    from agb.universe import Universe

MODEL_ID = "momentum-fake"
ARTIFACT = "ab" * 32

_CERTIFY = """
    INSERT INTO agb.model_certification (
        model_id, certifies, passed, artifact_sha256, artifact_path, ledger_sha256, features,
        train_start, train_end, declared_trials, dsr, pbo, sharpe_net, dsr_2x,
        break_even_multiple, sharpe_gross, gross_sharpe_flag, participation_max,
        participation_mean, cost_model_version, certified_capital, path_quantiles, gates,
        certified_at, actor)
    VALUES (%(model_id)s, 'stage 1 (classical ranker) only', %(passed)s, %(artifact_sha256)s,
            %(artifact_path)s, %(ledger)s, %(features)s, '2010-01-01', '2025-12-31',
            %(declared_trials)s, 0.97, 0.1, 0.8, 0.96, 3.0, 1.0, false, 0.001, 0.0005,
            %(cost_model_version)s, 100000, '{}', '{}', %(certified_at)s, 'research')
    RETURNING id
"""
_COMMAND = """
    INSERT INTO agb.command (actor, surface, raw_text, verb, args, outcome)
    VALUES ('operator', 'cli', %s, %s, '{}', 'accepted')
    RETURNING id
"""


def certify(
    db,
    model_id: str = MODEL_ID,
    artifact_sha256: str = ARTIFACT,
    *,
    passed: bool = True,
    declared_trials: int = 1,
    cost_model_version: str | None = None,
    features: list[str] | None = None,
) -> int:
    """`model_id`'s certification, recorded once as the research import would, and its id."""
    conn = db.connect("research")
    row = conn.execute(
        "SELECT id FROM agb.model_certification WHERE model_id = %s", (model_id,)
    ).fetchone()
    if row is not None:
        return row[0]
    [(certification_id,)] = conn.execute(
        _CERTIFY,
        {
            "model_id": model_id,
            "passed": passed,
            "artifact_sha256": artifact_sha256,
            "artifact_path": f"/research/certifications/{model_id}",
            "ledger": "cd" * 32,
            "features": Jsonb(features or ["mom_12_1"]),
            "declared_trials": declared_trials,
            "cost_model_version": cost_model_version or cost.CURRENT.version,
            "certified_at": datetime(2026, 1, 5, tzinfo=UTC),
        },
    ).fetchall()
    return certification_id


def promote(db, model_id: str = MODEL_ID) -> int:
    """Certify `model_id` if it is not yet, and make it the live model unless it already is.
    Returns its certification id."""
    certification_id = certify(db, model_id)
    conn = db.connect("cli")
    live = conn.execute("SELECT certification_id FROM agb.promoted_model").fetchone()
    if live is None or live[0] != certification_id:
        with conn.transaction():
            [(command_id,)] = conn.execute(
                _COMMAND, (f"agb promote {model_id}", "promote")
            ).fetchall()
            conn.execute(
                "INSERT INTO agb.promotion_event (certification_id, command_id, actor)"
                " VALUES (%s, %s, 'operator')",
                (certification_id, command_id),
            )
    return certification_id


def void(db, model_id: str = MODEL_ID) -> None:
    """Void `model_id`'s certification, as `agb void` does."""
    certification_id = certify(db, model_id)
    conn = db.connect("cli")
    with conn.transaction():
        [(command_id,)] = conn.execute(
            _COMMAND, (f"agb void {model_id} --reason test", "void")
        ).fetchall()
        conn.execute(
            "INSERT INTO agb.certification_event"
            " (certification_id, event, reason, cost_model_version, command_id, actor)"
            " VALUES (%s, 'voided', 'test', %s, %s, 'operator')",
            (certification_id, cost.CURRENT.version, command_id),
        )


# The close `back` sessions before the snapshot, for every name that printed on that session.
_CLOSE = """
    WITH sessions AS (
        SELECT date, row_number() OVER (ORDER BY date DESC) - 1 AS back
        FROM (SELECT DISTINCT date FROM sep WHERE date <= ?)
    )
    SELECT sep.ticker, sep.close
    FROM sep JOIN sessions ON sep.date = sessions.date
    WHERE sessions.back = ? AND sep.close IS NOT NULL AND sep.close > 0
"""


@dataclass(frozen=True)
class Momentum:
    """Momentum from the close `lookback` sessions before the snapshot to the close `skip`
    sessions before it."""

    certification_id: int
    lookback: int = 80
    skip: int = 5
    artifact_sha256: str = ARTIFACT
    name: str = MODEL_ID

    def rank(self, universe: Universe, lake: duckdb.DuckDBPyConnection) -> list[Ranked]:
        start = dict(lake.execute(_CLOSE, [universe.snapshot_date, self.lookback]).fetchall())
        end = dict(lake.execute(_CLOSE, [universe.snapshot_date, self.skip]).fetchall())
        scores = {
            symbol: end[symbol] / start[symbol] - 1.0
            for symbol in universe.tickers
            if symbol in start and symbol in end
        }
        return order(universe.tickers, scores)


def ranker(db) -> Momentum:
    """The stand-in, promoted in `db`."""
    return Momentum(promote(db))
