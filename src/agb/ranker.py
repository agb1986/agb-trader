"""The interface between a ranking model and the rebalance.

The pipeline asks a `Ranker` for the universe's stage-1 ranking and hands the result to
agb.rebalance. The pipeline's ranker is the promoted certification's model
(agb.registry.CertifiedRanker); the tests' fakes implement the same protocol. A ranker names
itself and the certification it was loaded from, with that certification's artifact digest,
and the basket records all three, which the pre-trade gate checks against the live
certification.

This module imports only the standard library. The protocol's arguments are typed against
the universe and the lake for the reader; nothing here imports them at runtime.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import duckdb

    from agb.universe import Universe


@dataclass(frozen=True)
class Ranked:
    """One name's place in a stage-1 ranking. `score` is the model's raw output, kept for
    provenance; a name the model could not score has none and ranks last."""

    symbol: str
    rank: int  # 1 is best
    score: float | None


class Ranker(Protocol):
    """A stage-1 ranking model. `name` identifies the model and its version; a change to
    either is a new name. `certification_id` is its `model_certification` row and
    `artifact_sha256` the digest of the artifact it was loaded from, as verified."""

    name: str
    certification_id: int
    artifact_sha256: str

    def rank(self, universe: Universe, lake: duckdb.DuckDBPyConnection) -> list[Ranked]:
        """Every member of `universe`, best first, from data on or before its snapshot date."""
        ...


def order(symbols: Iterable[str], scores: Mapping[str, float | None]) -> list[Ranked]:
    """Rank `symbols` by score, highest first. Ties break by symbol, so a ranking is a
    deterministic function of its scores, and unscored names come last, by symbol."""
    scored = [(s, scores.get(s)) for s in set(symbols)]
    scored.sort(key=lambda item: (item[1] is None, -(item[1] or 0.0), item[0]))
    return [Ranked(symbol, i + 1, score) for i, (symbol, score) in enumerate(scored)]
