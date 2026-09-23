"""A placeholder stage-1 ranker, so the rebalance can be built and exercised before the
classical model is certified. It is deliberately a single factor, deliberately named as a
placeholder, and kept in this one module so that replacing it with the certified model is one
change: construct the other `Ranker` in the pipeline and delete this file.

The factor is 12-1 momentum, the classical model's `mom_12_1`: the return from the close
`lookback` sessions before the snapshot to the close `skip` sessions before it, skipping the
most recent month. That is a real signal, which is why it is the placeholder rather than a
random or alphabetical ranking: a basket it proposes is one the eye can sanity-check. It is
still uncertified, and a basket ranked by it must never trade.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from agb.ranker import Ranked, order

if TYPE_CHECKING:
    import duckdb

    from agb.universe import Universe

# Sessions, not calendar days: twelve months less one.
LOOKBACK = 252
SKIP = 21

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


class PlaceholderMomentum:
    """12-1 momentum on the split-adjusted close. The name carries the parameters, so a run
    with a different window is a different ranker identity."""

    def __init__(self, lookback: int = LOOKBACK, skip: int = SKIP) -> None:
        if not 0 <= skip < lookback:
            raise ValueError("skip must be shorter than the lookback")
        self.lookback, self.skip = lookback, skip

    @property
    def name(self) -> str:
        return f"placeholder-momentum-{self.lookback}-{self.skip}"

    def rank(self, universe: Universe, lake: duckdb.DuckDBPyConnection) -> list[Ranked]:
        start = dict(lake.execute(_CLOSE, [universe.snapshot_date, self.lookback]).fetchall())
        end = dict(lake.execute(_CLOSE, [universe.snapshot_date, self.skip]).fetchall())
        scores = {
            symbol: end[symbol] / start[symbol] - 1.0
            for symbol in universe.tickers
            if symbol in start and symbol in end
        }
        return order(universe.tickers, scores)
