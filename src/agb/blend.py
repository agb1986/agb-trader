"""Stage 2: the blend that mixes the panel's safety score into the stage-1 ranking
(docs/architecture/02).

The shortlist is the stage-1 ranking's best `shortlist_n` names. Each gets two numbers in
[0, 1]: `stage1_rank_pct`, its place in the shortlist by stage-1 rank, best 1.0; and
`llm_score_pct`, its place among the names the panel scored, safest 1.0. Both are the
tie-averaged percentile the stage-1 features are normalised by, so the two terms are on one
scale. The blend is

    final_score = stage1_rank_pct + w * llm_score_pct

with `w` read from the immutable panel_config the basket names. Nothing is fitted: the panel
reads filings written before the outcome it would be fitted against, so a weight learnt from
history would be learning the outcome. At w = 0.2 the panel can move a name at most a fifth
of the ranking's range; at w = 0 it moves nothing and the blended order is the stage-1 order.

Failure is neutral. A name the panel did not score -- an EDGAR failure, no readable filings,
an Analyst or debate failure -- takes `llm_score_pct = 0.5` and so neither gains nor loses
against a name the panel scored in the middle. The panel is never told a name's stage-1 rank,
which is why nothing here is passed back into it.
"""

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import psycopg

from agb import features
from agb.debate import MAX_TOTAL
from agb.ranker import Ranked

# What an unscored name's panel term is: the middle of the range, which moves nothing.
NEUTRAL = 0.5
# The least spread the panel's scores may show for the blend to mean anything. A panel that
# scores every name alike ranks to noise once percentile-ranked, however right each score is.
# Measured as the standard deviation of the normalised scores, so 0.05 is about 1.2 points of
# the rubric's 24.
DISPERSION_MIN = 0.05
# A rubric dimension at or below this is a flag the approval request names: the rubric's
# bottom two, which is a dimension the Judge found materially wrong.
FLAG_MAX = 1
# A rubric dimension at or below this is one `/why` gives the Judge's reason for, which the
# spec sets at 2 or below (docs/architecture/05).
REASON_MAX = 2


@dataclass(frozen=True)
class Scored:
    """What the panel made of one name: the normalised score in [0, 1] with the rubric total
    it was computed from, and the `panel_score` row both were read from."""

    symbol: str
    score: float
    total: int
    score_id: int


@dataclass(frozen=True)
class Blended:
    """One shortlisted name's two terms and their blend."""

    symbol: str
    stage1_rank: int
    stage1_rank_pct: float
    llm_score_pct: float
    final_score: float
    scored: Scored | None

    @property
    def neutral(self) -> bool:
        """Whether the name took the neutral default because the panel did not score it."""
        return self.scored is None


def _percentiles(values: Sequence[float | None]) -> list[float]:
    """The tie-averaged percentile of each value, with a missing one taking the neutral
    middle, which is what an unscored name's panel term is."""
    return [NEUTRAL if pct is None else pct for pct in features.percentile_rank(values)]


def blend(shortlist: Sequence[Ranked], scores: Mapping[str, Scored], w: float) -> list[Blended]:
    """The shortlist blended with the panel's scores, best first. Ties break by stage-1 rank,
    so with w = 0 the order is exactly the stage-1 order."""
    stage1 = _percentiles([float(-ranked.rank) for ranked in shortlist])
    panel = _percentiles(
        [scores[ranked.symbol].score if ranked.symbol in scores else None for ranked in shortlist]
    )
    blended = [
        Blended(
            symbol=ranked.symbol,
            stage1_rank=ranked.rank,
            stage1_rank_pct=rank_pct,
            llm_score_pct=score_pct,
            final_score=rank_pct + w * score_pct,
            scored=scores.get(ranked.symbol),
        )
        for ranked, rank_pct, score_pct in zip(shortlist, stage1, panel, strict=True)
    ]
    return sorted(blended, key=lambda b: (-b.final_score, b.stage1_rank))


def ranking(blended: Sequence[Blended], tail: Sequence[Ranked]) -> list[str]:
    """The ranking the rebalance selects from: the blended shortlist first, then the stage-1
    tail in its own order, so an incumbent that fell out of the shortlist still has a place
    and can be held through or sold like any other."""
    return [b.symbol for b in blended] + [ranked.symbol for ranked in tail]


def coverage(blended: Sequence[Blended]) -> tuple[int, int]:
    """How many of the shortlist the panel scored, and how many there were."""
    return sum(1 for b in blended if b.scored is not None), len(blended)


def dispersion(blended: Sequence[Blended]) -> float | None:
    """How far apart the panel's scores are: the standard deviation of the normalised scores
    of the names it scored. None when it scored fewer than two, which has no spread."""
    scores = [b.scored.score for b in blended if b.scored is not None]
    if len(scores) < 2:
        return None
    return statistics.pstdev(scores)


_SCORES = """
    SELECT r.symbol, s.score, s.total, s.id
    FROM agb.panel_score s
    JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
    WHERE s.run_id = %s AND s.outcome = 'scored'
"""


def scores(conn: psycopg.Connection, run_id: int) -> dict[str, Scored]:
    """Every name the panel run scored, by symbol. A name it failed on is absent and takes
    the neutral default."""
    rows = conn.execute(_SCORES, (run_id,)).fetchall()
    return {row[0]: Scored(*row) for row in rows}


_SAFETY = """
    SELECT symbol, llm_total FROM agb.basket_blend WHERE basket_id = %s
"""


def safety(conn: psycopg.Connection, basket_id: int) -> dict[str, int | None]:
    """The panel's rubric total out of 24 for each name this basket blended, None for a name
    it did not score. Empty for a basket proposed with no panel, whose names have no score at
    all, and short of the basket for a name the shortlist did not reach."""
    return dict(conn.execute(_SAFETY, (basket_id,)).fetchall())


def safety_text(blended: Mapping[str, int | None], symbol: str) -> str:
    """How a name's safety score reads beside it. A name the panel scored carries its total
    out of the rubric's 24; one it failed on says so, because the blend gave it the neutral
    middle rather than a judgement; one no panel saw carries nothing."""
    if symbol not in blended:
        return ""
    total = blended[symbol]
    return "safety not scored" if total is None else f"safety {total}/{MAX_TOTAL}"


# The panel's flags for the approval request: what it could not read, what the filing rules
# caught, and any rubric dimension it scored at or below FLAG_MAX, for the names being bought
# or held. Each is a fact about a filing or a score, never a recommendation.
_FLAGS = """
    WITH held AS (
        SELECT r.id, r.symbol, r.outcome, r.delinquent, r.ipo
        FROM agb.panel_retrieval r
        JOIN agb.basket b ON b.panel_run_id = r.run_id
        WHERE b.id = %(basket)s
          AND r.symbol IN (SELECT symbol FROM agb.basket_target WHERE basket_id = %(basket)s)
    )
    SELECT symbol, flag FROM (
        SELECT symbol, 'no filings to read, scored neutral' AS flag
        FROM held WHERE outcome = 'no_filings'
        UNION ALL
        SELECT symbol, 'EDGAR could not be read, scored neutral'
        FROM held WHERE outcome = 'edgar_failure'
        UNION ALL
        SELECT symbol, 'delinquent filer (' || delinquent || ')'
        FROM held WHERE delinquent IS NOT NULL
        UNION ALL
        SELECT symbol, 'listed within the year' FROM held WHERE ipo
        UNION ALL
        SELECT h.symbol, 'the Analyst could not extract it, scored neutral'
        FROM held h JOIN agb.panel_extract e ON e.retrieval_id = h.id
        WHERE e.outcome = 'analyst_failure'
        UNION ALL
        SELECT h.symbol, 'the debate failed, scored neutral'
        FROM held h JOIN agb.panel_score s ON s.retrieval_id = h.id
        WHERE s.outcome = 'debate_failure'
        UNION ALL
        SELECT h.symbol, 'dimension ' || d.dimension || ' scored ' || d.score || ' of 4'
        FROM held h
        JOIN agb.panel_score s ON s.retrieval_id = h.id
        JOIN agb.panel_dimension d ON d.score_id = s.id
        WHERE s.outcome = 'scored' AND d.score <= %(flag_max)s
    ) flags
    ORDER BY symbol, flag
"""


def flags(conn: psycopg.Connection, basket_id: int) -> list[tuple[str, str]]:
    """The panel's flags on the names this basket ends up holding, as (symbol, flag). Empty
    for a basket proposed without a panel."""
    return conn.execute(_FLAGS, {"basket": basket_id, "flag_max": FLAG_MAX}).fetchall()
