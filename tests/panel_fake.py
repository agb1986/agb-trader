"""A stand-in for the stage-2 panel, for the tests that exercise the blend, the shadow basket
and the gate around the panel rather than the panel itself (tests/test_panel.py and
tests/test_debate.py cover the real one).

`FakePanel` writes the rows a real run writes -- a run, a retrieval and its filings per name,
a Judge call, a score and its six dimensions -- from scores the test chooses, and records
exactly what the rebalance told it. The rubric rules are the real ones: the dimensions go
through agb.debate's `score`, so a delinquent filer is capped and a recent IPO scores its
neutral 2 here as in production.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date

from agb import analyst, debate

# What a name scores when the test does not say: the six dimensions alike, cycling over three
# values so a shortlist's scores are spread well clear of the dispersion gate.
DEFAULTS = (4, 3, 2)
SECTOR = "Technology"


def judgement(scores: tuple[int, ...]) -> dict[int, tuple[int, str]]:
    """The Judge's answer for one name: a score and a reason for each rubric dimension."""
    return {
        n: (score, f"the Judge on dimension {n}: scored {score}")
        for n, score in zip(analyst.RUBRIC, scores, strict=True)
    }


def digest(*parts: str) -> str:
    return hashlib.sha256("/".join(parts).encode()).hexdigest()


@dataclass(frozen=True)
class Call:
    """What the rebalance handed the panel: which names to score, their sectors, the date
    nothing filed later may be read, and the job it belongs to. No rank, and no score."""

    symbols: list[str]
    sectors: dict[str, str]
    as_of: date
    job_id: int | None


class FakePanel:
    """The panel as `basket.Stage2` drives it. `scores` maps a symbol to its six dimension
    scores, or to None for a name whose debate failed; one it does not name takes a default.
    `retrievals` maps a symbol to an outcome EDGAR gave other than 'retrieved', `delinquent`
    a symbol to 'nt_10k' or 'late_10k', and `ipo` names the recent listings."""

    def __init__(
        self,
        config_id: int,
        *,
        shortlist_n: int = 50,
        w: float = 0.2,
        scores: dict[str, tuple[int, ...] | None] | None = None,
        retrievals: dict[str, str] | None = None,
        delinquent: dict[str, str] | None = None,
        ipo: tuple[str, ...] = (),
        analyst_failure: tuple[str, ...] = (),
        analyst_refusal: tuple[str, ...] = (),
    ):
        self.config_id = config_id
        self.shortlist_n = shortlist_n
        self.w = w
        self.scores = scores or {}
        self.retrievals = retrievals or {}
        self.delinquent = delinquent or {}
        self.ipo = ipo
        self.analyst_failure = analyst_failure
        self.analyst_refusal = analyst_refusal
        self.calls: list[Call] = []

    def dimensions(self, symbol: str, i: int) -> tuple[int, ...] | None:
        """The six scores the Judge gave a name, or None for a debate that failed."""
        if symbol in self.scores:
            return self.scores[symbol]
        return (DEFAULTS[i % len(DEFAULTS)],) * len(analyst.RUBRIC)

    def __call__(self, conn, shortlist, sectors, as_of, job_id=None) -> int:
        self.calls.append(Call(list(shortlist), dict(sectors), as_of, job_id))
        run_id = conn.execute(
            "INSERT INTO agb.panel_run (panel_config_id, job_id, as_of, actor)"
            " VALUES (%s, %s, %s, 'pipeline') RETURNING id",
            (self.config_id, job_id, as_of),
        ).fetchone()[0]
        for i, symbol in enumerate(shortlist):
            self._name(conn, run_id, symbol, sectors.get(symbol, SECTOR), as_of, i)
        return run_id

    def _name(self, conn, run_id: int, symbol: str, sector: str, as_of: date, i: int) -> None:
        outcome = self.retrievals.get(symbol, "retrieved")
        delinquent = self.delinquent.get(symbol)
        retrieval_id = conn.execute(
            """
            INSERT INTO agb.panel_retrieval
                (run_id, symbol, cik, company, outcome, delinquent, ipo, detail, aliases)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """,
            (
                run_id,
                symbol,
                f"{i + 1:010d}",
                f"{symbol} Corp",
                outcome,
                delinquent,
                symbol in self.ipo,
                None if outcome == "retrieved" else f"{outcome} for {symbol}",
                [f"{symbol} Corp", symbol],
            ),
        ).fetchone()[0]
        if outcome != "retrieved":
            return
        self._filings(conn, retrieval_id, symbol, as_of)
        if symbol in self.analyst_failure:
            self._extract(conn, retrieval_id, symbol, "analyst_failure")
            return
        if symbol in self.analyst_refusal:
            self._extract(conn, retrieval_id, symbol, "analyst_refusal")
            return
        self._extract(conn, retrieval_id, symbol, "extracted")
        self._score(conn, run_id, retrieval_id, symbol, sector, delinquent, i)

    def _filings(self, conn, retrieval_id: int, symbol: str, as_of: date) -> None:
        """The two pieces of one annual report the Analyst read, as `/why` cites them: the
        first in full, the second truncated, as a long filing is."""
        for document, section, chars, sent in (
            ("D1", "Item 1A", 4_000, 4_000),
            ("D2", "Item 7", 90_000, 60_000),
        ):
            conn.execute(
                """
                INSERT INTO agb.panel_filing
                    (retrieval_id, document, form, accession, filed, report_date, section,
                     document_sha256, text_sha256, chars, sent_chars)
                VALUES (%s, %s, '10-K', %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    retrieval_id,
                    document,
                    f"0000{retrieval_id:06d}-26-000001",
                    as_of,
                    as_of,
                    section,
                    digest(symbol, document, "document"),
                    digest(symbol, document, "text"),
                    chars,
                    sent,
                ),
            )

    def _extract(self, conn, retrieval_id: int, symbol: str, outcome: str) -> None:
        conn.execute(
            """
            INSERT INTO agb.panel_extract
                (retrieval_id, outcome, detail, scrubber_version, extract_sha256)
            VALUES (%s, %s, %s, '1', %s)
            """,
            (
                retrieval_id,
                outcome,
                None if outcome == "extracted" else f"the Analyst failed on {symbol}",
                digest(symbol, "extract") if outcome == "extracted" else None,
            ),
        )

    def _score(self, conn, run_id, retrieval_id, symbol, sector, delinquent, i) -> None:
        dimensions = self.dimensions(symbol, i)
        call_id = self._call(conn, run_id, retrieval_id, symbol)
        if dimensions is None:
            conn.execute(
                """
                INSERT INTO agb.panel_score
                    (retrieval_id, run_id, panel_config_id, sector, judge_call_id, outcome, detail)
                VALUES (%s, %s, %s, %s, %s, 'debate_failure', %s)
                """,
                (retrieval_id, run_id, self.config_id, sector, call_id, "the Judge did not answer"),
            )
            return
        score = debate.score(judgement(dimensions), delinquent, symbol in self.ipo)
        score_id = conn.execute(
            """
            INSERT INTO agb.panel_score
                (retrieval_id, run_id, panel_config_id, sector, judge_call_id, outcome, total,
                 score)
            VALUES (%s, %s, %s, %s, %s, 'scored', %s, %s) RETURNING id
            """,
            (retrieval_id, run_id, self.config_id, sector, call_id, score.total, score.value),
        ).fetchone()[0]
        for d in score.dimensions:
            conn.execute(
                """
                INSERT INTO agb.panel_dimension
                    (score_id, dimension, score, reason, source, judge_score, judge_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (score_id, d.dimension, d.score, d.reason, d.source, d.judge_score, d.judge_reason),
            )

    def _call(self, conn, run_id: int, retrieval_id: int, symbol: str) -> int:
        """The Judge's call, which a scored row must name."""
        return conn.execute(
            """
            INSERT INTO agb.panel_model_call
                (run_id, retrieval_id, role, model, response_model, request_sha256,
                 response_sha256, cached, input_tokens, output_tokens, cache_read_tokens,
                 cache_creation_tokens, stop_reason, request_id)
            VALUES (%s, %s, 'judge', 'judge-fake', 'judge-fake-20260101', %s, %s, false,
                    1000, 500, 0, 0, 'end_turn', %s) RETURNING id
            """,
            (
                run_id,
                retrieval_id,
                digest(symbol, "request"),
                digest(symbol, "response"),
                f"req_{symbol}",
            ),
        ).fetchone()[0]
