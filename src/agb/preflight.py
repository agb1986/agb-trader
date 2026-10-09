"""The panel's pre-flight suite: whether the panel can be trusted under one `panel_config`.

Four checks, each measured over a real panel run and recorded against the configuration it was
measured under (docs/architecture/03, "Pre-flight validation"):

    retrieval_fidelity  every quote the Analyst returned was found verbatim in the document it
                        cites, so nothing downstream scores fiction
    disclosure_recall   the committed fixture set of known impairment disclosures
                        (agb.disclosures) was caught, each in its rubric dimension
    spot_check          the operator read about 20 of the run's names and agreed with the
                        panel's reading of every one
    dispersion          the scores are spread far enough apart to rank for a reason, the same
                        measure and floor the monthly pre-trade gate applies

A result belongs to the configuration, not to the run: a new `panel_config` has no results and
reads as not yet pre-flighted, so changing the models, prompts, rubric or scrubber means
running the suite again. "The panel pre-flight has passed" is one of the paper-to-live criteria
(docs/architecture/04), and the eligibility check reads `agb.panel_preflight_ready`.

Nothing here passes by vacuity: a check with too little to measure raises rather than
recording a pass, and recall ignores a fixture whose phrase is not in the text the Analyst was
given instead of counting it against the panel.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import psycopg

from agb import analyst, blend, edgar, panel
from agb.disclosures import FIXTURES, Disclosure
from agb.panel_store import PanelStore

log = logging.getLogger(__name__)

CHECKS = ("retrieval_fidelity", "disclosure_recall", "spot_check", "dispersion")
# The fewest names a run must have read for fidelity or dispersion to mean anything. A handful
# of names can be clean, or spread out, by luck.
MIN_NAMES = 10
# The fewest usable fixtures recall needs: fewer and the check says nothing about recall.
MIN_FIXTURES = 3
# How many names the operator reviews, which the spec puts at about 20.
SPOT_CHECK_N = 20
# How much of a rejected quote the recorded detail keeps.
QUOTE_CHARS = 200


class PreflightError(Exception):
    """A check that cannot be measured: too little to measure, or a name not under review."""


@dataclass(frozen=True)
class Result:
    """One check's result, as it is recorded."""

    name: str
    passed: bool
    measured: str
    threshold: str
    detail: dict[str, Any] = field(default_factory=dict)
    run_id: int | None = None

    def __str__(self) -> str:
        return f"{self.name}: {'passed' if self.passed else 'FAILED'}, {self.measured}"


def record(conn: psycopg.Connection, config_id: int, result: Result, actor: str) -> int:
    """Append a check's result to `panel_preflight`; returns the row's id."""
    row = conn.execute(
        """
        INSERT INTO agb.panel_preflight
            (panel_config_id, name, run_id, passed, measured, threshold, detail, actor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            config_id,
            result.name,
            result.run_id,
            result.passed,
            result.measured,
            result.threshold,
            json.dumps(result.detail, sort_keys=True, default=str),
            actor,
        ),
    ).fetchone()
    assert row is not None
    return row[0]


_RUN_CONFIG = """
    SELECT panel_config_id FROM agb.panel_run WHERE id = %s
"""


def config_of(conn: psycopg.Connection, run_id: int) -> int:
    """The configuration a run was made under, which is the one its results belong to."""
    row = conn.execute(_RUN_CONFIG, (run_id,)).fetchone()
    if row is None:
        raise PreflightError(f"no panel_run {run_id}")
    return row[0]


_EXTRACTS = """
    SELECT count(*) FILTER (WHERE e.outcome = 'extracted'),
           count(*) FILTER (WHERE e.outcome = 'analyst_failure')
    FROM agb.panel_extract e
    JOIN agb.panel_retrieval r ON r.id = e.retrieval_id
    WHERE r.run_id = %s
"""
_QUOTE_COUNTS = """
    SELECT count(*) FILTER (WHERE q.verified), count(*) FILTER (WHERE NOT q.verified)
    FROM agb.panel_quote q
    JOIN agb.panel_extract e ON e.id = q.extract_id
    JOIN agb.panel_retrieval r ON r.id = e.retrieval_id
    WHERE r.run_id = %s
"""
_REJECTED = """
    SELECT r.symbol, q.dimension, q.document, q.section, q.rejection, left(q.quote, %s)
    FROM agb.panel_quote q
    JOIN agb.panel_extract e ON e.id = q.extract_id
    JOIN agb.panel_retrieval r ON r.id = e.retrieval_id
    WHERE r.run_id = %s AND NOT q.verified
    ORDER BY r.symbol, q.id
"""


def fidelity(conn: psycopg.Connection, run_id: int, minimum: int = MIN_NAMES) -> Result:
    """Retrieval fidelity over one run: every quote the Analyst returned was checked against
    the document it cited when it was written, so this reads the verdicts and reports each
    quote that was not found verbatim, or cited no document or section."""
    extracted, analyst_failures = conn.execute(_EXTRACTS, (run_id,)).fetchone() or (0, 0)
    if extracted < minimum:
        raise PreflightError(
            f"panel_run {run_id} extracted {extracted} names; retrieval fidelity needs at "
            f"least {minimum}"
        )
    verified, rejected = conn.execute(_QUOTE_COUNTS, (run_id,)).fetchone() or (0, 0)
    if verified + rejected == 0:
        raise PreflightError(f"panel_run {run_id} returned no quotes; there is nothing to check")
    rows = conn.execute(_REJECTED, (QUOTE_CHARS, run_id)).fetchall()
    quotes = [
        {
            "symbol": symbol,
            "dimension": dimension,
            "document": document,
            "section": section,
            "rejection": rejection,
            "quote": quote,
        }
        for symbol, dimension, document, section, rejection, quote in rows
    ]
    return Result(
        name="retrieval_fidelity",
        passed=rejected == 0,
        measured=f"{rejected} of {verified + rejected} quotes rejected over {extracted} names",
        threshold="no quote rejected",
        detail={
            "names": extracted,
            "analyst_failures": analyst_failures,
            "verified": verified,
            "rejected": rejected,
            "quotes": quotes,
        },
        run_id=run_id,
    )


def dispersion(conn: psycopg.Connection, run_id: int, minimum: int = MIN_NAMES) -> Result:
    """The dispersion gate over one run: the standard deviation of the normalised scores,
    against the floor the monthly pre-trade gate uses."""
    scores = blend.scores(conn, run_id)
    if len(scores) < minimum:
        raise PreflightError(
            f"panel_run {run_id} scored {len(scores)} names; the dispersion gate needs at "
            f"least {minimum}"
        )
    spread = blend.spread([scored.score for scored in scores.values()])
    assert spread is not None
    totals = sorted((scored.total, symbol) for symbol, scored in scores.items())
    return Result(
        name="dispersion",
        passed=spread >= blend.DISPERSION_MIN,
        measured=f"{spread:.4f} over {len(scores)} scored names",
        threshold=f">= {blend.DISPERSION_MIN}",
        detail={
            "scored": len(scores),
            "stddev": spread,
            "minimum": blend.DISPERSION_MIN,
            "lowest": f"{totals[0][1]} {totals[0][0]} of 24",
            "highest": f"{totals[-1][1]} {totals[-1][0]} of 24",
        },
        run_id=run_id,
    )


@dataclass(frozen=True)
class Finding:
    """What became of one fixture: 'found' in its dimension, 'missed' by the Analyst, or
    'unusable' because the phrase is not in the text the Analyst was given."""

    fixture: Disclosure
    status: str
    detail: str

    def __str__(self) -> str:
        when = self.fixture.as_of.isoformat()
        return (
            f"{self.fixture.symbol:<6}  {when}  dimension {self.fixture.dimension}  "
            f"{self.status:<8}  {self.detail}"
        )


_RETRIEVAL = """
    SELECT id, outcome, detail FROM agb.panel_retrieval WHERE run_id = %s AND symbol = %s
"""
_TEXTS = """
    SELECT text_sha256 FROM agb.panel_filing WHERE retrieval_id = %s ORDER BY id
"""
_VERIFIED_QUOTES = """
    SELECT q.dimension, q.quote
    FROM agb.panel_quote q
    JOIN agb.panel_extract e ON e.id = q.extract_id
    WHERE e.retrieval_id = %s AND q.verified
    ORDER BY q.id
"""


def recall_runs(
    conn: psycopg.Connection,
    config_id: int,
    client: edgar.Edgar,
    model: analyst.Model,
    store: PanelStore,
    fixtures: Sequence[Disclosure] = FIXTURES,
    workers: int = panel.WORKERS,
) -> dict[date, int]:
    """One panel run per fixture date, over the names with a disclosure as of that date.
    Retrieval and the Analyst only: recall asks what the Analyst found, not how the debate
    scored it, so no debate is run and nothing is spent on one."""
    runs = {}
    for as_of in sorted({fixture.as_of for fixture in fixtures}):
        symbols = sorted({f.symbol for f in fixtures if f.as_of == as_of})
        runs[as_of] = panel.run(
            conn, config_id, symbols, as_of, client, model, store, workers=workers
        )
        log.info("recall run %d: %d names as of %s", runs[as_of], len(symbols), as_of)
    return runs


def findings(
    conn: psycopg.Connection,
    store: PanelStore,
    runs: Mapping[date, int],
    fixtures: Sequence[Disclosure] = FIXTURES,
) -> list[Finding]:
    """Each fixture against the run that read it."""
    out = []
    for fixture in fixtures:
        run_id = runs.get(fixture.as_of)
        if run_id is None:
            out.append(Finding(fixture, "unusable", f"no run as of {fixture.as_of}"))
            continue
        out.append(_finding(conn, store, run_id, fixture))
    return out


def _finding(
    conn: psycopg.Connection, store: PanelStore, run_id: int, fixture: Disclosure
) -> Finding:
    row = conn.execute(_RETRIEVAL, (run_id, fixture.symbol)).fetchone()
    if row is None:
        return Finding(fixture, "unusable", f"panel_run {run_id} did not read it")
    retrieval_id, outcome, detail = row
    if outcome != "retrieved":
        return Finding(fixture, "unusable", f"{outcome}: {detail}")
    wanted = analyst.normalise(fixture.phrase)
    texts = [digest for (digest,) in conn.execute(_TEXTS, (retrieval_id,)).fetchall()]
    if not any(wanted in analyst.normalise(store.get_text(digest)) for digest in texts):
        return Finding(
            fixture,
            "unusable",
            f"{fixture.phrase!r} is not in the {len(texts)} sections the Analyst read",
        )
    quoted = [
        dimension
        for dimension, quote in conn.execute(_VERIFIED_QUOTES, (retrieval_id,)).fetchall()
        if wanted in analyst.normalise(quote)
    ]
    if fixture.dimension in quoted:
        return Finding(fixture, "found", f"quoted in dimension {fixture.dimension}")
    if quoted:
        others = ", ".join(str(d) for d in sorted(set(quoted)))
        return Finding(fixture, "missed", f"quoted in dimension {others}, not {fixture.dimension}")
    return Finding(fixture, "missed", "not quoted")


def recall(
    conn: psycopg.Connection,
    store: PanelStore,
    runs: Mapping[date, int],
    fixtures: Sequence[Disclosure] = FIXTURES,
    minimum: int = MIN_FIXTURES,
) -> Result:
    """Known-disclosure recall over the fixture set, by dimension."""
    if not fixtures:
        raise PreflightError("the fixture set is empty; see agb.disclosures")
    found = findings(conn, store, runs, fixtures)
    usable = [f for f in found if f.status != "unusable"]
    missed = [f for f in found if f.status == "missed"]
    if len(usable) < minimum:
        raise PreflightError(
            f"{len(usable)} of {len(found)} fixtures are usable; recall needs at least "
            f"{minimum}. Run `agb panel-preflight fixtures` and fix the rest"
        )
    by_dimension: dict[str, int] = {}
    for finding in missed:
        key = str(finding.fixture.dimension)
        by_dimension[key] = by_dimension.get(key, 0) + 1
    return Result(
        name="disclosure_recall",
        passed=not missed,
        measured=f"{len(usable) - len(missed)} of {len(usable)} disclosures found",
        threshold="every usable fixture found in its dimension",
        detail={
            "runs": {as_of.isoformat(): run_id for as_of, run_id in sorted(runs.items())},
            "fixtures": len(found),
            "usable": len(usable),
            "missed": len(missed),
            "by_dimension": by_dimension,
            "findings": [
                {
                    "symbol": f.fixture.symbol,
                    "as_of": f.fixture.as_of.isoformat(),
                    "dimension": f.fixture.dimension,
                    "status": f.status,
                    "detail": f.detail,
                }
                for f in found
            ],
        },
    )


@dataclass(frozen=True)
class Review:
    """One name under spot check: what the panel made of it, and the operator's verdict."""

    retrieval_id: int
    score_id: int
    symbol: str
    company: str | None
    total: int
    score: float
    verdicts: tuple[tuple[str, str | None, str], ...] = ()

    @property
    def verdict(self) -> str | None:
        """The verdict on the name: a disagreement by any operator carries it."""
        if any(verdict == "disagree" for verdict, _, _ in self.verdicts):
            return "disagree"
        return "agree" if self.verdicts else None


_SCORED = """
    SELECT r.id, s.id, r.symbol, r.company, s.total, s.score
    FROM agb.panel_score s
    JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
    WHERE s.run_id = %s AND s.outcome = 'scored'
    ORDER BY s.score, r.symbol
"""
_VERDICTS = """
    SELECT retrieval_id, verdict, note, actor
    FROM agb.panel_spot_check
    WHERE run_id = %s
    ORDER BY id
"""


def sample(conn: psycopg.Connection, run_id: int, n: int = SPOT_CHECK_N) -> list[Review]:
    """The names the operator reviews: `n` of the run's scored names, evenly spaced across the
    score range so the sample carries the panel's best and worst reading of the month and not
    just its middle. The same names every time, so the review can be done over several
    sittings."""
    rows = conn.execute(_SCORED, (run_id,)).fetchall()
    if len(rows) < n:
        raise PreflightError(
            f"panel_run {run_id} scored {len(rows)} names; a spot check of {n} needs that many"
        )
    verdicts: dict[int, list[tuple[str, str | None, str]]] = {}
    for retrieval_id, verdict, note, actor in conn.execute(_VERDICTS, (run_id,)).fetchall():
        verdicts.setdefault(retrieval_id, []).append((verdict, note, actor))
    picked = [rows[round(i * (len(rows) - 1) / (n - 1))] for i in range(n)]
    return [Review(*row, verdicts=tuple(verdicts.get(row[0], ()))) for row in dict.fromkeys(picked)]


def pending(reviews: Sequence[Review]) -> Review | None:
    """The next name awaiting a verdict, or None when the sample is complete."""
    return next((review for review in reviews if review.verdict is None), None)


def review(
    conn: psycopg.Connection,
    run_id: int,
    symbol: str,
    verdict: str,
    note: str | None,
    actor: str,
) -> Review:
    """Record one name's verdict. Only a name in the sample may be reviewed, so what is
    recorded is a verdict on what the operator was shown."""
    if verdict not in ("agree", "disagree"):
        raise PreflightError(f"a verdict is 'agree' or 'disagree', not {verdict!r}")
    if verdict == "disagree" and not (note and note.strip()):
        raise PreflightError("a disagreement needs a note saying what the panel got wrong")
    reviews = sample(conn, run_id)
    found = next((r for r in reviews if r.symbol == symbol), None)
    if found is None:
        raise PreflightError(f"{symbol} is not among the {len(reviews)} names under spot check")
    conn.execute(
        """
        INSERT INTO agb.panel_spot_check
            (run_id, retrieval_id, score_id, verdict, note, actor)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, found.retrieval_id, found.score_id, verdict, note, actor),
    )
    return found


def spot_check(conn: psycopg.Connection, run_id: int, n: int = SPOT_CHECK_N) -> Result:
    """The spot check over one run: every name in the sample reviewed, and none disagreed
    with. The verdict is the operator's own, so this only counts them."""
    reviews = sample(conn, run_id, n)
    reviewed = [r for r in reviews if r.verdict is not None]
    disagreed = [r for r in reviews if r.verdict == "disagree"]
    actors = sorted({actor for r in reviews for _, _, actor in r.verdicts})
    return Result(
        name="spot_check",
        passed=len(reviewed) == len(reviews) and not disagreed,
        measured=f"{len(reviewed)} of {len(reviews)} names reviewed, {len(disagreed)} disagreed",
        threshold=f"all {len(reviews)} reviewed, none disagreed",
        detail={
            "names": len(reviews),
            "reviewed": len(reviewed),
            "disagreed": [
                {
                    "symbol": r.symbol,
                    "total": r.total,
                    "notes": [note for verdict, note, _ in r.verdicts if verdict == "disagree"],
                }
                for r in disagreed
            ],
            "actors": actors,
        },
        run_id=run_id,
    )


_DIMENSIONS = """
    SELECT d.dimension, d.score, d.reason, d.source
    FROM agb.panel_dimension d
    JOIN agb.panel_score s ON s.id = d.score_id
    WHERE s.retrieval_id = %s
    ORDER BY d.dimension
"""
_EVIDENCE_QUOTES = """
    SELECT q.dimension, q.document, q.section, q.scrubbed
    FROM agb.panel_quote q
    JOIN agb.panel_extract e ON e.id = q.extract_id
    WHERE e.retrieval_id = %s AND q.verified
    ORDER BY q.dimension, q.id
"""


def review_text(
    conn: psycopg.Connection, run_id: int, review: Review, reviews: Sequence[Review]
) -> str:
    """One name laid out for the operator: what the panel scored it, dimension by dimension,
    with the quotes it reasoned from, and how to record a verdict."""
    done = sum(1 for r in reviews if r.verdict is not None)
    lines = [
        f"{review.symbol}  {review.company or '?'}",
        f"  total {review.total} of 24, score {review.score:.3f}"
        f"  ({done} of {len(reviews)} names reviewed)",
        "",
    ]
    for dimension, score, reason, source in conn.execute(
        _DIMENSIONS, (review.retrieval_id,)
    ).fetchall():
        rule = "  (capped by rule)" if source == "rule" else ""
        lines.append(f"  {dimension} {analyst.RUBRIC[dimension][0]:<34} {score} of 4{rule}")
        lines.append(f"      {reason}")
    quotes = conn.execute(_EVIDENCE_QUOTES, (review.retrieval_id,)).fetchall()
    lines.append("")
    if quotes:
        lines.append(f"  quotes ({len(quotes)}), as the debate saw them:")
        for dimension, document, section, scrubbed in quotes:
            lines.append(f"    {dimension}  {document} {section}")
            lines.append(f"       {scrubbed}")
    else:
        lines.append("  the Analyst kept no quotes on this name")
    for verdict, note, actor in review.verdicts:
        lines.append(f"  {actor} said {verdict}{': ' + note if note else ''}")
    lines.append("")
    lines.append("  record your verdict with:")
    lines.append(
        f"    agb panel-preflight spot-check {run_id} --symbol {review.symbol} "
        f"--verdict agree|disagree [--note ...]"
    )
    return "\n".join(lines)


_STATUS = """
    SELECT name, passed, measured, threshold, run_id, actor, at
    FROM agb.panel_preflight_status
    WHERE panel_config_id = %s
    ORDER BY seq
"""
_READY = """
    SELECT passed, checks_passed, outstanding
    FROM agb.panel_preflight_ready
    WHERE panel_config_id = %s
"""


def ready(conn: psycopg.Connection, config_id: int) -> tuple[bool, list[str]]:
    """Whether the whole suite has passed under one configuration, and what is outstanding.
    A configuration nobody has pre-flighted has all four checks outstanding, which is what the
    paper-to-live criterion reads."""
    row = conn.execute(_READY, (config_id,)).fetchone()
    if row is None:
        raise PreflightError(f"no panel_config {config_id}")
    passed, _, outstanding = row
    return bool(passed), list(outstanding or ())


def status_text(conn: psycopg.Connection, config_id: int) -> str:
    """The four checks under one configuration, newest result each."""
    passed, outstanding = ready(conn, config_id)
    rows = conn.execute(_STATUS, (config_id,)).fetchall()
    if passed:
        head = f"panel_config {config_id}: pre-flight passed"
    elif len(outstanding) == len(CHECKS):
        head = f"panel_config {config_id}: not yet pre-flighted"
    else:
        head = f"panel_config {config_id}: outstanding — {', '.join(outstanding)}"
    lines = [head, ""]
    for name, check_passed, measured, threshold, run_id, actor, at in rows:
        verdict = "-" if check_passed is None else ("passed" if check_passed else "FAILED")
        lines.append(f"  {name:<19}{verdict:<8}{measured or 'never run'}")
        if check_passed is not None:
            run = f"run {run_id}, " if run_id is not None else ""
            lines.append(
                f"  {'':<19}{'':<8}threshold {threshold}; {run}{actor}, {at:%Y-%m-%d %H:%M} UTC"
            )
    return "\n".join(lines)


def fixtures_text(
    conn: psycopg.Connection,
    store: PanelStore,
    runs: Mapping[date, int],
    fixtures: Sequence[Disclosure] = FIXTURES,
) -> str:
    """Every fixture's standing, so an entry the record does not support can be fixed or
    dropped rather than counted against the panel."""
    found = findings(conn, store, runs, fixtures)
    lines = [f"{len(found)} fixtures, {sum(1 for f in found if f.status != 'unusable')} usable", ""]
    lines += [f"  {finding}" for finding in found]
    return "\n".join(lines)


_RECORDED = """
    SELECT 1 FROM agb.panel_preflight
    WHERE panel_config_id = %s AND name = %s AND run_id IS NOT DISTINCT FROM %s
"""


def recorded(conn: psycopg.Connection, config_id: int, name: str, run_id: int | None) -> bool:
    """Whether this check's result for this run is already on record, so a re-reading of a
    finished check does not append the same result again."""
    return conn.execute(_RECORDED, (config_id, name, run_id)).fetchone() is not None


def detail_text(result: Result) -> str:
    """What the check found, for the console: the part of `detail` an operator acts on."""
    detail = result.detail
    lines: list[str] = []
    if result.name == "retrieval_fidelity":
        for quote in detail.get("quotes", ()):
            lines.append(
                f"  {quote['symbol']:<6} dimension {quote['dimension']}  "
                f"{quote['document']} {quote['section']}: {quote['rejection']}"
            )
            lines.append(f"       {quote['quote']}")
    elif result.name == "disclosure_recall":
        for finding in detail.get("findings", ()):
            if finding["status"] != "found":
                lines.append(
                    f"  {finding['symbol']:<6} {finding['as_of']}  dimension "
                    f"{finding['dimension']}  {finding['status']}: {finding['detail']}"
                )
    elif result.name == "spot_check":
        for name in detail.get("disagreed", ()):
            notes = "; ".join(note for note in name["notes"] if note)
            lines.append(f"  {name['symbol']:<6} {name['total']} of 24  {notes}")
    elif result.name == "dispersion":
        lines.append(f"  lowest {detail['lowest']}, highest {detail['highest']}")
    return "\n".join(lines)
