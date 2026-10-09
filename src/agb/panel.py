"""The LLM panel: filings, Analyst quotes and the scrubbed extract, then the debate and the
rubric score.

`run` gathers the evidence for each shortlisted name, as of the panel's run date
(docs/architecture/03):

1. Retrieve its filings from EDGAR (agb.edgar), recording a delinquent filer, a recent IPO
   read from its registration statement, or an EDGAR failure.
2. Ask the Analyst for verbatim quotes against the rubric (agb.analyst), answering from the
   panel store instead when an identical request was made before, and verify each quote
   against the document it cites.
3. Scrub the company's names and tickers from the verified quotes (agb.scrubber). The result
   is the extract the Prosecutor, Defender and Judge read.

`score` then debates each extract of a run (agb.debate): the Prosecutor argues from it, the
Defender answers, and the Judge scores the six rubric dimensions, which code checks, adjusts
by the filing rules and sums. `replay` recomputes a stored score from the stored Judge's answer
without calling a model.

Every run references a panel_config row, made only by `agb panel-config create`, and refuses
one whose prompt hashes, rubric or scrubber version are not the code's. Every body (filing
documents and texts, requests, responses, extracts) goes to the content-addressed panel store,
and Postgres holds the structured rows that refer to them. Each name is written in its own
transaction, after its network calls have finished.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import psycopg

from agb import analyst, debate, edgar
from agb.panel_store import PanelStore
from agb.scrubber import SCRUBBER_VERSION, Scrubber

log = logging.getLogger(__name__)

ACTOR = "pipeline"
ROLES = ("analyst", "prosecutor", "defender", "judge")
# Names worked on at once. EDGAR's rate limit is enforced by the shared client whatever this is.
WORKERS = 4


class PanelError(Exception):
    """A run that cannot start: no such panel_config, or one the code does not match."""


@dataclass(frozen=True)
class Config:
    id: int
    models: dict[str, str]
    prompt_hashes: dict[str, str]
    rubric_version: str
    scrubber_version: str
    shortlist_n: int
    w: float


def prompt_hashes() -> dict[str, str]:
    """The prompt-template hash of each role."""
    return {"analyst": analyst.template_hash()} | {
        role: debate.template_hash(role) for role in debate.ROLES
    }


def create_config(
    conn: psycopg.Connection,
    actor: str,
    shortlist_n: int,
    w: float,
    models: dict[str, str] | None = None,
) -> int:
    """A new panel_config row for this code's prompts, rubric and scrubber. Only the cli role
    may insert one."""
    models = {**default_models(), **(models or {})}
    row = conn.execute(
        """
        INSERT INTO agb.panel_config
            (analyst_model, prosecutor_model, defender_model, judge_model, prompt_hashes,
             rubric_version, scrubber_version, shortlist_n, w, actor)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            *(models[role] for role in ROLES),
            json.dumps(prompt_hashes(), sort_keys=True),
            analyst.RUBRIC_VERSION,
            SCRUBBER_VERSION,
            shortlist_n,
            w,
            actor,
        ),
    ).fetchone()
    assert row is not None
    return row[0]


def default_models() -> dict[str, str]:
    return {"analyst": analyst.MODEL, **analyst.DEBATE_MODELS, "judge": analyst.JUDGE_MODEL}


def load_config(conn: psycopg.Connection, config_id: int) -> Config:
    row = conn.execute(
        """
        SELECT id, analyst_model, prosecutor_model, defender_model, judge_model, prompt_hashes,
               rubric_version, scrubber_version, shortlist_n, w
        FROM agb.panel_config WHERE id = %s
        """,
        (config_id,),
    ).fetchone()
    if row is None:
        raise PanelError(f"no panel_config {config_id}; create one with `agb panel-config create`")
    return Config(
        id=row[0],
        models=dict(zip(ROLES, row[1:5], strict=True)),
        prompt_hashes=row[5],
        rubric_version=row[6],
        scrubber_version=row[7],
        shortlist_n=row[8],
        w=float(row[9]),
    )


def check(config: Config) -> None:
    """Refuses a configuration made for other prompts, another rubric or another scrubber."""
    stale = [
        f"the {role.capitalize()}'s prompt templates"
        for role, template in prompt_hashes().items()
        if config.prompt_hashes.get(role) != template
    ]
    if config.rubric_version != analyst.RUBRIC_VERSION:
        stale.append(f"rubric version {config.rubric_version}")
    if config.scrubber_version != SCRUBBER_VERSION:
        stale.append(f"scrubber version {config.scrubber_version}")
    if stale:
        raise PanelError(
            f"panel_config {config.id} was made for {', '.join(stale)}, which this code does "
            "not have; create a new one with `agb panel-config create`"
        )


def newest_config(conn: psycopg.Connection) -> Config | None:
    """The configuration a rebalance runs the panel under: the newest row `agb panel-config
    create` has made, or None when there is none. With none the rebalance is stage-1 only."""
    row = conn.execute("SELECT id FROM agb.panel_config ORDER BY id DESC LIMIT 1").fetchone()
    return None if row is None else load_config(conn, row[0])


@dataclass(frozen=True)
class Runner:
    """The panel as the monthly rebalance drives it: `run` to retrieve and extract, then
    `score` to debate, both under one immutable configuration. The rebalance holds one of
    these and knows nothing else of the panel."""

    config: Config
    client: edgar.Edgar
    model: analyst.Model
    store: PanelStore
    workers: int = WORKERS

    @property
    def config_id(self) -> int:
        return self.config.id

    @property
    def shortlist_n(self) -> int:
        return self.config.shortlist_n

    @property
    def w(self) -> float:
        return self.config.w

    def __call__(
        self,
        conn: psycopg.Connection,
        shortlist: Sequence[str],
        sectors: Mapping[str, str],
        as_of: date,
        job_id: int | None = None,
    ) -> int:
        """Score `shortlist` as of `as_of`; returns the run the scores belong to."""
        run_id = run(
            conn,
            self.config.id,
            shortlist,
            as_of,
            self.client,
            self.model,
            self.store,
            job_id,
            self.workers,
        )
        score(conn, run_id, sectors, self.model, self.store, self.workers)
        return run_id


@dataclass
class Name:
    """One name's work, from retrieval to extract, before it is written."""

    retrieval: edgar.Retrieval
    documents: list[analyst.Document] = field(default_factory=list)
    sent: list[int] = field(default_factory=list)
    request: dict[str, Any] | None = None
    request_sha256: str | None = None
    completion: analyst.Completion | None = None
    cached: bool = False
    failure: str | None = None


def run(
    conn: psycopg.Connection,
    config_id: int,
    shortlist: Sequence[str],
    as_of: date,
    client: edgar.Edgar,
    model: analyst.Model,
    store: PanelStore,
    job_id: int | None = None,
    workers: int = WORKERS,
) -> int:
    """Gather the evidence for each name on `shortlist` as of `as_of`; returns the run's id."""
    config = load_config(conn, config_id)
    check(config)
    if len(shortlist) > config.shortlist_n:
        raise PanelError(
            f"shortlist of {len(shortlist)} names exceeds panel_config {config.id}'s N of "
            f"{config.shortlist_n}"
        )
    row = conn.execute(
        "INSERT INTO agb.panel_run (panel_config_id, job_id, as_of, actor) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (config.id, job_id, as_of, ACTOR),
    ).fetchone()
    assert row is not None
    run_id = row[0]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        names = [
            Name(r)
            for r in pool.map(lambda symbol: edgar.retrieve(client, symbol, as_of), shortlist)
        ]
        for name in names:
            _prepare(conn, name, config.models["analyst"], store)
        missed = [n for n in names if n.request is not None and n.completion is None]
        for name, outcome in zip(missed, pool.map(lambda n: _call(model, n), missed), strict=True):
            name.completion, name.failure = outcome

    for name in names:
        with conn.transaction():
            _write(conn, run_id, name, store)
    log.info(
        "panel run %d: %d names, %d model calls, %d from cache",
        run_id,
        len(names),
        len(missed),
        sum(n.cached for n in names),
    )
    return run_id


def _prepare(conn: psycopg.Connection, name: Name, model_id: str, store: PanelStore) -> None:
    """Number the documents, build the Analyst's request, and look for its answer in the cache."""
    retrieval = name.retrieval
    if retrieval.outcome != "retrieved":
        return
    pieces = [
        (s.filing.form, s.filing.filed.isoformat(), s.section, s.text) for s in retrieval.sections
    ]
    name.documents, name.sent = analyst.documents(pieces)
    name.request = analyst.request(
        retrieval.name or retrieval.ticker, retrieval.ticker, name.documents, model_id
    )
    name.request_sha256 = analyst.cache_key(name.request)
    name.completion = cached(conn, store, name.request_sha256)
    name.cached = name.completion is not None


def cached(conn: psycopg.Connection, store: PanelStore, request_sha256: str) -> Any:
    """The answer to an earlier identical request, if one was made and its body is stored."""
    for (response_sha256,) in conn.execute(
        "SELECT response_sha256 FROM agb.panel_model_call WHERE request_sha256 = %s ORDER BY id",
        (request_sha256,),
    ):
        if store.has(response_sha256):
            return analyst.Completion(**json.loads(store.get(response_sha256)))
    return None


def _call(model: analyst.Model, name: Name) -> tuple[analyst.Completion | None, str | None]:
    assert name.request is not None
    try:
        return model.complete(name.request), None
    except analyst.AnalystError as exc:
        log.warning("%s: the Analyst failed: %s", name.retrieval.ticker, exc)
        return None, str(exc)


def _write(conn: psycopg.Connection, run_id: int, name: Name, store: PanelStore) -> None:
    retrieval = name.retrieval
    scrubber = Scrubber(retrieval.aliases, retrieval.tickers)
    row = conn.execute(
        """
        INSERT INTO agb.panel_retrieval
            (run_id, symbol, cik, company, outcome, delinquent, ipo, detail, aliases)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            run_id,
            retrieval.ticker,
            retrieval.cik,
            retrieval.name,
            retrieval.outcome,
            retrieval.delinquent,
            retrieval.ipo,
            retrieval.detail,
            scrubber.aliases,
        ),
    ).fetchone()
    assert row is not None
    retrieval_id = row[0]
    if retrieval.outcome != "retrieved":
        return

    filing_ids: dict[str, int] = {}
    for n, (section, sent) in enumerate(zip(retrieval.sections, name.sent, strict=True), start=1):
        filing = section.filing
        row = conn.execute(
            """
            INSERT INTO agb.panel_filing
                (retrieval_id, document, form, accession, filed, report_date, section,
                 document_sha256, text_sha256, chars, sent_chars)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                retrieval_id,
                f"D{n}",
                filing.form,
                filing.accession,
                filing.filed,
                filing.report_date,
                section.section,
                store.put(section.raw),
                store.put_text(section.text),
                len(section.text),
                sent,
            ),
        ).fetchone()
        assert row is not None
        filing_ids[f"D{n}"] = row[0]

    call_id = None
    if name.completion is not None:
        assert name.request is not None and name.request_sha256 is not None
        call_id = log_call(
            conn,
            run_id,
            retrieval_id,
            "analyst",
            name.request,
            name.request_sha256,
            name.completion,
            name.cached,
            store,
        )
    failure = name.failure
    quotes: list[analyst.Quote] = []
    if name.completion is not None:
        try:
            quotes = analyst.verify(analyst.parse(name.completion), name.documents)
        except analyst.AnalystError as exc:
            failure = str(exc)
    if failure is not None:
        conn.execute(
            """
            INSERT INTO agb.panel_extract
                (retrieval_id, model_call_id, outcome, detail, scrubber_version)
            VALUES (%s, %s, 'analyst_failure', %s, %s)
            """,
            (retrieval_id, call_id, failure, SCRUBBER_VERSION),
        )
        return

    forms = {d.id: d for d in name.documents}
    extract = extract_body(quotes, forms, scrubber)
    row = conn.execute(
        """
        INSERT INTO agb.panel_extract
            (retrieval_id, model_call_id, outcome, scrubber_version, extract_sha256)
        VALUES (%s, %s, 'extracted', %s, %s)
        RETURNING id
        """,
        (retrieval_id, call_id, SCRUBBER_VERSION, store.put(analyst.canonical(extract))),
    ).fetchone()
    assert row is not None
    extract_id = row[0]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO agb.panel_quote
                (extract_id, filing_id, dimension, document, section, quote, verified,
                 rejection, scrubbed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    extract_id,
                    filing_ids.get(q.document),
                    q.dimension,
                    q.document,
                    q.section,
                    q.text,
                    q.verified,
                    q.rejection,
                    scrubber.scrub(q.text) if q.verified else None,
                )
                for q in quotes
            ],
        )
    rejected = sum(not q.verified for q in quotes)
    if rejected:
        log.warning("%s: rejected %d of %d quotes", retrieval.ticker, rejected, len(quotes))


def log_call(
    conn: psycopg.Connection,
    run_id: int,
    retrieval_id: int,
    role: str,
    request: dict[str, Any],
    request_sha256: str,
    completion: analyst.Completion,
    was_cached: bool,
    store: PanelStore,
) -> int:
    """Store a call's request and response bodies and log it, with the model asked for and the
    model that answered; returns the panel_model_call id."""
    stored = store.put(analyst.canonical(request))
    assert stored == request_sha256
    row = conn.execute(
        """
        INSERT INTO agb.panel_model_call
            (run_id, retrieval_id, role, model, response_model, request_sha256,
             response_sha256, cached, input_tokens, output_tokens, cache_read_tokens,
             cache_creation_tokens, stop_reason, request_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            run_id,
            retrieval_id,
            role,
            request["model"],
            completion.model,
            request_sha256,
            store.put(analyst.canonical(asdict(completion))),
            was_cached,
            completion.input_tokens,
            completion.output_tokens,
            completion.cache_read_tokens,
            completion.cache_creation_tokens,
            completion.stop_reason,
            completion.request_id,
        ),
    ).fetchone()
    assert row is not None
    return row[0]


@dataclass
class Call:
    """One debate call: its request, and the answer from the model or the cache."""

    role: str
    request: dict[str, Any]
    request_sha256: str
    completion: analyst.Completion | None = None
    cached: bool = False


@dataclass
class Debate:
    """One name's debate, from extract to score, before it is written."""

    retrieval_id: int
    symbol: str
    sector: str
    delinquent: str | None
    ipo: bool
    extract: dict[str, Any]
    calls: list[Call] = field(default_factory=list)
    answers: dict[str, str] = field(default_factory=dict)
    score: debate.Score | None = None
    failure: str | None = None


def score(
    conn: psycopg.Connection,
    run_id: int,
    sectors: Mapping[str, str],
    model: analyst.Model,
    store: PanelStore,
    workers: int = WORKERS,
) -> int:
    """Debate and score each name `run_id` extracted and has not scored yet, telling the debate
    roles its sector from `sectors` (symbol to sector); returns how many names were debated.

    The roles speak in turn, each stage for every name at once: the Prosecutor, then the
    Defender answering the Prosecutor, then the Judge reading both. A request made before is
    answered from the store. A name whose call fails or whose answer is unusable is recorded as
    a debate failure, with no score."""
    row = conn.execute(
        "SELECT panel_config_id FROM agb.panel_run WHERE id = %s", (run_id,)
    ).fetchone()
    if row is None:
        raise PanelError(f"no panel_run {run_id}")
    config = load_config(conn, row[0])
    check(config)
    rows = conn.execute(
        """
        SELECT r.id, r.symbol, r.delinquent, r.ipo, e.extract_sha256
        FROM agb.panel_retrieval r
        JOIN agb.panel_extract e ON e.retrieval_id = r.id
        WHERE r.run_id = %s AND e.outcome = 'extracted'
          AND NOT EXISTS (SELECT 1 FROM agb.panel_score s WHERE s.retrieval_id = r.id)
        ORDER BY r.id
        """,
        (run_id,),
    ).fetchall()
    missing = [symbol for _, symbol, *_ in rows if not (sectors.get(symbol) or "").strip()]
    if missing:
        raise PanelError(f"no sector for {', '.join(missing)}; the debate needs every name's")
    debates = [
        Debate(
            retrieval_id,
            symbol,
            sectors[symbol].strip(),
            delinquent,
            ipo,
            json.loads(store.get(extract_sha256)),
        )
        for retrieval_id, symbol, delinquent, ipo, extract_sha256 in rows
    ]

    asked = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for role in debate.ROLES:
            live = [d for d in debates if d.failure is None]
            for d in live:
                request = _debate_request(role, config.models[role], d)
                call = Call(role, request, analyst.cache_key(request))
                call.completion = cached(conn, store, call.request_sha256)
                call.cached = call.completion is not None
                d.calls.append(call)
            missed = [d for d in live if d.calls[-1].completion is None]
            asked += len(missed)
            for d, outcome in zip(missed, pool.map(lambda d: _ask(model, d), missed), strict=True):
                d.calls[-1].completion, d.failure = outcome
            for d in live:
                if d.failure is None:
                    _read(d, d.calls[-1])

    for d in debates:
        with conn.transaction():
            _write_score(conn, run_id, config.id, d, store)
    log.info(
        "panel run %d: %d names debated, %d scored, %d model calls",
        run_id,
        len(debates),
        sum(d.failure is None for d in debates),
        asked,
    )
    return len(debates)


def _debate_request(role: str, model_id: str, d: Debate) -> dict[str, Any]:
    case = (model_id, d.sector, debate.status(d.delinquent, d.ipo), d.extract)
    if role == "prosecutor":
        return debate.prosecutor_request(*case)
    if role == "defender":
        return debate.defender_request(*case, d.answers["prosecutor"])
    return debate.judge_request(*case, d.answers["prosecutor"], d.answers["defender"])


def _ask(model: analyst.Model, d: Debate) -> tuple[analyst.Completion | None, str | None]:
    call = d.calls[-1]
    try:
        return model.complete(call.request), None
    except analyst.AnalystError as exc:
        log.warning("%s: the %s failed: %s", d.symbol, call.role.capitalize(), exc)
        return None, f"{call.role}: {exc}"


def _read(d: Debate, call: Call) -> None:
    """Take a role's answer: an advocate's argument, or the Judge's scores with the filing
    rules applied."""
    assert call.completion is not None
    try:
        if call.role == "judge":
            d.score = debate.score(debate.parse(call.completion), d.delinquent, d.ipo)
        else:
            d.answers[call.role] = debate.answer(call.completion)
    except debate.DebateError as exc:
        log.warning("%s: the %s's answer is unusable: %s", d.symbol, call.role.capitalize(), exc)
        d.failure = f"{call.role}: {exc}"


def _write_score(
    conn: psycopg.Connection, run_id: int, config_id: int, d: Debate, store: PanelStore
) -> None:
    call_ids = {
        call.role: log_call(
            conn,
            run_id,
            d.retrieval_id,
            call.role,
            call.request,
            call.request_sha256,
            call.completion,
            call.cached,
            store,
        )
        for call in d.calls
        if call.completion is not None
    }
    result = d.score
    row = conn.execute(
        """
        INSERT INTO agb.panel_score
            (retrieval_id, run_id, panel_config_id, sector, prosecutor_call_id,
             defender_call_id, judge_call_id, outcome, detail, total, score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            d.retrieval_id,
            run_id,
            config_id,
            d.sector,
            call_ids.get("prosecutor"),
            call_ids.get("defender"),
            call_ids.get("judge"),
            "debate_failure" if result is None else "scored",
            d.failure,
            None if result is None else result.total,
            None if result is None else result.value,
        ),
    ).fetchone()
    assert row is not None
    if result is None:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO agb.panel_dimension
                (score_id, dimension, score, reason, source, judge_score, judge_reason)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (row[0], x.dimension, x.score, x.reason, x.source, x.judge_score, x.judge_reason)
                for x in result.dimensions
            ],
        )


def replay(conn: psycopg.Connection, store: PanelStore, score_id: int) -> debate.Score:
    """Recompute a stored score from the stored Judge's answer and the name's filing status,
    without calling a model."""
    row = conn.execute(
        """
        SELECT c.response_sha256, r.delinquent, r.ipo
        FROM agb.panel_score s
        JOIN agb.panel_model_call c ON c.id = s.judge_call_id
        JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
        WHERE s.id = %s AND s.outcome = 'scored'
        """,
        (score_id,),
    ).fetchone()
    if row is None:
        raise PanelError(f"no scored panel_score {score_id}")
    response_sha256, delinquent, ipo = row
    completion = analyst.Completion(**json.loads(store.get(response_sha256)))
    return debate.score(debate.parse(completion), delinquent, ipo)


def extract_body(
    quotes: list[analyst.Quote],
    documents: dict[str, analyst.Document],
    scrubber: Scrubber,
) -> dict[str, Any]:
    """The scrubbed extract the debate roles read: the verified quotes by dimension, each with
    the form, filing date and section it came from. Nothing in it names the company."""
    return {
        "rubric_version": analyst.RUBRIC_VERSION,
        "scrubber_version": SCRUBBER_VERSION,
        "quotes": [
            {
                "dimension": q.dimension,
                "form": documents[q.document].form,
                "filed": documents[q.document].filed,
                "section": scrubber.scrub(q.section),
                "quote": scrubber.scrub(q.text),
            }
            for q in sorted(
                (q for q in quotes if q.verified), key=lambda q: (q.dimension or 0, q.document)
            )
        ],
    }
