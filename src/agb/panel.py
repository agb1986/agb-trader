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
transaction as soon as its network calls have landed, so a pass that fails part-way keeps what
it finished and has paid for; how each pass ended is recorded in agb.panel_run_state.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import psycopg

from agb import analyst, debate, edgar
from agb.panel_store import PanelStore, StoreError
from agb.scrubber import SCRUBBER_VERSION, Scrubber

log = logging.getLogger(__name__)

ACTOR = "pipeline"
ROLES = ("analyst", "prosecutor", "defender", "judge")
# Names worked on at once. EDGAR's rate limit is enforced by the shared client whatever this is.
WORKERS = 4


def _nothing() -> None:
    """The default beat: a run nobody is watching the liveness of."""


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
    beat: Callable[[], None] = _nothing

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
            self.beat,
        )
        score(conn, run_id, sectors, self.model, self.store, self.workers, self.beat)
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
    beat: Callable[[], None] = _nothing,
) -> int:
    """Gather the evidence for each name on `shortlist` as of `as_of`; returns the run's id.

    Each name is written as soon as its retrieval and its Analyst call have landed, so a pass
    that fails part-way keeps every name it finished, and the pass is recorded in
    agb.panel_run_state as completed or interrupted. A retry of the same job resumes the run
    it started, under the same configuration and date, and works only on the names that run
    has not stored. `beat` is called as each retrieval and each model call lands, for a caller
    whose liveness is watched while this runs."""
    config = load_config(conn, config_id)
    check(config)
    if len(shortlist) > config.shortlist_n:
        raise PanelError(
            f"shortlist of {len(shortlist)} names exceeds panel_config {config.id}'s N of "
            f"{config.shortlist_n}"
        )
    run_id, stored = _resumable(conn, config.id, job_id, as_of, shortlist)
    if run_id is None:
        row = conn.execute(
            "INSERT INTO agb.panel_run (panel_config_id, job_id, as_of, actor) "
            "VALUES (%s, %s, %s, %s) RETURNING id",
            (config.id, job_id, as_of, ACTOR),
        ).fetchone()
        assert row is not None
        run_id = row[0]
    else:
        log.info(
            "panel run %d: resuming job %d with %d of %d names stored",
            run_id,
            job_id,
            len(stored),
            len(shortlist),
        )
    pending = [symbol for symbol in shortlist if symbol not in stored]

    try:
        asked, hits = _evidence(
            conn,
            run_id,
            pending,
            as_of,
            client,
            model,
            config.models["analyst"],
            store,
            workers,
            beat,
        )
    except BaseException as exc:
        _interrupted(conn, run_id, "evidence", exc)
        raise
    _mark(conn, run_id, "evidence", "completed")
    log.info(
        "panel run %d: %d names, %d model calls, %d from cache", run_id, len(pending), asked, hits
    )
    return run_id


def _resumable(
    conn: psycopg.Connection,
    config_id: int,
    job_id: int | None,
    as_of: date,
    shortlist: Sequence[str],
) -> tuple[int | None, set[str]]:
    """The run an earlier pass of this job started, and the names it stored; or no run, when
    there was none or it stored a name this shortlist no longer has."""
    if job_id is None:
        return None, set()
    row = conn.execute(
        "SELECT id FROM agb.panel_run WHERE panel_config_id = %s AND job_id = %s AND as_of = %s "
        "ORDER BY id DESC LIMIT 1",
        (config_id, job_id, as_of),
    ).fetchone()
    if row is None:
        return None, set()
    stored = {
        symbol
        for (symbol,) in conn.execute(
            "SELECT symbol FROM agb.panel_retrieval WHERE run_id = %s", (row[0],)
        )
    }
    if not stored <= set(shortlist):
        return None, set()
    return row[0], stored


def _evidence(
    conn: psycopg.Connection,
    run_id: int,
    symbols: Sequence[str],
    as_of: date,
    client: edgar.Edgar,
    model: analyst.Model,
    model_id: str,
    store: PanelStore,
    workers: int,
    beat: Callable[[], None],
) -> tuple[int, int]:
    """Retrieve, ask and write each of `symbols`, each name in its own transaction as soon as
    its network calls have landed; returns the model calls made and the answers from the cache.

    On any failure the calls already in flight are waited for and the names they answer
    written, since they are paid for either way, and nothing else is started."""
    asked = hits = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        retrievals = {
            pool.submit(edgar.retrieve, client, symbol, as_of): symbol for symbol in symbols
        }
        calls: dict[Future[tuple[analyst.Completion | None, str | None]], Name] = {}
        try:
            while retrievals or calls:
                done, _ = wait([*retrievals, *calls], return_when=FIRST_COMPLETED)
                for future in done:
                    beat()
                    if future in retrievals:
                        del retrievals[future]
                        name = Name(future.result())
                        _prepare(conn, name, model_id, store)
                        hits += name.cached
                        if name.request is not None and name.completion is None:
                            calls[pool.submit(_call, model, name)] = name
                            asked += 1
                            continue
                    else:
                        name = calls.pop(future)
                        name.completion, name.failure = future.result()
                    with conn.transaction():
                        _write(conn, run_id, name, store)
        except BaseException:
            pool.shutdown(wait=False, cancel_futures=True)
            _salvage(calls, lambda name, outcome: _keep(conn, run_id, name, outcome, store))
            raise
    return asked, hits


def _keep(
    conn: psycopg.Connection,
    run_id: int,
    name: Name,
    outcome: tuple[analyst.Completion | None, str | None],
    store: PanelStore,
) -> None:
    name.completion, name.failure = outcome
    with conn.transaction():
        _write(conn, run_id, name, store)


def _salvage[T, R](futures: Mapping[Future[R], T], keep: Callable[[T, R], None]) -> None:
    """After a pass has failed: wait for the model calls in `futures` that were already in
    flight, and `keep` each that answered. Stops at the first that cannot be kept, which means
    the database is what failed.

    The pool must already be shut down with its queued calls cancelled. Those are left out
    before waiting: `shutdown(cancel_futures=True)` cancels them without notifying waiters, so
    `as_completed` would wait for them forever."""
    started = [future for future in futures if not future.cancelled()]
    for future in as_completed(started):
        if future.exception() is not None:
            continue
        try:
            keep(futures[future], future.result())
        except Exception:
            log.exception("could not keep an answer that landed after the panel failed")
            return


def _mark(
    conn: psycopg.Connection, run_id: int, stage: str, state: str, detail: str | None = None
) -> None:
    conn.execute(
        "INSERT INTO agb.panel_run_state (run_id, stage, state, detail, actor) "
        "VALUES (%s, %s, %s, %s, %s)",
        (run_id, stage, state, detail, ACTOR),
    )


def _interrupted(conn: psycopg.Connection, run_id: int, stage: str, exc: BaseException) -> None:
    """Record that a pass stopped on `exc`, without hiding `exc` if the record fails too."""
    try:
        _mark(conn, run_id, stage, "interrupted", f"{type(exc).__name__}: {exc}")
    except psycopg.Error:
        log.exception("panel run %d: could not record that the %s pass stopped", run_id, stage)


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
    name.completion = cached(conn, store, name.request_sha256, USABLE["analyst"])
    name.cached = name.completion is not None


# What makes a stored answer worth replaying, by role: one that raises is asked for again.
USABLE: dict[str, Callable[[analyst.Completion], object]] = {
    "analyst": analyst.parse,
    "prosecutor": debate.answer,
    "defender": debate.answer,
    "judge": debate.parse,
}


def cached(
    conn: psycopg.Connection,
    store: PanelStore,
    request_sha256: str,
    usable: Callable[[analyst.Completion], object],
) -> analyst.Completion | None:
    """The answer to an earlier identical request, if one was made, its body is stored and
    sound, and `usable` accepts it. An answer that was cut off, refused or garbled is logged
    with its call but never replayed, and neither is one whose body has gone bad, so the next
    run asks again instead of inheriting the failure."""
    for (response_sha256,) in conn.execute(
        "SELECT response_sha256 FROM agb.panel_model_call WHERE request_sha256 = %s ORDER BY id",
        (request_sha256,),
    ):
        if not store.has(response_sha256):
            continue
        try:
            completion = analyst.Completion(**json.loads(store.get(response_sha256)))
        except StoreError as exc:
            # A corrupt body is not a cache hit: the model is asked again, and storing its
            # answer rewrites the body if the answer is the same one.
            log.warning("cached answer %s is unreadable, asking again: %s", response_sha256, exc)
            continue
        try:
            usable(completion)
        except (analyst.AnalystError, debate.DebateError):
            continue
        return completion
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
    failed = "analyst_failure"
    quotes: list[analyst.Quote] = []
    if name.completion is not None:
        try:
            quotes = analyst.verify(analyst.parse(name.completion), name.documents)
        except analyst.Refusal as exc:
            failure, failed = str(exc), "analyst_refusal"
        except analyst.AnalystError as exc:
            failure = str(exc)
    if failure is not None:
        conn.execute(
            """
            INSERT INTO agb.panel_extract
                (retrieval_id, model_call_id, outcome, detail, scrubber_version)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (retrieval_id, call_id, failed, failure, SCRUBBER_VERSION),
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
    id: int | None = None


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
    refused: bool = False


def score(
    conn: psycopg.Connection,
    run_id: int,
    sectors: Mapping[str, str],
    model: analyst.Model,
    store: PanelStore,
    workers: int = WORKERS,
    beat: Callable[[], None] = _nothing,
) -> int:
    """Debate and score each name `run_id` extracted and has not scored yet, telling the debate
    roles its sector from `sectors` (symbol to sector); returns how many names were debated.

    The roles speak in turn, each stage for every name at once: the Prosecutor, then the
    Defender answering the Prosecutor, then the Judge reading both. A request made before is
    answered from the store. A name whose call fails or whose answer is unusable is recorded as
    a debate failure, with no score. Each call is logged as it lands and each name's score is
    written as its Judge answers, so a pass that fails part-way keeps what it finished and a
    rerun answers those calls from the store; the pass is recorded in agb.panel_run_state."""
    row = conn.execute(
        "SELECT panel_config_id FROM agb.panel_run WHERE id = %s", (run_id,)
    ).fetchone()
    if row is None:
        raise PanelError(f"no panel_run {run_id}")
    try:
        debated, scored, asked = _debate(conn, run_id, row[0], sectors, model, store, workers, beat)
    except BaseException as exc:
        _interrupted(conn, run_id, "score", exc)
        raise
    _mark(conn, run_id, "score", "completed")
    log.info(
        "panel run %d: %d names debated, %d scored, %d model calls",
        run_id,
        debated,
        scored,
        asked,
    )
    return debated


def _debate(
    conn: psycopg.Connection,
    run_id: int,
    config_id: int,
    sectors: Mapping[str, str],
    model: analyst.Model,
    store: PanelStore,
    workers: int,
    beat: Callable[[], None],
) -> tuple[int, int, int]:
    """The body of `score`; returns the names debated and scored, and the model calls made."""
    config = load_config(conn, config_id)
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
        pending: dict[Future[tuple[analyst.Completion | None, str | None]], Debate] = {}
        try:
            for role in debate.ROLES:
                for d in [d for d in debates if d.failure is None]:
                    request = _debate_request(role, config.models[role], d)
                    call = Call(role, request, analyst.cache_key(request))
                    call.completion = cached(conn, store, call.request_sha256, USABLE[role])
                    call.cached = call.completion is not None
                    d.calls.append(call)
                    if call.completion is None:
                        pending[pool.submit(_ask, model, d)] = d
                    else:
                        _land(conn, run_id, config.id, d, store)
                asked += len(pending)
                for future in as_completed(list(pending)):
                    beat()
                    d = pending.pop(future)
                    d.calls[-1].completion, d.failure = future.result()
                    _land(conn, run_id, config.id, d, store)
        except BaseException:
            pool.shutdown(wait=False, cancel_futures=True)
            _salvage(
                pending, lambda d, outcome: _keep_debate(conn, run_id, config.id, d, outcome, store)
            )
            raise
    return len(debates), sum(d.failure is None for d in debates), asked


def _land(
    conn: psycopg.Connection, run_id: int, config_id: int, d: Debate, store: PanelStore
) -> None:
    """Log the call `d` has just had answered and take its answer, then write the name's score
    if this was its last call: the Judge's, or one that failed."""
    call = d.calls[-1]
    with conn.transaction():
        if call.completion is not None:
            call.id = log_call(
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
            _read(d, call)
        if d.failure is not None or call.role == "judge":
            _write_score(conn, run_id, config_id, d)


def _keep_debate(
    conn: psycopg.Connection,
    run_id: int,
    config_id: int,
    d: Debate,
    outcome: tuple[analyst.Completion | None, str | None],
    store: PanelStore,
) -> None:
    """Keep an answer that landed after the pass failed: log the call, so a rerun answers it
    from the store, and write the name's score if that was its last call."""
    d.calls[-1].completion, d.failure = outcome
    _land(conn, run_id, config_id, d, store)


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
        d.refused = isinstance(exc, debate.Refusal)


def _write_score(conn: psycopg.Connection, run_id: int, config_id: int, d: Debate) -> None:
    call_ids = {call.role: call.id for call in d.calls if call.id is not None}
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
            "scored" if result is not None else "debate_refusal" if d.refused else "debate_failure",
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
