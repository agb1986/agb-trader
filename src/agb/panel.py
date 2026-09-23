"""The panel's evidence-gathering half: filings, Analyst quotes and the scrubbed extract.

For each shortlisted name, as of the panel's run date (docs/architecture/03):

1. Retrieve its filings from EDGAR (agb.edgar), recording a delinquent filer, a recent IPO
   read from its registration statement, or an EDGAR failure.
2. Ask the Analyst for verbatim quotes against the rubric (agb.analyst), answering from the
   panel store instead when an identical request was made before, and verify each quote
   against the document it cites.
3. Scrub the company's names and tickers from the verified quotes (agb.scrubber). The result
   is the extract the Prosecutor, Defender and Judge read.

Every run references a panel_config row, made only by `agb panel-config create`, and refuses
one whose prompt hash, rubric or scrubber version is not the code's. Every body (filing
documents and texts, requests, responses, extracts) goes to the content-addressed panel store,
and Postgres holds the structured rows that refer to them. Each name is written in its own
transaction, after its network calls have finished.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

import psycopg

from agb import analyst, edgar
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
    """The prompt-template hash of each role this code has. The debate roles add theirs."""
    return {"analyst": analyst.template_hash()}


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
    stale = []
    if config.prompt_hashes.get("analyst") != analyst.template_hash():
        stale.append("the Analyst's prompt templates")
    if config.rubric_version != analyst.RUBRIC_VERSION:
        stale.append(f"rubric version {config.rubric_version}")
    if config.scrubber_version != SCRUBBER_VERSION:
        stale.append(f"scrubber version {config.scrubber_version}")
    if stale:
        raise PanelError(
            f"panel_config {config.id} was made for {', '.join(stale)}, which this code does "
            "not have; create a new one with `agb panel-config create`"
        )


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
        call_id = _log_call(conn, run_id, retrieval_id, name, store)
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


def _log_call(
    conn: psycopg.Connection, run_id: int, retrieval_id: int, name: Name, store: PanelStore
) -> int:
    completion = name.completion
    assert completion is not None and name.request is not None
    request_sha256 = store.put(analyst.canonical(name.request))
    assert request_sha256 == name.request_sha256
    row = conn.execute(
        """
        INSERT INTO agb.panel_model_call
            (run_id, retrieval_id, role, model, response_model, request_sha256,
             response_sha256, cached, input_tokens, output_tokens, cache_read_tokens,
             cache_creation_tokens, stop_reason, request_id)
        VALUES (%s, %s, 'analyst', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            run_id,
            retrieval_id,
            name.request["model"],
            completion.model,
            request_sha256,
            store.put(analyst.canonical(asdict(completion))),
            name.cached,
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
