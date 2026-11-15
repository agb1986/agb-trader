import json
import re
import threading

import edgar_fake
import psycopg
import pytest
from edgar_fake import AS_OF, FakeEdgar, client
from test_jobs import add_job

from agb import analyst, cli, panel
from agb.analyst import AnalystError, Completion, normalise
from agb.panel_store import PanelStore

SHORTLIST = ["ACME", "LATE", "NEWCO", "SHELL", "DOWN"]

# The passages the fake Analyst quotes wherever they appear, as a real one would.
PASSAGES = [
    (
        4,
        "accounted for 62% of our revenue in fiscal 2025, and the loss of that customer would "
        "materially harm Acme.",
    ),
    (
        1,
        "As of December 31, 2025, ACME was not in compliance with the leverage covenant under its "
        "revolving credit facility",
    ),
    (2, "we identified a material weakness in our internal control over financial reporting"),
    (5, "the Board of Acme Widgets Corp appointed a new Chief Financial Officer."),
    (1, "NewCo Robotics, Inc. has an accumulated deficit of $412 million."),
]
# And one it makes up, citing the first document.
FABRICATED = "Acme Widgets Corp is the subject of an SEC investigation into its revenue."
DOCUMENT = re.compile(r'<document id="(D\d+)"[^>]*section="([^"]*)">\n(.*?)\n</document>', re.S)


class FakeAnalyst:
    """Quotes every passage in PASSAGES that the request's documents contain, plus the
    fabricated one. `fail` names tickers whose call errors, `garble` ones answered in prose."""

    def __init__(self, fail=(), garble=()):
        self.calls = []
        self.fail = set(fail)
        self.garble = set(garble)
        self._lock = threading.Lock()

    def complete(self, request):
        content = request["messages"][0]["content"]
        ticker = re.search(r"\(ticker (\w+)\)", content).group(1)
        with self._lock:
            self.calls.append(ticker)
        if ticker in self.fail:
            raise AnalystError(f"{request['model']}: OverloadedError")
        quotes = [
            {"dimension": dimension, "document": doc, "section": section, "quote": passage}
            for doc, section, text in DOCUMENT.findall(content)
            for dimension, passage in PASSAGES
            if normalise(passage) in normalise(text)
        ]
        quotes.append({"dimension": 3, "document": "D1", "section": "Item 1A", "quote": FABRICATED})
        text = "I found some quotes." if ticker in self.garble else json.dumps({"quotes": quotes})
        return Completion(
            text, f"{request['model']}-20251001", 20_000, 900, 1_800, 0, "end_turn", f"req_{ticker}"
        )


@pytest.fixture
def config(db):
    return panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)


@pytest.fixture
def store(tmp_path):
    return PanelStore(tmp_path / "panel")


def run(db, config, store, model, shortlist=SHORTLIST):
    return panel.run(db.connect("pipeline"), config, shortlist, AS_OF, client(), model, store)


def rows(db, query, *params):
    return db.connect("cli").execute(query, params).fetchall()


def use_cli_env(db, monkeypatch):
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)


def test_only_the_cli_creates_a_panel_config_and_none_is_ever_changed(db, config):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        panel.create_config(db.connect("pipeline"), "pipeline", shortlist_n=50, w=0.2)
    with pytest.raises(psycopg.Error):
        db.connect("cli").execute("UPDATE agb.panel_config SET w = 1")
    with pytest.raises(psycopg.Error):
        db.connect("cli").execute("DELETE FROM agb.panel_config")

    [row] = rows(
        db,
        "SELECT shortlist_n, w::float, analyst_model, judge_model, prompt_hashes, rubric_version "
        "FROM agb.panel_config",
    )
    assert row == (50, 0.2, "claude-haiku-4-5", "claude-opus-5", panel.prompt_hashes(), "1")


def test_panel_config_create_and_list(db, monkeypatch, capsys):
    use_cli_env(db, monkeypatch)

    assert cli.main(["panel-config", "create"]) == 0
    assert capsys.readouterr().out == "panel_config 1 created\n"
    argv = ["panel-config", "create", "--n", "40", "--w", "0", "--judge-model", "claude-opus-5-5"]
    assert cli.main([*argv, "--actor", "alice"]) == 0
    assert cli.main(["panel-config", "list"]) == 0

    header, first, second = capsys.readouterr().out.splitlines()[1:]
    assert header.split()[:3] == ["ID", "CREATED", "(UTC)"]
    assert first.split()[0] == "1" and first.split()[3:5] == ["50", "0.20"]
    assert second.split()[3:] == [
        "40",
        "0.00",
        "1",
        "1",
        "claude-haiku-4-5/claude-sonnet-5/claude-sonnet-5/claude-opus-5-5",
        "alice",
    ]


def test_a_run_refuses_a_config_that_is_missing_stale_or_too_small(db, config, store):
    model = FakeAnalyst()
    with pytest.raises(panel.PanelError, match="no panel_config 99"):
        run(db, 99, store, model)

    stale = (
        db.connect("cli")
        .execute(
            """
        INSERT INTO agb.panel_config
            (analyst_model, prosecutor_model, defender_model, judge_model, prompt_hashes,
             rubric_version, scrubber_version, shortlist_n, w, actor)
        VALUES ('m', 'm', 'm', 'm', '{"analyst": "0"}', '0', '1', 50, 0.2, 'operator')
        RETURNING id
        """
        )
        .fetchone()[0]
    )
    with pytest.raises(panel.PanelError, match="prompt templates, rubric version 0"):
        run(db, stale, store, model)

    small = panel.create_config(db.connect("cli"), "operator", shortlist_n=4, w=0.2)
    with pytest.raises(panel.PanelError, match="5 names exceeds panel_config 3's N of 4"):
        run(db, small, store, model)

    assert rows(db, "SELECT count(*) FROM agb.panel_run") == [(0,)]
    assert model.calls == []


def test_a_run_retrieves_quotes_and_scrubs_each_name(db, config, store):
    model = FakeAnalyst()

    run_id = run(db, config, store, model)

    assert rows(
        db, "SELECT panel_config_id, as_of, actor FROM agb.panel_run WHERE id = %s", run_id
    ) == [(config, AS_OF, "pipeline")]
    retrievals = {
        symbol: rest
        for symbol, *rest in rows(
            db,
            "SELECT symbol, outcome, delinquent, ipo, detail IS NOT NULL FROM agb.panel_retrieval",
        )
    }
    assert retrievals == {
        "ACME": ["retrieved", None, False, False],
        "LATE": ["retrieved", "nt_10k", False, False],
        "NEWCO": ["retrieved", None, True, False],
        "SHELL": ["no_filings", None, False, True],
        "DOWN": ["edgar_failure", None, False, True],
    }
    assert sorted(model.calls) == ["ACME", "LATE", "NEWCO"]

    filings = rows(
        db,
        """
        SELECT f.document, f.form, f.section, f.text_sha256, f.document_sha256, f.chars,
               f.sent_chars
        FROM agb.panel_filing f JOIN agb.panel_retrieval r ON r.id = f.retrieval_id
        WHERE r.symbol = 'ACME' ORDER BY f.id
        """,
    )
    assert [f[:3] for f in filings] == [
        ("D1", "10-K", "Item 1A"),
        ("D2", "10-K", "Item 7"),
        ("D3", "10-Q", "Full text"),
        ("D4", "8-K", "Full text"),
        ("D5", "8-K", "Full text"),
    ]
    assert store.get_text(filings[0][3]).startswith("Item 1A. Risk Factors")
    assert store.get(filings[0][4]).startswith(b"<html>")
    assert all(chars == sent > 0 for *_, chars, sent in filings)

    calls = rows(
        db,
        """
        SELECT c.role, c.model, c.response_model, c.cached, c.input_tokens, c.request_id,
               c.request_sha256, c.response_sha256
        FROM agb.panel_model_call c WHERE c.run_id = %s ORDER BY c.request_id
        """,
        run_id,
    )
    assert [c[:6] for c in calls] == [
        ("analyst", "claude-haiku-4-5", "claude-haiku-4-5-20251001", False, 20_000, f"req_{t}")
        for t in ("ACME", "LATE", "NEWCO")
    ]
    for *_, request_sha256, response_sha256 in calls:
        request = json.loads(store.get(request_sha256))
        assert request["temperature"] == 0 and analyst.cache_key(request) == request_sha256
        assert json.loads(store.get(response_sha256))["request_id"].startswith("req_")

    quotes = rows(
        db,
        """
        SELECT q.document, q.dimension, q.verified, q.rejection, q.scrubbed, q.filing_id IS NOT NULL
        FROM agb.panel_quote q JOIN agb.panel_extract e ON e.id = q.extract_id
        JOIN agb.panel_retrieval r ON r.id = e.retrieval_id
        WHERE r.symbol = 'ACME' ORDER BY q.id
        """,
    )
    assert [q[:3] for q in quotes] == [
        ("D1", 4, True),
        ("D2", 1, True),
        ("D3", 2, True),
        ("D4", 5, True),
        ("D5", 5, True),
        ("D1", 3, False),
    ]
    assert quotes[-1][3:] == ("not found verbatim in D1", None, True)
    assert quotes[1][4] == (
        "As of December 31, 2025, the Company was not in compliance with the leverage covenant "
        "under its revolving credit facility"
    )


def extract(db, store, symbol, run_id=None):
    [(outcome, detail, sha)] = rows(
        db,
        """
        SELECT e.outcome, e.detail, e.extract_sha256
        FROM agb.panel_extract e JOIN agb.panel_retrieval r ON r.id = e.retrieval_id
        WHERE r.symbol = %s AND (%s::bigint IS NULL OR r.run_id = %s)
        """,
        symbol,
        run_id,
        run_id,
    )
    return outcome, detail, sha and json.loads(store.get(sha))


def test_a_run_beats_as_each_retrieval_and_each_call_lands(db, config, store):
    """A run of a real shortlist takes far longer than the heartbeat's staleness window, and
    the service cannot beat from its own loop while it is inside one. So the run reports every
    retrieval and every model call to whoever is watching its liveness."""
    model = FakeAnalyst()
    beats = []

    panel.run(
        db.connect("pipeline"),
        config,
        SHORTLIST,
        AS_OF,
        client(),
        model,
        store,
        beat=lambda: beats.append(len(beats)),
    )

    assert len(beats) == len(SHORTLIST) + len(model.calls)


def test_the_extract_holds_only_verified_quotes_and_never_the_name(db, config, store):
    run(db, config, store, FakeAnalyst())

    outcome, _, body = extract(db, store, "ACME")
    assert outcome == "extracted"
    assert [(q["dimension"], q["form"], q["filed"]) for q in body["quotes"]] == [
        (1, "10-K", "2026-02-20"),
        (2, "10-Q", "2026-05-01"),
        (4, "10-K", "2026-02-20"),
        (5, "8-K", "2026-06-10"),
        (5, "8-K", "2026-08-15"),
    ]
    assert body["quotes"][2]["quote"].endswith("would materially harm the Company.")
    assert (
        body["quotes"][3]["quote"]
        == "the Board of the Company appointed a new Chief Financial Officer."
    )
    text = json.dumps(body)
    assert "acme" not in text.lower() and "SEC investigation" not in text

    _, _, newco = extract(db, store, "NEWCO")
    assert [q["quote"] for q in newco["quotes"]] == [
        "The Company has an accumulated deficit of $412 million."
    ]
    assert "newco" not in json.dumps(newco).lower() and "NEWC" not in json.dumps(newco)
    assert rows(db, "SELECT count(*) FROM agb.panel_extract") == [(3,)]


def test_a_rerun_on_identical_inputs_is_answered_from_the_cache(db, config, store):
    model = FakeAnalyst()
    first = run(db, config, store, model)
    assert len(model.calls) == 3

    second = run(db, config, store, model)

    assert len(model.calls) == 3
    # Names are written as they finish, so the order is the request's, not the retrieval's.
    calls = (
        "SELECT request_sha256, response_sha256, cached FROM agb.panel_model_call "
        "WHERE run_id = %s ORDER BY request_sha256"
    )
    before, after = rows(db, calls, first), rows(db, calls, second)
    assert [c[:2] for c in after] == [c[:2] for c in before]
    assert [c[2] for c in before] == [False] * 3 and [c[2] for c in after] == [True] * 3
    for symbol in ("ACME", "LATE", "NEWCO"):
        assert extract(db, store, symbol, second) == extract(db, store, symbol, first)

    # Anything that changes the request is a new call.
    third = panel.create_config(
        db.connect("cli"),
        "operator",
        shortlist_n=50,
        w=0.2,
        models={"analyst": "claude-haiku-4-5-20251001"},
    )
    run(db, third, store, model, ["ACME"])
    assert len(model.calls) == 4


def test_a_failed_or_garbled_answer_is_an_analyst_failure(db, config, store):
    model = FakeAnalyst(fail={"ACME"}, garble={"NEWCO"})

    run_id = run(db, config, store, model)

    assert extract(db, store, "ACME")[:2] == (
        "analyst_failure",
        "claude-haiku-4-5: OverloadedError",
    )
    assert extract(db, store, "NEWCO")[:2] == ("analyst_failure", "the answer is not JSON")
    assert extract(db, store, "LATE")[0] == "extracted"
    logged = rows(
        db,
        """
        SELECT r.symbol, e.model_call_id IS NOT NULL
        FROM agb.panel_extract e JOIN agb.panel_retrieval r ON r.id = e.retrieval_id
        WHERE r.run_id = %s ORDER BY r.symbol
        """,
        run_id,
    )
    assert logged == [("ACME", False), ("LATE", True), ("NEWCO", True)]

    # A call that failed left nothing to answer from, and an answer that could not be read is
    # not replayed, so the next run asks again for both; LATE's good answer is replayed.
    again = FakeAnalyst()
    run(db, config, store, again)
    assert sorted(again.calls) == ["ACME", "NEWCO"]
    assert extract(db, store, "ACME", run_id + 1)[0] == "extracted"
    assert extract(db, store, "NEWCO", run_id + 1)[0] == "extracted"


def test_a_missing_body_is_not_a_cache_hit(db, config, store):
    model = FakeAnalyst()
    run(db, config, store, model, ["ACME"])
    [(response_sha256,)] = rows(db, "SELECT response_sha256 FROM agb.panel_model_call")
    store.path(response_sha256).unlink()

    run(db, config, store, model, ["ACME"])

    assert model.calls == ["ACME", "ACME"]


def test_edgar_is_asked_with_the_user_agent_and_nothing_after_the_as_of_date(db, config, store):
    fake = FakeEdgar()
    panel.run(db.connect("pipeline"), config, ["ACME"], AS_OF, client(fake), FakeAnalyst(), store)

    assert {agent for _, agent in fake.requests} == {"agb-trader test test@example.com"}
    assert not any(url.endswith("/q3.htm") for url, _ in fake.requests)


class Crash(FakeAnalyst):
    """Answers until its `at`th call, then fails the way a lost process or database would: not
    with an AnalystError, which is a name's failure, but with an error that ends the pass."""

    def __init__(self, at):
        super().__init__()
        self.at = at

    def complete(self, request):
        answer = super().complete(request)
        if len(self.calls) >= self.at:
            raise RuntimeError("connection to the pipeline lost")
        return answer


def states(db, run_id):
    return rows(
        db,
        "SELECT stage, state, detail FROM agb.panel_run_state WHERE run_id = %s ORDER BY id",
        run_id,
    )


def test_a_pass_that_dies_keeps_each_finished_name_and_its_retry_resumes(
    db, config, store, monkeypatch
):
    # Fifty regular filers, each with ACME's filings under its own CIK and ticker.
    shortlist = [f"Q{chr(65 + n // 26)}{chr(65 + n % 26)}" for n in range(50)]
    for n, ticker in enumerate(shortlist):
        company = {**edgar_fake.COMPANIES["ACME"], "cik": 5000 + n, "name": f"{ticker} CORP"}
        monkeypatch.setitem(edgar_fake.COMPANIES, ticker, company | {"tickers": [ticker]})
    fake = FakeEdgar()
    job_id = add_job(db)

    def go(model):
        return panel.run(
            db.connect("pipeline"),
            config,
            shortlist,
            AS_OF,
            client(fake),
            model,
            store,
            job_id=job_id,
            workers=1,
        )

    # The 49th call dies; the 48 names answered before it are each in the database already.
    crash = Crash(at=49)
    with pytest.raises(RuntimeError, match="connection to the pipeline lost"):
        go(crash)

    [(run_id,)] = rows(db, "SELECT id FROM agb.panel_run")
    stored = rows(
        db,
        "SELECT r.symbol FROM agb.panel_retrieval r JOIN agb.panel_extract e "
        "ON e.retrieval_id = r.id WHERE r.run_id = %s AND e.outcome = 'extracted' ORDER BY r.id",
        run_id,
    )
    stored = {symbol for (symbol,) in stored}
    assert len(stored) == 48
    assert states(db, run_id) == [
        ("evidence", "interrupted", "RuntimeError: connection to the pipeline lost")
    ]

    # The retry of the same job resumes the run and asks only for the two names it lacks.
    retry = FakeAnalyst()
    assert go(retry) == run_id
    assert len(retry.calls) == 2
    assert set(retry.calls) == set(shortlist) - stored
    assert rows(db, "SELECT count(*) FROM agb.panel_run") == [(1,)]
    assert rows(
        db,
        "SELECT count(*), count(*) FILTER (WHERE e.outcome = 'extracted') "
        "FROM agb.panel_retrieval r JOIN agb.panel_extract e ON e.retrieval_id = r.id "
        "WHERE r.run_id = %s",
        run_id,
    ) == [(50, 50)]
    assert states(db, run_id)[-1] == ("evidence", "completed", None)


def test_a_run_outside_a_job_or_for_another_shortlist_starts_afresh(db, config, store):
    job_id = add_job(db)
    pipeline = db.connect("pipeline")
    model = FakeAnalyst()

    first = panel.run(pipeline, config, ["ACME"], AS_OF, client(), model, store, job_id=job_id)
    # A shortlist without a name the earlier pass stored is not the same pass.
    second = panel.run(pipeline, config, ["LATE"], AS_OF, client(), model, store, job_id=job_id)
    third = run(db, config, store, model, ["ACME"])

    assert len({first, second, third}) == 3
    for run_id in (first, second, third):
        assert states(db, run_id) == [("evidence", "completed", None)]


def test_every_pass_ends_completed_or_interrupted(db, config, store):
    with pytest.raises(RuntimeError):
        run(db, config, store, Crash(at=1))
    done = run(db, config, store, FakeAnalyst())
    with pytest.raises(panel.PanelError, match="exceeds"):
        run(db, config, store, FakeAnalyst(), [f"X{n}" for n in range(51)])

    assert states(db, done - 1) == [
        ("evidence", "interrupted", "RuntimeError: connection to the pipeline lost")
    ]
    assert states(db, done) == [("evidence", "completed", None)]
    # A shortlist refused before anything was started leaves no run at all.
    assert rows(db, "SELECT count(*) FROM agb.panel_run") == [(2,)]
    assert (
        rows(
            db,
            "SELECT r.id FROM agb.panel_run r WHERE NOT EXISTS "
            "(SELECT 1 FROM agb.panel_run_state s WHERE s.run_id = r.id)",
        )
        == []
    )
    with pytest.raises(psycopg.Error):
        db.connect("pipeline").execute("UPDATE agb.panel_run_state SET state = 'completed'")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        db.connect("cli").execute(
            "INSERT INTO agb.panel_run_state (run_id, stage, state, actor) "
            "VALUES (%s, 'evidence', 'completed', 'cli')",
            (done,),
        )
