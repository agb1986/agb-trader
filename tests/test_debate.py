import json
import threading

import psycopg
import pytest
from test_panel import SHORTLIST, FakeAnalyst, rows, run

from agb import analyst, debate, panel
from agb.analyst import AnalystError, Completion
from agb.panel_store import PanelStore
from agb.scrubber import SCRUBBER_VERSION

NAMES = [*SHORTLIST, "STALE"]
SECTORS = {
    "ACME": "Industrials",
    "LATE": "Consumer Cyclical",
    "NEWCO": "Technology",
    "SHELL": "Financial Services",
    "DOWN": "Energy",
    "STALE": "Healthcare",
}
# The debate roles never see a ticker, so the fake tells the names apart by the filing status
# they are told, which differs for each name that reaches the debate.
STATUS = {
    debate.STATUS["regular"]: "ACME",
    debate.STATUS["nt_10k"]: "LATE",
    debate.STATUS["late_10k"]: "STALE",
    debate.STATUS["ipo"]: "NEWCO",
}
# The Judge's six scores for each name.
JUDGE = {
    "ACME": [1, 2, 3, 4, 4, 3],
    "LATE": [3, 4, 2, 1, 4, 4],
    "STALE": [0, 4, 4, 4, 4, 4],
    "NEWCO": [2, 4, 4, 3, 4, 0],
}
# What identifies each name, which none of its debate requests may contain. (The fake EDGAR
# serves ACME's documents for LATE and STALE too, so their extracts may still say ACME.)
IDENTIFIERS = {
    "ACME": ["acme"],
    "LATE": ["late filer", "LATE"],
    "STALE": ["stale co", "STALE"],
    "NEWCO": ["newco", "NEWC"],
}


class FakeDebate:
    """The Prosecutor, Defender and Judge, told apart by their system prompts. The Judge adds a
    wrong total and normalisation of its own to every answer. `broken` maps (symbol, role) to
    what goes wrong with that call: 'error', 'refusal', 'max_tokens', 'prose' or 'range'."""

    def __init__(self, broken=None):
        self.broken = broken or {}
        self.requests = []
        self._lock = threading.Lock()

    def complete(self, request):
        system = request["system"][0]["text"]
        role = next(r for r in debate.ROLES if system.startswith(f"You are the {r.capitalize()}"))
        content = request["messages"][0]["content"]
        status = content.split("\n")[1].removeprefix("Filing status: ")
        symbol = STATUS[status]
        with self._lock:
            self.requests.append((symbol, role, request))
        fault = self.broken.get((symbol, role))
        if fault == "error":
            raise AnalystError(f"{request['model']}: OverloadedError")
        stop = fault if fault in ("refusal", "max_tokens") else "end_turn"
        if role == "judge":
            scores = JUDGE[symbol]
            if fault == "range":
                scores = [5, *scores[1:]]
            body = {
                f"dimension_{n}": {"score": s, "reason": f"Dimension {n} scores {s}."}
                for n, s in enumerate(scores, start=1)
            }
            body |= {"total": 99, "normalised": 0.99}
            text = "The Company looks fine." if fault == "prose" else json.dumps(body)
        else:
            text = f"The {role}'s argument on the {SECTORS[symbol]} case."
        return Completion(
            text, f"{request['model']}-20260101", 3_000, 400, 0, 0, stop, f"req_{symbol}_{role}"
        )

    def roles(self, symbol):
        return [role for s, role, _ in self.requests if s == symbol]


class NoModel:
    def complete(self, request):
        raise AssertionError("a model was called")


@pytest.fixture
def config(db):
    return panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)


@pytest.fixture
def store(tmp_path):
    return PanelStore(tmp_path / "panel")


def debated(db, config, store, model, sectors=SECTORS, analyst_model=None, names=NAMES):
    run_id = run(db, config, store, analyst_model or FakeAnalyst(), names)
    return run_id, panel.score(db.connect("pipeline"), run_id, sectors, model, store)


def scores(db, run_id):
    return {
        symbol: rest
        for symbol, *rest in rows(
            db,
            """
            SELECT r.symbol, s.outcome, s.detail, s.total, s.score, s.sector, s.panel_config_id
            FROM agb.panel_score s JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
            WHERE s.run_id = %s
            """,
            run_id,
        )
    }


def dimensions(db, symbol, run_id):
    return rows(
        db,
        """
        SELECT d.dimension, d.score, d.reason, d.source, d.judge_score, d.judge_reason
        FROM agb.panel_dimension d
        JOIN agb.panel_score s ON s.id = d.score_id
        JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
        WHERE r.symbol = %s AND s.run_id = %s
        ORDER BY d.dimension
        """,
        symbol,
        run_id,
    )


def test_the_judge_scores_six_dimensions_and_code_sums_them(db, config, store):
    model = FakeDebate()

    run_id, n = debated(db, config, store, model)

    assert n == 4
    got = scores(db, run_id)
    assert set(got) == {"ACME", "LATE", "NEWCO", "STALE"}
    # The Judge said 99 and 0.99 every time; the stored total and score are code's.
    assert got["ACME"] == ["scored", None, 17, 17 / 24, "Industrials", config]
    assert all(row[-1] == config for row in got.values())
    assert dimensions(db, "ACME", run_id) == [
        (n, s, f"Dimension {n} scores {s}.", "judge", s, f"Dimension {n} scores {s}.")
        for n, s in enumerate(JUDGE["ACME"], start=1)
    ]

    calls = rows(
        db,
        """
        SELECT c.role, c.model, c.response_model, c.cached, c.request_id
        FROM agb.panel_model_call c JOIN agb.panel_retrieval r ON r.id = c.retrieval_id
        WHERE r.symbol = 'ACME' AND c.run_id = %s ORDER BY c.id
        """,
        run_id,
    )
    assert calls == [
        ("analyst", "claude-haiku-4-5", "claude-haiku-4-5-20251001", False, "req_ACME"),
        ("prosecutor", "claude-sonnet-5", "claude-sonnet-5-20260101", False, "req_ACME_prosecutor"),
        ("defender", "claude-sonnet-5", "claude-sonnet-5-20260101", False, "req_ACME_defender"),
        ("judge", "claude-opus-5", "claude-opus-5-20260101", False, "req_ACME_judge"),
    ]
    [linked] = rows(
        db,
        """
        SELECT p.role, d.role, j.role
        FROM agb.panel_score s
        JOIN agb.panel_model_call p ON p.id = s.prosecutor_call_id
        JOIN agb.panel_model_call d ON d.id = s.defender_call_id
        JOIN agb.panel_model_call j ON j.id = s.judge_call_id
        JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
        WHERE r.symbol = 'ACME'
        """,
    )
    assert linked == ("prosecutor", "defender", "judge")


def test_the_filing_rules_override_the_judge(db, config, store):
    run_id, _ = debated(db, config, store, FakeDebate())
    got = scores(db, run_id)

    # An NT 10-K caps going concern and accounting integrity at 1; the Judge's scores are kept.
    late = dimensions(db, "LATE", run_id)
    assert [(d[1], d[3], d[4]) for d in late[:2]] == [(1, "rule", 3), (1, "rule", 4)]
    assert late[0][2].startswith("An NT 10-K") and late[0][2].endswith(
        "caps this dimension at 1 by rule."
    )
    assert [d[3] for d in late[2:]] == ["judge"] * 4
    assert got["LATE"][2] == 1 + 1 + 2 + 1 + 4 + 4

    # A stale 10-K does the same, and a Judge that found worse than the cap keeps its score.
    stale = dimensions(db, "STALE", run_id)
    assert [(d[1], d[3], d[4]) for d in stale[:2]] == [(0, "judge", 0), (1, "rule", 4)]
    assert "more than 15 months" in stale[1][2]
    assert got["STALE"][2] == 0 + 1 + 4 + 4 + 4 + 4

    # A recent IPO scores a neutral 2 on the disclosure delta whatever the Judge said.
    newco = dimensions(db, "NEWCO", run_id)
    assert newco[5][1:5] == (2, debate.IPO_REASON.format(score=2), "rule", 0)
    assert [d[3] for d in newco[:5]] == ["judge"] * 5
    assert got["NEWCO"][2:4] == [19, 19 / 24]


def test_a_name_that_was_never_extracted_has_no_score(db, config, store):
    model = FakeDebate()

    run_id, n = debated(db, config, store, model, analyst_model=FakeAnalyst(fail={"ACME"}))

    # SHELL has no filings, DOWN's EDGAR retrieval failed and ACME's Analyst failed: the blend
    # gives them the neutral 0.5, not a debate.
    assert n == 3
    assert set(scores(db, run_id)) == {"LATE", "NEWCO", "STALE"}
    assert {symbol for symbol, _, _ in model.requests} == {"LATE", "NEWCO", "STALE"}


def test_the_debate_reads_the_sector_and_never_the_name(db, config, store):
    model = FakeDebate()

    debated(db, config, store, model)

    assert len(model.requests) == 12
    for symbol, role, request in model.requests:
        content = request["messages"][0]["content"]
        assert content.startswith(f"Sector: {SECTORS[symbol]}\n")
        text = json.dumps(request)
        for identifier in IDENTIFIERS[symbol]:
            haystack = text if identifier.isupper() else text.lower()
            assert identifier not in haystack, (symbol, role, identifier)
        # The models take no sampling parameters; only the Judge is held to the schema.
        assert "temperature" not in request
        assert ("output_config" in request) == (role == "judge")
        assert request["max_tokens"] == analyst.MAX_TOKENS
    for symbol in ("ACME", "LATE", "NEWCO", "STALE"):
        assert model.roles(symbol) == ["prosecutor", "defender", "judge"]

    [(_, _, defender)] = [r for r in model.requests if r[:2] == ("ACME", "defender")]
    [(_, _, judge)] = [r for r in model.requests if r[:2] == ("ACME", "judge")]
    prosecution = "The prosecutor's argument on the Industrials case."
    defence = "The defender's argument on the Industrials case."
    assert f"<prosecution>\n{prosecution}\n</prosecution>" in defender["messages"][0]["content"]
    assert f"<defence>\n{defence}\n</defence>" in judge["messages"][0]["content"]
    assert "the Company" in defender["messages"][0]["content"]


def test_a_score_replays_from_the_store_without_a_model(db, config, store):
    run_id, _ = debated(db, config, store, FakeDebate())
    pipeline = db.connect("pipeline")

    for score_id, total, value in rows(
        db, "SELECT id, total, score FROM agb.panel_score WHERE run_id = %s", run_id
    ):
        replayed = panel.replay(pipeline, store, score_id)
        [(symbol,)] = rows(
            db,
            "SELECT r.symbol FROM agb.panel_score s "
            "JOIN agb.panel_retrieval r ON r.id = s.retrieval_id WHERE s.id = %s",
            score_id,
        )
        assert (replayed.total, replayed.value) == (total, value)
        assert [
            (d.dimension, d.score, d.reason, d.source, d.judge_score, d.judge_reason)
            for d in replayed.dimensions
        ] == dimensions(db, symbol, run_id)

    # A new run on identical inputs is answered from the cache: no model is asked.
    second, n = debated(db, config, store, NoModel(), analyst_model=NoModel())
    assert n == 4
    assert {s: row[:5] for s, row in scores(db, second).items()} == {
        s: row[:5] for s, row in scores(db, run_id).items()
    }
    cached = rows(
        db,
        "SELECT DISTINCT cached FROM agb.panel_model_call WHERE run_id = %s AND role <> 'analyst'",
        second,
    )
    assert cached == [(True,)]

    with pytest.raises(panel.PanelError, match="no scored panel_score 99"):
        panel.replay(pipeline, store, 99)


def test_scoring_a_run_twice_debates_nothing_new(db, config, store):
    model = FakeDebate()
    run_id, _ = debated(db, config, store, model)

    assert panel.score(db.connect("pipeline"), run_id, SECTORS, model, store) == 0
    assert len(model.requests) == 12
    assert rows(db, "SELECT count(*) FROM agb.panel_score") == [(4,)]


def test_scoring_refuses_a_missing_run_a_stale_config_or_a_missing_sector(db, config, store):
    pipeline = db.connect("pipeline")
    model = FakeDebate()
    with pytest.raises(panel.PanelError, match="no panel_run 99"):
        panel.score(pipeline, 99, SECTORS, model, store)

    # A config made before the debate prompts existed, or for other ones.
    hashes = panel.prompt_hashes() | {"judge": "0"}
    stale = (
        db.connect("cli")
        .execute(
            """
            INSERT INTO agb.panel_config
                (analyst_model, prosecutor_model, defender_model, judge_model, prompt_hashes,
                 rubric_version, scrubber_version, shortlist_n, w, actor)
            VALUES ('m', 'm', 'm', 'm', %s, %s, %s, 50, 0.2, 'operator')
            RETURNING id
            """,
            (json.dumps(hashes), analyst.RUBRIC_VERSION, SCRUBBER_VERSION),
        )
        .fetchone()[0]
    )
    [(stale_run,)] = pipeline.execute(
        "INSERT INTO agb.panel_run (panel_config_id, as_of, actor) "
        "VALUES (%s, '2026-09-30', 'pipeline') RETURNING id",
        (stale,),
    ).fetchall()
    with pytest.raises(panel.PanelError, match="made for the Judge's prompt templates, which"):
        panel.score(pipeline, stale_run, SECTORS, model, store)

    run_id = run(db, config, store, FakeAnalyst(), NAMES)
    for sectors in ({**SECTORS, "NEWCO": " "}, {k: v for k, v in SECTORS.items() if k != "NEWCO"}):
        with pytest.raises(panel.PanelError, match="no sector for NEWCO"):
            panel.score(pipeline, run_id, sectors, model, store)
    assert model.requests == []
    assert rows(db, "SELECT count(*) FROM agb.panel_score") == [(0,)]


def test_a_failed_refused_cut_off_or_garbled_call_is_a_debate_failure(db, config, store):
    model = FakeDebate(
        broken={
            ("ACME", "prosecutor"): "error",
            ("LATE", "defender"): "refusal",
            ("NEWCO", "judge"): "max_tokens",
            ("STALE", "judge"): "range",
        }
    )

    run_id, _ = debated(db, config, store, model)

    got = scores(db, run_id)
    assert {s: row[:4] for s, row in got.items()} == {
        "ACME": ["debate_failure", "prosecutor: claude-sonnet-5: OverloadedError", None, None],
        "LATE": ["debate_failure", "defender: the model declined to answer", None, None],
        "NEWCO": ["debate_failure", "judge: the answer was cut off at max_tokens", None, None],
        "STALE": [
            "debate_failure",
            "judge: the Judge's score for dimension 1 is outside 0 to 4",
            None,
            None,
        ],
    }
    # A name stops at the call that failed, and every answer received is logged.
    assert model.roles("ACME") == ["prosecutor"]
    assert model.roles("LATE") == ["prosecutor", "defender"]
    linked = rows(
        db,
        """
        SELECT r.symbol, s.prosecutor_call_id IS NOT NULL, s.defender_call_id IS NOT NULL,
               s.judge_call_id IS NOT NULL
        FROM agb.panel_score s JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
        ORDER BY r.symbol
        """,
    )
    assert linked == [
        ("ACME", False, False, False),
        ("LATE", True, True, False),
        ("NEWCO", True, True, True),
        ("STALE", True, True, True),
    ]
    assert rows(db, "SELECT count(*) FROM agb.panel_dimension") == [(0,)]


def test_a_prose_judge_is_a_debate_failure(db, config, store):
    run_id, _ = debated(db, config, store, FakeDebate(broken={("ACME", "judge"): "prose"}))

    assert scores(db, run_id)["ACME"][:2] == [
        "debate_failure",
        "judge: the Judge's answer is not JSON",
    ]
    assert scores(db, run_id)["LATE"][0] == "scored"


def test_scores_are_append_only_and_only_the_pipeline_writes_them(db, config, store):
    run_id, _ = debated(db, config, store, FakeDebate())
    cli = db.connect("cli")

    for table, column in (("panel_score", "sector"), ("panel_dimension", "reason")):
        with pytest.raises(psycopg.Error):
            cli.execute(f"UPDATE agb.{table} SET {column} = {column}")
        with pytest.raises(psycopg.Error):
            db.connect("pipeline").execute(f"DELETE FROM agb.{table}")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        cli.execute(
            "INSERT INTO agb.panel_score (retrieval_id, run_id, panel_config_id, sector, outcome, "
            "detail) VALUES (1, %s, %s, 'x', 'debate_failure', 'x')",
            (run_id, config),
        )
    for role in ("scheduler", "execution", "bot", "ui"):
        db.connect(role).execute("SELECT count(*) FROM agb.panel_score, agb.panel_dimension")


def test_a_score_must_agree_with_its_run_and_config(db, config, store):
    run_id, _ = debated(db, config, store, FakeDebate())
    other = panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)
    [(retrieval_id,)] = rows(
        db,
        "SELECT r.id FROM agb.panel_retrieval r WHERE r.run_id = %s AND r.symbol = 'SHELL'",
        run_id,
    )
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        db.connect("pipeline").execute(
            "INSERT INTO agb.panel_score (retrieval_id, run_id, panel_config_id, sector, "
            "outcome, detail) VALUES (%s, %s, %s, 'x', 'debate_failure', 'x')",
            (retrieval_id, run_id, other),
        )


# The Judge's answer and the rubric arithmetic, without a database.


def judge(body, stop="end_turn"):
    text = body if isinstance(body, str) else json.dumps(body)
    return Completion(text, "claude-opus-5", 0, 0, 0, 0, stop, None)


SIX = {f"dimension_{n}": {"score": 4, "reason": f"Reason {n}."} for n in range(1, 7)}


def test_parse_takes_six_scores_and_ignores_the_models_arithmetic():
    body = SIX | {"dimension_3": {"score": 0, "reason": " Bad. "}, "total": 3, "score": 0.1}

    assert debate.parse(judge(body)) == {
        1: (4, "Reason 1."),
        2: (4, "Reason 2."),
        3: (0, "Bad."),
        4: (4, "Reason 4."),
        5: (4, "Reason 5."),
        6: (4, "Reason 6."),
    }
    result = debate.score(debate.parse(judge(body)), None, False)
    assert (result.total, result.value) == (20, 20 / 24)
    assert debate.parse(judge(f"```json\n{json.dumps(SIX)}\n```")) == debate.parse(judge(SIX))


@pytest.mark.parametrize(
    ("body", "error"),
    [
        ("not json", "not JSON"),
        ([1, 2], "not a JSON object"),
        ({k: v for k, v in SIX.items() if k != "dimension_6"}, "no score for dimension 6"),
        (SIX | {"dimension_2": {"score": "3", "reason": "x"}}, "dimension 2 is not an integer"),
        (SIX | {"dimension_2": {"score": True, "reason": "x"}}, "dimension 2 is not an integer"),
        (SIX | {"dimension_2": {"score": 2.5, "reason": "x"}}, "dimension 2 is not an integer"),
        (SIX | {"dimension_4": {"score": -1, "reason": "x"}}, "dimension 4 is outside 0 to 4"),
        (SIX | {"dimension_5": {"score": 3, "reason": "  "}}, "no reason for dimension 5"),
        (SIX | {"dimension_5": {"score": 3}}, "no reason for dimension 5"),
    ],
)
def test_parse_refuses_anything_but_six_scored_and_reasoned_dimensions(body, error):
    with pytest.raises(debate.DebateError, match=error):
        debate.parse(judge(body))


@pytest.mark.parametrize(
    ("stop", "error"), [("refusal", "declined"), ("max_tokens", "cut off at max_tokens")]
)
def test_a_refused_or_cut_off_answer_is_an_error(stop, error):
    with pytest.raises(debate.DebateError, match=error):
        debate.parse(judge(SIX, stop))
    with pytest.raises(debate.DebateError, match=error):
        debate.answer(judge("An argument.", stop))
    with pytest.raises(debate.DebateError, match="empty"):
        debate.answer(judge("  "))


def test_the_rules_cap_a_delinquent_filer_and_neutralise_an_ipo():
    judgement = {n: (s, f"Reason {n}.") for n, s in enumerate([4, 0, 3, 3, 3, 4], start=1)}

    regular = debate.score(judgement, None, False)
    assert [d.source for d in regular.dimensions] == ["judge"] * 6
    assert regular.total == 17

    late = debate.score(judgement, "late_10k", False)
    assert [(d.score, d.source) for d in late.dimensions[:2]] == [(1, "rule"), (0, "judge")]
    assert late.total == 14 and late.dimensions[0].judge_score == 4

    ipo = debate.score(judgement, None, True)
    assert (ipo.dimensions[5].score, ipo.dimensions[5].source) == (2, "rule")
    assert ipo.total == 15

    both = debate.score(judgement, "nt_10k", True)
    assert [d.score for d in both.dimensions] == [1, 0, 3, 3, 3, 2]
    assert debate.status("nt_10k", True) == debate.STATUS["nt_10k"]


def test_each_role_has_its_own_prompt_hash_in_the_config(db, config):
    [(hashes,)] = rows(db, "SELECT prompt_hashes FROM agb.panel_config WHERE id = %s", config)
    assert set(hashes) == {"analyst", "prosecutor", "defender", "judge"}
    assert hashes["judge"] == debate.template_hash("judge")
    assert len(set(hashes.values())) == 4
