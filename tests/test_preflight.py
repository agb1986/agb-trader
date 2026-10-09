"""The panel pre-flight suite (issue #39): the four checks that say whether the panel can be
trusted under one configuration, and the record they leave behind.

The panel here is the real one for the checks that read what the Analyst did -- retrieval
fidelity and known-disclosure recall, over tests/edgar_fake.py -- and tests/panel_fake.py for
the two that read scores, which the debate produces (tests/test_debate.py covers that).
"""

import json
from dataclasses import replace

import psycopg
import pytest
from edgar_fake import AS_OF, client
from panel_fake import FakePanel
from test_panel import FABRICATED, SHORTLIST, FakeAnalyst, rows, use_cli_env

from agb import blend, cli, panel, preflight
from agb.disclosures import Disclosure
from agb.panel_store import PanelStore

# The fake market of tests/panel_fake.py: enough names to sample twenty from.
NAMES = [f"N{i:02d}" for i in range(1, 26)]
SECTORS = dict.fromkeys(NAMES, "Technology")
# What a flat panel scores: every name alike, so the scores have no spread at all.
FLAT = {symbol: (3, 3, 3, 3, 3, 3) for symbol in NAMES}


class HonestAnalyst(FakeAnalyst):
    """The fake Analyst without its one fabricated quote, so every quote it returns is in the
    document it cites: a run that retrieval fidelity passes."""

    def complete(self, request):
        completion = super().complete(request)
        if completion.text.startswith("{"):
            body = json.loads(completion.text)
            body["quotes"] = [q for q in body["quotes"] if q["quote"] != FABRICATED]
            completion = replace(completion, text=json.dumps(body))
        return completion


@pytest.fixture
def config(db):
    return panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2)


@pytest.fixture
def store(tmp_path):
    return PanelStore(tmp_path / "panel")


def analyst_run(db, config, store, model, shortlist=SHORTLIST, as_of=AS_OF):
    """A real panel run: retrieval and the Analyst, which is all the two evidence checks read."""
    return panel.run(db.connect("pipeline"), config, shortlist, as_of, client(), model, store)


def scored_run(db, config, scores=None, names=NAMES):
    """A scored run, from the stand-in panel."""
    fake = FakePanel(config, shortlist_n=50, scores=scores)
    return fake(db.connect("pipeline"), names, SECTORS, AS_OF)


def preflight_rows(db):
    return rows(
        db,
        "SELECT name, run_id, passed, measured, threshold, actor FROM agb.panel_preflight"
        " ORDER BY id",
    )


def test_retrieval_fidelity_reports_every_quote_not_found_verbatim(db, config, store):
    run_id = analyst_run(db, config, store, FakeAnalyst())
    conn = db.connect("cli")

    result = preflight.fidelity(conn, run_id, minimum=3)

    assert not result.passed
    assert result.measured == "3 of 12 quotes rejected over 3 names"
    assert result.threshold == "no quote rejected"
    assert result.run_id == run_id
    assert result.detail["verified"] == 9 and result.detail["rejected"] == 3
    assert [(q["symbol"], q["dimension"], q["rejection"]) for q in result.detail["quotes"]] == [
        (symbol, 3, "not found verbatim in D1") for symbol in ("ACME", "LATE", "NEWCO")
    ]
    assert FABRICATED[:40] in preflight.detail_text(result)


def test_retrieval_fidelity_passes_a_run_whose_every_quote_is_in_the_filing(db, config, store):
    run_id = analyst_run(db, config, store, HonestAnalyst())

    result = preflight.fidelity(db.connect("cli"), run_id, minimum=3)

    assert result.passed
    assert result.measured == "0 of 9 quotes rejected over 3 names"
    assert result.detail["quotes"] == []


def test_fidelity_refuses_a_run_too_small_or_too_quiet_to_measure(db, config, store):
    run_id = analyst_run(db, config, store, FakeAnalyst())
    conn = db.connect("cli")

    with pytest.raises(preflight.PreflightError, match="extracted 3 names; .* at least 10"):
        preflight.fidelity(conn, run_id)

    # A run that kept no quotes at all passes nothing: there is no fidelity in an empty record.
    with pytest.raises(preflight.PreflightError, match="no quotes; there is nothing to check"):
        preflight.fidelity(conn, scored_run(db, config))


def test_known_disclosure_recall_reports_misses_by_dimension(db, config, store):
    fixtures = (
        Disclosure("ACME", AS_OF, 1, "not in compliance with the leverage covenant", "covenant"),
        Disclosure("ACME", AS_OF, 2, "material weakness", "the 10-Q"),
        Disclosure("NEWCO", AS_OF, 1, "accumulated deficit of $412 million", "the prospectus"),
        Disclosure("ACME", AS_OF, 6, "material weakness", "a miss: the Analyst reads it as 2"),
        Disclosure("ACME", AS_OF, 3, "a nationwide recall of its flagship product", "not on file"),
        Disclosure("SHELL", AS_OF, 1, "going concern", "EDGAR serves nothing readable"),
    )
    conn = db.connect("cli")
    runs = preflight.recall_runs(
        db.connect("pipeline"), config, client(), FakeAnalyst(), store, fixtures
    )
    assert list(runs) == [AS_OF]

    result = preflight.recall(conn, store, runs, fixtures)

    assert not result.passed
    assert result.measured == "3 of 4 disclosures found"
    assert result.threshold == "every usable fixture found in its dimension"
    assert result.run_id is None and result.detail["runs"] == {AS_OF.isoformat(): runs[AS_OF]}
    assert result.detail["by_dimension"] == {"6": 1}
    assert [(f["symbol"], f["dimension"], f["status"]) for f in result.detail["findings"]] == [
        ("ACME", 1, "found"),
        ("ACME", 2, "found"),
        ("NEWCO", 1, "found"),
        ("ACME", 6, "missed"),
        ("ACME", 3, "unusable"),
        ("SHELL", 1, "unusable"),
    ]
    findings = {(f["symbol"], f["dimension"]): f["detail"] for f in result.detail["findings"]}
    assert findings["ACME", 6] == "quoted in dimension 2, not 6"
    assert findings["ACME", 3].startswith("'a nationwide recall of its flagship product' is not")
    assert findings["SHELL", 1] == "no_filings: no 10-K, 10-Q or registration statement on file"

    standing = preflight.fixtures_text(conn, store, runs, fixtures)
    assert standing.splitlines()[0] == "6 fixtures, 4 usable"
    assert "unusable" in standing and "dimension 6  missed" in standing

    passing = preflight.recall(conn, store, runs, fixtures[:3])
    assert passing.passed and passing.measured == "3 of 3 disclosures found"


def test_recall_refuses_a_fixture_set_with_too_little_on_the_record(db, config, store):
    fixtures = (
        Disclosure("ACME", AS_OF, 1, "not in compliance with the leverage covenant", "covenant"),
        Disclosure("ACME", AS_OF, 3, "a nationwide recall of its flagship product", "not on file"),
    )
    runs = preflight.recall_runs(
        db.connect("pipeline"), config, client(), FakeAnalyst(), store, fixtures
    )

    with pytest.raises(preflight.PreflightError, match="1 of 2 fixtures are usable"):
        preflight.recall(db.connect("cli"), store, runs, fixtures)
    with pytest.raises(preflight.PreflightError, match="the fixture set is empty"):
        preflight.recall(db.connect("cli"), store, {}, ())
    # A fixture nothing read is unusable, not a miss.
    [finding] = preflight.findings(db.connect("cli"), store, {}, fixtures[:1])
    assert finding.status == "unusable" and finding.detail == f"no run as of {AS_OF}"


def test_the_dispersion_gate_is_computed_and_recorded(db, config):
    conn = db.connect("cli")
    spread_out = scored_run(db, config)

    result = preflight.dispersion(conn, spread_out)

    scores = blend.scores(conn, spread_out)
    assert result.passed
    assert result.measured.endswith(" over 25 scored names")
    assert result.detail["stddev"] == blend.spread([s.score for s in scores.values()])
    assert result.detail["minimum"] == blend.DISPERSION_MIN
    assert result.detail["stddev"] >= blend.DISPERSION_MIN
    assert result.detail["lowest"].endswith(" 12 of 24") and result.detail["highest"].endswith(
        " 24 of 24"
    )

    flat = preflight.dispersion(conn, scored_run(db, config, scores=FLAT))
    assert not flat.passed
    assert flat.measured == "0.0000 over 25 scored names"
    assert flat.threshold == f">= {blend.DISPERSION_MIN}"

    with pytest.raises(preflight.PreflightError, match="scored 5 names; .* at least 10"):
        preflight.dispersion(conn, scored_run(db, config, names=NAMES[:5]))


def quote_for(db, review):
    """A verified quote on one name under review, as the real panel writes them, so the
    operator reads the evidence and not just the score."""
    db.connect("pipeline").execute(
        """
        INSERT INTO agb.panel_quote
            (extract_id, filing_id, dimension, document, section, quote, verified, scrubbed)
        SELECT e.id, f.id, 1, 'D1', 'Item 1A', 'the Company breached its covenant', true,
               'the Company breached its covenant'
        FROM agb.panel_extract e
        JOIN agb.panel_filing f ON f.retrieval_id = e.retrieval_id AND f.document = 'D1'
        WHERE e.retrieval_id = %s
        """,
        (review.retrieval_id,),
    )


def test_the_spot_check_presents_about_twenty_names_and_records_a_verdict_each(
    db, config, monkeypatch, capsys
):
    run_id = scored_run(db, config)
    conn = db.connect("cli")
    reviews = preflight.sample(conn, run_id)
    assert len(reviews) == preflight.SPOT_CHECK_N
    # The same twenty every time, spanning the panel's whole reading of the month.
    assert [r.symbol for r in preflight.sample(conn, run_id)] == [r.symbol for r in reviews]
    assert (reviews[0].total, reviews[-1].total) == (12, 24)
    quote_for(db, reviews[0])
    use_cli_env(db, monkeypatch)

    assert cli.main(["panel-preflight", "spot-check", str(run_id)]) == 0

    out = capsys.readouterr().out
    assert f"{reviews[0].symbol}  {reviews[0].symbol} Corp" in out
    assert "total 12 of 24, score 0.500  (0 of 20 names reviewed)" in out
    assert "  1 Going concern and liquidity        2 of 4" in out
    assert "      the Judge on dimension 1: scored 2" in out
    assert "  quotes (1), as the debate saw them:" in out
    assert "    1  D1 Item 1A" in out
    assert "the Company breached its covenant" in out
    assert f"agb panel-preflight spot-check {run_id} --symbol {reviews[0].symbol}" in out

    for review in reviews[:-1]:
        argv = ["panel-preflight", "spot-check", str(run_id), "--symbol", review.symbol]
        assert cli.main([*argv, "--verdict", "agree", "--actor", "alice"]) == 0
    out = capsys.readouterr().out
    assert f"{reviews[-2].symbol}: agree recorded" in out
    assert "(19 of 20 names reviewed)" in out
    assert preflight_rows(db) == []

    last = ["panel-preflight", "spot-check", str(run_id), "--symbol", reviews[-1].symbol]
    assert cli.main([*last, "--verdict", "agree", "--actor", "alice"]) == 0

    out = capsys.readouterr().out
    assert "spot_check: passed, 20 of 20 names reviewed, 0 disagreed" in out
    assert preflight_rows(db) == [
        (
            "spot_check",
            run_id,
            True,
            "20 of 20 names reviewed, 0 disagreed",
            "all 20 reviewed, none disagreed",
            "alice",
        )
    ]
    # Reading a finished check again does not record it twice.
    assert cli.main(["panel-preflight", "spot-check", str(run_id)]) == 0
    assert len(preflight_rows(db)) == 1


def test_the_spot_check_takes_the_operators_verdict_and_nothing_else(
    db, config, monkeypatch, capsys
):
    run_id = scored_run(db, config)
    conn = db.connect("cli")
    reviews = preflight.sample(conn, run_id)
    unsampled = next(symbol for symbol in NAMES if symbol not in {r.symbol for r in reviews})
    use_cli_env(db, monkeypatch)
    argv = ["panel-preflight", "spot-check", str(run_id)]

    assert cli.main([*argv, "--symbol", unsampled, "--verdict", "agree"]) == 1
    assert cli.main([*argv, "--symbol", reviews[0].symbol]) == 1
    assert cli.main([*argv, "--symbol", reviews[0].symbol, "--verdict", "disagree"]) == 1

    err = capsys.readouterr().err.splitlines()
    assert err[0] == f"agb: refused: {unsampled} is not among the 20 names under spot check"
    assert err[1] == "agb: refused: --symbol needs a --verdict"
    assert err[2] == ("agb: refused: a disagreement needs a note saying what the panel got wrong")

    for review in reviews:
        first = review is reviews[0]
        note = ["--note", "the going-concern language is boilerplate"] if first else []
        verdict = "disagree" if first else "agree"
        argv = ["panel-preflight", "spot-check", str(run_id), "--symbol", review.symbol]
        # The last verdict completes the review, so the check is measured and recorded: a
        # disagreement outstanding means a failure, and the command says so with its status.
        expected = 1 if review is reviews[-1] else 0
        assert cli.main([*argv, "--verdict", verdict, *note, "--actor", "alice"]) == expected
    capsys.readouterr()

    result = preflight.spot_check(conn, run_id)
    assert not result.passed
    assert result.measured == "20 of 20 names reviewed, 1 disagreed"
    assert result.detail["disagreed"] == [
        {
            "symbol": reviews[0].symbol,
            "total": 12,
            "notes": ["the going-concern language is boilerplate"],
        }
    ]
    # One verdict per name per operator, and nobody rewrites one.
    with pytest.raises(psycopg.errors.UniqueViolation):
        preflight.review(conn, run_id, reviews[0].symbol, "agree", None, "alice")
    conn.rollback()
    with pytest.raises(psycopg.Error):
        conn.execute("UPDATE agb.panel_spot_check SET verdict = 'agree'")


def test_results_are_kept_per_configuration_and_a_new_one_is_not_preflighted(
    db, config, store, monkeypatch, capsys
):
    conn = db.connect("cli")
    run_id = analyst_run(db, config, store, HonestAnalyst())
    scored = scored_run(db, config)
    for result in (
        preflight.fidelity(conn, run_id, minimum=3),
        preflight.dispersion(conn, scored),
        preflight.Result("disclosure_recall", True, "3 of 3 disclosures found", "every one", {}),
        preflight.Result("spot_check", True, "20 of 20 reviewed", "all 20", {}, run_id=scored),
    ):
        preflight.record(conn, config, result, "operator")

    assert preflight.ready(conn, config) == (True, [])
    assert "pre-flight passed" in preflight.status_text(conn, config)

    other = panel.create_config(conn, "operator", shortlist_n=50, w=0.4)
    assert preflight.ready(conn, other) == (False, list(preflight.CHECKS))
    use_cli_env(db, monkeypatch)
    assert cli.main(["panel-preflight", "show", str(other)]) == 0

    out = capsys.readouterr().out
    assert out.splitlines()[0] == f"panel_config {other}: not yet pre-flighted"
    assert [line.split()[:2] for line in out.splitlines()[2:]] == [
        [name, "-"] for name in preflight.CHECKS
    ]
    # The first configuration's results are untouched by the new one.
    assert preflight.ready(conn, config) == (True, [])

    # A failure outstanding is the newest result, not the kindest.
    preflight.record(
        conn,
        config,
        preflight.Result("spot_check", False, "19 of 20", "all 20", {}, run_id=scored),
        "operator",
    )
    assert preflight.ready(conn, config) == (False, ["spot_check"])
    assert cli.main(["panel-preflight", "show", str(config)]) == 0
    assert capsys.readouterr().out.splitlines()[0].endswith("outstanding — spot_check")


def test_a_recorded_result_is_never_changed_and_only_the_cli_and_pipeline_record_one(db, config):
    run_id = scored_run(db, config)
    result = preflight.Result("dispersion", True, "0.2000 over 25 names", ">= 0.05", {}, run_id)
    preflight.record(db.connect("cli"), config, result, "operator")
    preflight.record(db.connect("pipeline"), config, result, "pipeline")

    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        preflight.record(db.connect("ui"), config, result, "ui")
    with pytest.raises(psycopg.Error):
        db.connect("cli").execute("UPDATE agb.panel_preflight SET passed = false")
    with pytest.raises(psycopg.Error):
        db.connect("cli").execute("DELETE FROM agb.panel_preflight")
    assert [row[5] for row in preflight_rows(db)] == ["operator", "pipeline"]


def test_show_refuses_a_configuration_that_does_not_exist(db, monkeypatch, capsys):
    use_cli_env(db, monkeypatch)

    assert cli.main(["panel-preflight", "show", "99"]) == 1
    assert capsys.readouterr().err == "agb: refused: no panel_config 99\n"
    with pytest.raises(preflight.PreflightError, match="no panel_run 99"):
        preflight.config_of(db.connect("cli"), 99)


def test_fixtures_needs_a_recall_run_before_it_can_report(db, config, monkeypatch, capsys):
    use_cli_env(db, monkeypatch)

    assert cli.main(["panel-preflight", "fixtures", str(config)]) == 1

    assert "has no recall run" in capsys.readouterr().err
