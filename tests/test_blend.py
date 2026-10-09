"""Stage 2 end to end: the blend that mixes the panel's safety score into the stage-1 ranking,
the shadow basket that records what the other weight would have held, the gate's two panel
checks, and what the operator reads -- the safety scores and flags on the approval request, and
`/why` for one name (issue #38). tests/test_panel.py covers the panel itself; the panel here is
tests/panel_fake.py, which writes the rows a real run writes from scores the test chooses."""

import json
import re
from datetime import UTC, date, datetime

import psycopg
import pytest
from edgar_fake import AS_OF, client
from market_fake import SECTOR, SIGNAL, Market
from panel_fake import FakePanel
from test_basket import EVENING, plan_rebalance, rows, run
from test_execution import CALENDAR
from test_gate import move
from test_panel import SHORTLIST, FakeAnalyst

from agb import analyst, approval, basket, blend, debate, panel, readout
from agb.panel_store import PanelStore
from agb.rebalance import SIZE

# The month 2 rebalance, which proves a shadow basket holds its own names: the Monday after the
# October close, signalled on the Friday before it.
NEXT_PERIOD = "2026-11"
NEXT_TRADE = date(2026, 11, 2)
NEXT_SIGNAL = date(2026, 10, 30)
NEXT_EVENING = datetime(2026, 10, 30, 22, 0, tzinfo=UTC)

LIVE = "(SELECT max(id) FROM agb.basket WHERE kind = 'live')"
SHADOW = "(SELECT max(id) FROM agb.basket WHERE kind = 'shadow')"
# The twenty-six names of the fake market, worst growth last, so the stage-1 ranking is ZTOP
# first and then N01..N25 -- an order no alphabetical sort can produce.
RANKING = ["ZTOP", *(f"N{i:02d}" for i in range(1, 26))]


def market(tmp_path, **kwargs):
    return Market(tmp_path, **kwargs).names(25).name("ZTOP", growth=0.02)


def fake_panel(db, w, *, shortlist_n=25, **scores):
    """The stand-in panel and the configuration it ran under, which agree on the weight and
    the shortlist, as a real `panel.Runner` and its config do."""
    config = panel.create_config(db.connect("cli"), "operator", shortlist_n=shortlist_n, w=w)
    return FakePanel(config, shortlist_n=shortlist_n, w=w, **scores)


def symbols(db, which):
    return [
        s for (s,) in rows(db, f"SELECT symbol FROM agb.basket_target WHERE basket_id = {which}")
    ]


def targets(db, which):
    return rows(
        db,
        "SELECT symbol, held, rank FROM agb.basket_target"
        f" WHERE basket_id = {which} ORDER BY symbol",
    )


def blends(db, which):
    return rows(
        db,
        "SELECT symbol, stage1_rank, stage1_rank_pct, llm_total, neutral, final_score"
        f" FROM agb.basket_blend WHERE basket_id = {which} ORDER BY stage1_rank",
    )


def failed_checks(db):
    return rows(
        db,
        "SELECT level, name, measured, threshold FROM agb.gate_check"
        " WHERE NOT passed ORDER BY name",
    )


# What the panel is told.


def test_the_panel_is_told_the_shortlist_alphabetically_and_never_a_stage1_rank(db, tmp_path):
    """The shortlist is handed over sorted, because the order it was ranked in is itself the
    stage-1 rank. Nothing else about stage 1 goes with it, and the panel's tables have nowhere
    to put a rank even if it did."""
    plan_rebalance(db)
    fake = fake_panel(db, 0.2, shortlist_n=10)
    root = market(tmp_path).land()

    run(db, root, runner=fake)

    [(job_id,)] = rows(db, "SELECT id FROM agb.job")
    [call] = fake.calls
    assert call.symbols == [f"N{i:02d}" for i in range(1, 10)] + ["ZTOP"]
    assert call.sectors == dict.fromkeys(call.symbols, SECTOR)
    assert (call.as_of, call.job_id) == (SIGNAL, job_id)
    # ZTOP is the stage-1 best and the last name the panel was told, so the order it was given
    # carries no rank either.
    assert rows(
        db, f"SELECT symbol FROM agb.basket_rank WHERE basket_id = {LIVE} AND rank = 1"
    ) == [("ZTOP",)]
    recorded = rows(
        db,
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = 'agb' AND table_name LIKE %s",
        "panel\\_%",
    )
    assert [c for (c,) in recorded if "rank" in c or "stage1" in c] == []


def test_no_panel_prompt_mentions_a_rank(db, tmp_path):
    """And the real panel's own requests, which are built from the filings alone: what the
    Analyst is sent names the company and its filings, never a rank or a score."""
    recorder = Recorder()

    panel.run(
        db.connect("pipeline"),
        panel.create_config(db.connect("cli"), "operator", shortlist_n=50, w=0.2),
        SHORTLIST,
        AS_OF,
        client(),
        recorder,
        PanelStore(tmp_path / "panel"),
    )

    [(read,)] = rows(db, "SELECT count(*) FROM agb.panel_retrieval WHERE outcome = 'retrieved'")
    assert read and len(recorder.requests) == read  # one request per name there was a filing for
    for request in recorder.requests:
        sent = json.dumps(request).lower()  # system and messages both, however they are shaped
        assert "rank" not in sent
        assert "percentile" not in sent


class Recorder(FakeAnalyst):
    """The fake Analyst, keeping what it was sent."""

    def __init__(self):
        super().__init__()
        self.requests = []

    def complete(self, request):
        self.requests.append(request)
        return super().complete(request)


# The blend.


def test_at_w_zero_the_traded_basket_is_the_stage1_basket(db, tmp_path):
    """With w = 0 the panel moves nothing: the basket is the stage-1 top twenty however the
    panel scored it, and every blended name's final score is its stage-1 percentile alone. The
    shadow basket, at the other weight, is what the scores would have changed."""
    plan_rebalance(db)
    # The worst five of the shortlist are the safest names, and the rest are unsafe, so a
    # weight that counted would reorder the basket.
    scores = {s: (1,) * 6 for s in RANKING[:20]} | {s: (4,) * 6 for s in RANKING[20:25]}
    fake = fake_panel(db, 0.0, scores=scores)
    root = market(tmp_path).land()

    run(db, root, runner=fake)

    assert [s for s, _, _ in targets(db, LIVE)] == sorted(RANKING[:SIZE])
    live = blends(db, LIVE)
    assert [s for s, *_ in live] == RANKING[:25]
    assert [rank for _, rank, *_ in live] == list(range(1, 26))
    for _, _, rank_pct, _, neutral, final in live:
        assert not neutral and final == pytest.approx(rank_pct)
    # The shadow holds names the traded basket passed over, at the weight it did not trade.
    assert rows(db, f"SELECT panel_w::float FROM agb.basket WHERE id = {SHADOW}") == [
        (pytest.approx(basket.SHADOW_W),)
    ]
    shadow = set(symbols(db, SHADOW))
    assert shadow != set(RANKING[:SIZE])
    assert "N20" in shadow and "N20" not in symbols(db, LIVE)
    # Same panel run, both weights: the counterfactual is the scores, not a second panel.
    assert len(fake.calls) == 1
    assert rows(
        db, "SELECT count(DISTINCT panel_run_id), count(DISTINCT panel_config_id) FROM agb.basket"
    ) == [(1, 1)]


def test_a_name_the_panel_did_not_score_takes_the_neutral_middle(db, tmp_path):
    """A name EDGAR could not be read for is neither helped nor hurt: it takes 0.5, which is
    the middle of the range, and the basket records that it was neutral rather than judged."""
    plan_rebalance(db)
    fake = fake_panel(db, 0.2, retrievals={"N04": "edgar_failure"})
    root = market(tmp_path).land()

    run(db, root, runner=fake)

    rows_by_symbol = {s: row for s, *row in blends(db, LIVE)}
    _, _, total, neutral, _ = rows_by_symbol["N04"]
    assert (total, neutral) == (None, True)
    pct = rows(
        db,
        f"SELECT llm_score_pct FROM agb.basket_blend WHERE basket_id = {LIVE} AND symbol = 'N04'",
    )
    assert pct == [(pytest.approx(blend.NEUTRAL),)]
    assert rows(
        db,
        f"SELECT count(*) FROM agb.basket_blend WHERE basket_id = {LIVE} AND neutral",
    ) == [(1,)]


# The shadow basket.


def test_the_shadow_basket_holds_its_own_names_and_is_never_traded(db, tmp_path):
    """A month's shadow basket is selected against the previous shadow basket's holdings, not
    the traded book, so by the second month it holds names of its own -- and the state machine
    refuses to move it, whatever the surface."""
    plan_rebalance(db)
    scores = {s: (1,) * 6 for s in RANKING[:20]} | {s: (4,) * 6 for s in RANKING[20:25]}
    first = fake_panel(db, 0.0, scores=scores)
    run(db, market(tmp_path / "m1").land(), runner=first)
    [(shadow_id,)] = rows(db, f"SELECT {SHADOW}")
    held = set(symbols(db, shadow_id))

    plan_rebalance(db, period=NEXT_PERIOD, session_date=NEXT_TRADE)
    second = FakePanel(first.config_id, shortlist_n=first.shortlist_n, w=0.0, scores=scores)
    run(
        db,
        market(tmp_path / "m2", end=NEXT_SIGNAL).land(),
        runner=second,
        clock=lambda: NEXT_EVENING,
    )

    assert len(second.calls) == 1
    # Nothing has filled, so the traded book is still empty and month 2 buys everything.
    assert basket.book(db.connect("pipeline")) == {}
    assert [h for _, h, _ in targets(db, LIVE)] == [False] * SIZE
    # The shadow book is month 1's shadow, so month 2's shadow carries those names through.
    assert basket.shadow_book(db.connect("pipeline")).keys() == held
    carried = {s for s, was_held, _ in targets(db, SHADOW) if was_held}
    assert carried and carried <= held

    [(month2_shadow,)] = rows(db, f"SELECT {SHADOW}")
    assert rows(db, "SELECT state FROM agb.basket_state WHERE id = %s", month2_shadow) == [
        ("proposed",)
    ]
    assert rows(
        db, "SELECT count(*) FROM agb.basket_order WHERE basket_id = %s", month2_shadow
    ) == [(0,)]
    for state, role, surface in (("pending", "bot", "telegram"), ("approved", "ui", "ui")):
        with pytest.raises(psycopg.errors.InsufficientPrivilege, match="never traded"):
            move(db, month2_shadow, 2, "proposed", state, role, surface)
    with pytest.raises(psycopg.errors.InsufficientPrivilege, match="never traded"):
        move(db, month2_shadow, 2, "proposed", "submitted", "pipeline")


# The gate.


def test_the_gate_halts_on_incomplete_panel_coverage(db, tmp_path):
    """Twenty-four of twenty-five scored is not a blend of the shortlist: the month halts
    rather than trade a basket a quarter of whose ranking came from one neutral default."""
    plan_rebalance(db)
    fake = fake_panel(db, 0.2, scores={"N04": None})
    root = market(tmp_path).land()

    run(db, root, runner=fake)

    assert failed_checks(db) == [("provenance", "panel_coverage", "24 of 25 scored", "all 25")]
    assert rows(db, "SELECT kind, state FROM agb.basket_state ORDER BY id") == [
        ("live", "halted"),
        ("shadow", "proposed"),
    ]
    assert rows(db, "SELECT cause, basket_id FROM agb.halt_open") == [
        ("gate_breach", rows(db, f"SELECT {LIVE}")[0][0])
    ]


def test_the_gate_halts_when_the_panels_scores_have_no_spread(db, tmp_path):
    """A panel that scores every name alike ranks to noise once percentile-ranked, however
    right each score is, so the month halts."""
    plan_rebalance(db)
    fake = fake_panel(db, 0.2, scores=dict.fromkeys(RANKING, (3,) * 6))
    root = market(tmp_path).land()

    run(db, root, runner=fake)

    assert failed_checks(db) == [
        ("provenance", "panel_dispersion", "0.0000", f">= {blend.DISPERSION_MIN}")
    ]
    assert rows(db, "SELECT kind, state FROM agb.basket_state ORDER BY id") == [
        ("live", "halted"),
        ("shadow", "proposed"),
    ]
    assert rows(db, "SELECT cause FROM agb.halt_open") == [("gate_breach",)]


def test_a_basket_proposed_with_no_panel_is_not_checked_for_coverage(db, tmp_path):
    """The checks are about a panel's work. A stage-1 only month has none to check, and is
    neither halted for it nor passed on it."""
    plan_rebalance(db)
    root = market(tmp_path).land()

    run(db, root)

    assert rows(db, "SELECT name FROM agb.gate_check WHERE name LIKE %s", "panel%") == []
    assert rows(db, "SELECT state FROM agb.basket_state") == [("approved",)]
    assert rows(db, "SELECT panel_run_id, panel_config_id, panel_w FROM agb.basket") == [
        (None, None, None)
    ]
    assert rows(db, "SELECT count(*) FROM agb.basket_blend") == [(0,)]


# What the operator reads.


def test_the_approval_request_carries_the_safety_scores_and_the_panels_flags(db, tmp_path):
    """Every name being bought carries its rubric total, and what the panel found wrong with a
    name the basket holds is listed under FLAGS: a delinquent filer, a recent listing, and any
    dimension it scored at or below the flag threshold. Each is a fact, never a verdict."""
    plan_rebalance(db)
    fake = fake_panel(
        db,
        0.0,
        scores={"N05": (0, 4, 4, 4, 4, 4)},
        delinquent={"N03": "nt_10k"},
        ipo=("N07",),
    )
    root = market(tmp_path).land()

    run(db, root, runner=fake)
    [(basket_id,)] = rows(db, f"SELECT {LIVE}")
    text = approval.request_text(db.connect("bot"), basket_id, EVENING, CALENDAR)

    bought = {line.split()[1]: line for line in text.splitlines() if line.startswith("  + ")}
    assert set(bought) == set(RANKING[:SIZE])
    assert "safety 20/24" in bought["N05"]
    assert re.search(r"safety \d+/24", bought["ZTOP"])
    flags = text.split("FLAGS")[1].split("\n\nGATE")[0]
    assert "delinquent filer (nt_10k)" in flags
    assert flags.count("N03") == 3  # the delinquency, and the two dimensions it caps to 1
    assert "dimension 1 scored 1 of 4" in flags and "dimension 2 scored 1 of 4" in flags
    assert "dimension 1 scored 0 of 4" in flags  # N05, as the Judge scored it
    assert "listed within the year" in flags
    # A name with nothing against it is not mentioned, and nothing here recommends anything.
    assert "N09" not in flags
    assert "sell" not in flags.lower() and "avoid" not in flags.lower()


def test_why_prints_the_blend_the_rubric_and_the_filings_it_read(db, tmp_path):
    """`/why` for one name: the rebalance and the weight, its stage-1 rank, its safety score
    and where that placed it, every rubric dimension with a reason for the low ones only, what
    a filing rule set, the filings the Analyst read and the fact that it read them scrubbed."""
    plan_rebalance(db)
    fake = fake_panel(db, 0.2, scores={"N03": (4, 4, 2, 4, 4, 4)}, delinquent={"N03": "nt_10k"})
    root = market(tmp_path).land()

    run(db, root, runner=fake)
    text = readout.why(db.connect("bot"), "n03")  # the operator types it however they like

    assert text.startswith("WHY N03 — rebalance 2026-10, blended at w = 0.2")
    assert "stage-1 rank 4 of 26 ranked, shortlisted with 25 names" in text
    # 1 + 1 + 2 + 4 + 4 + 4, the two capped dimensions included.
    assert re.search(r"safety 16/24 \(0\.67\), \d+(st|nd|rd|th) safest of the 25 shortlisted", text)
    assert "delinquent filer (nt_10k), which caps dimensions 1 and 2" in text
    rubric = text.split("RUBRIC\n")[1].split("\n\nSOURCES")[0]
    for dimension, (title, _) in analyst.RUBRIC.items():
        assert f"{dimension}. {title}" in rubric
    assert rubric.count("set by a filing rule; the Judge scored 4") == 2
    # A reason for the dimensions scoring 2 or below, and for no others. On the two the rule
    # capped it is the rule's reason, which is what set the score; on the third it is the
    # Judge's own, and the three the Judge scored 4 carry none.
    assert rubric.count(debate.DELINQUENT_REASON["nt_10k"].format(cap=debate.DELINQUENT_CAP)) == 2
    assert "the Judge on dimension 3: scored 2" in rubric
    assert rubric.count("the Judge on dimension") == 1
    assert "SOURCES (2)" in text
    assert f"D1  10-K filed {SIGNAL}, Item 1A, in full" in text
    assert "D2  10-K filed" in text and "first 60,000 of 90,000 characters" in text
    assert text.endswith(readout.SCRUBBED)


def test_why_has_an_answer_before_any_panel_has_run_and_for_a_name_it_never_saw(db, tmp_path):
    plan_rebalance(db)
    assert "no panel has scored a basket" in readout.why(db.connect("bot"), "N01")

    fake = fake_panel(db, 0.2, shortlist_n=10)
    run(db, market(tmp_path).land(), runner=fake)

    text = readout.why(db.connect("bot"), "N20")
    assert "N20 was not shortlisted" in text
    assert "the panel scored only the shortlist" in text
