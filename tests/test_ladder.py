"""The autonomy ladder and the trading mode (docs/architecture/04-risk-and-approval.md).

Every criterion here is a view over the recorded history, so each fixture writes history —
settled rebalances, recorded checks, commands the operator ran — and then reads what the
ladder makes of it. Nothing is a stored counter, which is why a rejection in the middle of a
run of clean rebalances simply restarts the count.

The two moves up are deliberate and happen at the terminal; the move down happens by itself.
tests/test_gate.py covers the gate's own approval of a basket at L2, tests/test_approval_loop.py
the Telegram approval it replaces, and tests/test_mechanics.py the three arithmetic checks that
three of the paper-to-live criteria read.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from market_fake import PERIOD
from psycopg import errors
from test_approval_loop import cli_main
from test_bot_commands import ask
from test_execution import CALENDAR, NOW
from test_gate import (
    BASE,
    HELD,
    MARKS,
    NAMES,
    Case,
    buy,
    go_live,
    hold,
    insert,
    move,
    rows,
    run,
    sell,
    state,
    targets,
)
from test_preflight import scored_run

from agb import approval, execution, ladder, mechanics, panel, preflight

# Long enough at L1 for the time criterion to be met.
A_YEAR = timedelta(days=400)
# A basket that sells a quarter of its book: inside the gate's 60% turnover limit, above the
# 30% a clean basket proceeds on. Five positions are kept at 300 shares, fifteen names bought
# at 230, so the targets are worth $99,000 of the $100,000 capital.
BUSY = Case(
    target=replace(
        BASE,
        targets=targets(HELD[:5], shares=Decimal(300)) + targets(NAMES[:15], shares=Decimal(230)),
        orders=tuple(sell(s) for s in HELD[5:])
        + tuple(buy(s, qty=Decimal(230), notional=Decimal(4600)) for s in NAMES[:15]),
    ),
    marks=MARKS | {s: Decimal(20) for s in HELD},
    book={s: Decimal(300) for s in HELD},
)
# The same marks for a basket gated against a book of held names, which the turnover check
# has to price.
PRICED = Case(marks=MARKS | {s: Decimal(20) for s in HELD})


def a_command(db, verb, raw_text=None, outcome="accepted"):
    """One command on record, as a surface writes it before acting on it."""
    [(command_id,)] = rows(
        db,
        "INSERT INTO agb.command (actor, surface, raw_text, verb, outcome)"
        " VALUES ('ada', 'cli', %s, %s, %s) RETURNING id",
        raw_text or f"agb {verb}",
        verb,
        outcome,
    )
    return command_id


def at_level(db, level, *, since=None, attestation="the record is sound", actor="ada"):
    """The level set by hand the way `agb level` records it, optionally timestamped in the
    past so the time spent at the level can be measured."""
    command_id = a_command(db, "level", f"agb level {level}")
    db.connect("cli").execute(
        "INSERT INTO agb.ladder_event (level, actor, at, reason, attestation, command_id)"
        " VALUES (%s, %s, coalesce(%s::timestamptz, now()), %s, %s, %s)",
        (level, actor, since, f"raised to {level} by {actor}", attestation, command_id),
    )


def settled(db, n, *, mode="live", outcome="filled"):
    """One rebalance of the month, gated in `mode` and settled in `outcome`. The gate run is
    written by hand because what makes a rebalance paper or live is the mode it was gated
    under, and these fixtures care about the record rather than the checks."""
    basket_id = insert(db, replace(BASE, orders=()), reference=f"{PERIOD}r{n}")
    conn = db.connect("pipeline")
    [(run_id,)] = conn.execute(
        "INSERT INTO agb.gate_run (basket_id, checks, mode, actor)"
        " VALUES (%s, 1, %s, 'pipeline') RETURNING id",
        (basket_id, mode),
    ).fetchall()
    conn.execute(
        "INSERT INTO agb.gate_check (run_id, level, name, measured, threshold, passed)"
        " VALUES (%s, 'basket', 'positions', '20', '= 20', true)",
        (run_id,),
    )
    if outcome == "halted":
        move(db, basket_id, 2, "proposed", "halted", "pipeline")
    elif outcome == "missed":
        move(db, basket_id, 2, "proposed", "missed", "scheduler")
    else:
        move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
        if outcome == "rejected":
            move(db, basket_id, 3, "pending", "rejected", "bot", "telegram")
        else:
            move(db, basket_id, 3, "pending", "approved", "bot", "telegram")
            move(db, basket_id, 4, "approved", "submitted", "execution")
            move(db, basket_id, 5, "submitted", "filled", "execution")
    return basket_id


def clean(db, count, *, mode="live", first=1):
    """A run of settled rebalances that all filled."""
    return [settled(db, n, mode=mode) for n in range(first, first + count)]


def arithmetic(db):
    """The three arithmetic checks of the paper record, each with a pass on it."""
    conn = db.connect("cli")
    for name in mechanics.CHECKS:
        result = mechanics.Result(name, True, "measured over paper", "the check passes")
        mechanics.record(conn, result, "ada")


def preflighted(db):
    """A panel configuration with the whole pre-flight suite passed against it."""
    conn = db.connect("cli")
    config = panel.create_config(conn, "operator", shortlist_n=50, w=0.2)
    run_id = scored_run(db, config)
    for name in preflight.CHECKS:
        result = preflight.Result(name, True, "measured", "the check passes", {}, run_id)
        preflight.record(conn, config, result, "ada")
    return config


def exercised(db, *verbs):
    """The commands the last three paper-to-live criteria want to have been used once."""
    for verb in verbs:
        a_command(db, verb)


def earned_live(db):
    """Every paper-to-live criterion satisfied."""
    clean(db, 12, mode="paper")
    arithmetic(db)
    preflighted(db)
    exercised(db, "pause", "liquidate", "approve")


def criterion(db, name):
    [found] = [c for c in ladder.live_status(db.connect("cli")) if c.name == name]
    return found


# -- the eligibility views -----------------------------------------------------------------


def test_the_ladder_starts_at_l0_and_counts_clean_live_rebalances(db):
    """Acceptance criterion 1: the system starts at L0, recorded by the migration, and the
    one criterion for L1 is counted from the rebalances that have settled."""
    conn = db.connect("cli")
    now = ladder.current(conn)

    assert (now.level, now.actor, now.attestation) == ("L0", "system", None)
    assert ladder.approval_required(conn) is True
    assert ladder.status(conn) == [ladder.Criterion(1, "clean_rebalances", "0", ">= 6", False)]
    assert ladder.ready(conn) == ladder.Readiness("L1", False, ["clean_rebalances"])

    clean(db, 6)

    assert ladder.status(conn) == [ladder.Criterion(1, "clean_rebalances", "6", ">= 6", True)]
    assert ladder.ready(conn) == ladder.Readiness("L1", True, [])


@pytest.mark.parametrize("outcome", ["rejected", "missed", "halted"])
def test_a_rebalance_that_did_not_fill_ends_the_run_of_clean_ones(db, outcome):
    """Nothing keeps a count, so a rejection, a miss or a halt simply restarts the run: the
    criterion counts the rebalances since the last one that did not fill."""
    clean(db, 2)
    settled(db, 3, outcome=outcome)
    clean(db, 2, first=4)

    assert ladder.status(db.connect("cli")) == [
        ladder.Criterion(1, "clean_rebalances", "2", ">= 6", False)
    ]


def test_paper_rebalances_are_no_progress_up_the_ladder(db):
    """The ladder is about live trading, so a month of paper counts towards nothing."""
    clean(db, 6, mode="paper")

    assert ladder.ready(db.connect("cli")) == ladder.Readiness("L1", False, ["clean_rebalances"])


def test_a_fresh_l1_has_not_yet_earned_l2(db):
    """L2 asks for a year at L1, which a level set today has not had."""
    at_level(db, "L1")

    assert ladder.ready(db.connect("cli")) == ladder.Readiness("L2", False, ["days_at_l1"])


def test_at_l1_the_criteria_are_the_time_held_and_nothing_rejected(db):
    """Acceptance criterion 1 for the step above: a year at L1 with no rejection in it."""
    at_level(db, "L1", since=datetime.now(UTC) - A_YEAR)
    conn = db.connect("cli")

    days, rejections = ladder.status(conn)

    assert (days.name, days.measured, days.threshold, days.met) == (
        "days_at_l1",
        "400",
        ">= 365",
        True,
    )
    assert (rejections.name, rejections.measured, rejections.threshold) == (
        "rejections_at_l1",
        "0",
        "= 0",
    )
    assert ladder.ready(conn) == ladder.Readiness("L2", True, [])

    settled(db, 1, outcome="rejected")

    assert ladder.ready(conn) == ladder.Readiness("L2", False, ["rejections_at_l1"])


def test_l2_is_the_top_of_the_ladder(db):
    """There is nothing above L2, so nothing measures eligibility and no rebalance waits."""
    at_level(db, "L2")
    conn = db.connect("cli")

    assert ladder.status(conn) == []
    assert ladder.ready(conn) is None
    assert ladder.approval_required(conn) is False
    assert "L2 is the top of the ladder." in ladder.level_text(conn)


# -- moving up, at the terminal ------------------------------------------------------------


def test_the_level_moves_up_only_when_it_is_earned_and_only_with_an_attestation(
    db, monkeypatch, capsys
):
    """Acceptance criterion 2: `agb level` refuses while the view is unsatisfied, refuses
    without the human's attestation, and records both the refusal and the move."""
    code, out, err = cli_main(db, monkeypatch, capsys, "level", "L1", "--attest", "six months")

    assert (code, out) == (1, "")
    assert err == "agb: refused: L1 is not earned yet: clean_rebalances outstanding\n"

    clean(db, 6)
    code, _, err = cli_main(db, monkeypatch, capsys, "level", "L1")

    assert (code, err) == (
        1,
        "agb: refused: a move to L1 needs your attestation that it is warranted\n",
    )

    attestation = "read all six rebalances one by one"
    code, out, err = cli_main(
        db, monkeypatch, capsys, "level", "L1", "--attest", attestation, "--actor", "ada"
    )

    assert (code, err) == (0, "")
    assert out == f"the level is L1: {ladder.MEANING['L1']}.\n"
    assert rows(
        db,
        "SELECT e.level, e.actor, e.reason, e.attestation, c.verb, c.args->>'level', c.raw_text"
        " FROM agb.ladder_event e JOIN agb.command c ON c.id = e.command_id",
    ) == [
        (
            "L1",
            "ada",
            "raised from L0 by ada",
            attestation,
            "level",
            "L1",
            f"agb level L1 --attest '{attestation}'",
        )
    ]
    assert rows(db, "SELECT reason FROM agb.command WHERE outcome = 'refused' ORDER BY id") == [
        ("L1 is not earned yet: clean_rebalances outstanding",),
        ("a move to L1 needs your attestation that it is warranted",),
    ]


def test_the_ladder_is_climbed_one_level_at_a_time_and_never_to_where_it_stands(
    db, monkeypatch, capsys
):
    """Eligibility is only ever measured for the next level up, and the level in force is
    not a move at all."""
    code, _, err = cli_main(db, monkeypatch, capsys, "level", "L2", "--attest", "straight up")

    assert (code, err) == (
        1,
        "agb: refused: the ladder is climbed one level at a time; L1 comes after L0\n",
    )

    code, _, err = cli_main(db, monkeypatch, capsys, "level", "L0")

    assert code == 1
    assert err.startswith("agb: refused: the level is already L0, set ")
    assert rows(db, "SELECT count(*) FROM agb.ladder_event") == [(1,)]


def test_tightening_the_level_by_hand_needs_no_attestation(db, monkeypatch, capsys):
    """Loosening is deliberate; tightening is always allowed."""
    at_level(db, "L1")

    code, out, _ = cli_main(db, monkeypatch, capsys, "level", "L0", "--actor", "ada")

    assert (code, out) == (0, f"the level is L0: {ladder.MEANING['L0']}.\n")
    assert rows(db, "SELECT level, reason, attestation FROM agb.ladder_event ORDER BY id")[-1] == (
        "L0",
        "lowered from L1 by ada",
        None,
    )


def test_a_level_and_a_mode_that_do_not_exist_are_refused(db):
    """The CLI's own `choices` stops these, so they are put to the module directly: nothing
    downstream should ever see a level or a mode the system does not have."""
    conn = db.connect("cli")

    with pytest.raises(ladder.Refused, match="L3 is not a level; the levels are L0, L1, L2"):
        ladder.raise_to(conn, "L3", "why not", approval.Source("ada", "cli", "agb level L3"))
    with pytest.raises(ladder.Refused, match="shadow is not a mode; the modes are paper, live"):
        ladder.set_mode(conn, "shadow", "why not", approval.Source("ada", "cli", "agb mode shadow"))

    assert rows(db, "SELECT verb, outcome FROM agb.command ORDER BY id") == [
        ("level", "refused"),
        ("mode", "refused"),
    ]


@pytest.mark.parametrize("role", ["scheduler", "execution"])
def test_a_service_may_only_ever_drop_the_level_to_l0_as_the_system(db, role):
    """The ladder loosens at the terminal and nowhere else: the services that notice a
    breach may record the drop and nothing besides it."""
    command_id = a_command(db, "level")
    conn = db.connect(role)

    conn.execute(
        "INSERT INTO agb.ladder_event (level, actor, reason) VALUES ('L0', 'system', %s)",
        (f"{role} dropped the level",),
    )

    for level, actor, attestation in (("L1", "system", "warranted"), ("L0", role, None)):
        with pytest.raises(errors.InsufficientPrivilege):
            conn.execute(
                "INSERT INTO agb.ladder_event (level, actor, reason, attestation, command_id)"
                " VALUES (%s, %s, 'no', %s, %s)",
                (level, actor, attestation, command_id if attestation else None),
            )


@pytest.mark.parametrize("role", ["pipeline", "bot", "ui"])
def test_a_service_with_no_say_in_the_level_cannot_record_one(db, role):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute(
            "INSERT INTO agb.ladder_event (level, actor, reason) VALUES ('L0', 'system', 'no')"
        )


# -- paper to live -------------------------------------------------------------------------


def test_the_paper_to_live_view_shows_each_criterion_as_met_or_unmet(db):
    """Acceptance criterion 6: nine criteria, each measured from the record. A criterion
    nobody has measured reads unmet rather than passing by default."""
    conn = db.connect("cli")

    assert [c.name for c in ladder.live_status(conn)] == [
        "paper_rebalances",
        "fill_tolerance",
        "fee_arithmetic",
        "partial_fills",
        "undiagnosed_breaches",
        "panel_preflight",
        "pause_exercised",
        "liquidate_exercised",
        "approval_exercised",
    ]
    assert (criterion(db, "fill_tolerance").measured, criterion(db, "fill_tolerance").met) == (
        "not run",
        False,
    )
    assert criterion(db, "fill_tolerance").threshold == "a recorded pass"
    assert criterion(db, "pause_exercised").measured == "never"
    assert criterion(db, "undiagnosed_breaches").met is True
    assert ladder.live_ready(conn) == ladder.Readiness(
        "live",
        False,
        [
            "paper_rebalances",
            "fill_tolerance",
            "fee_arithmetic",
            "partial_fills",
            "panel_preflight",
            "pause_exercised",
            "liquidate_exercised",
            "approval_exercised",
        ],
    )


def test_pre_flight_is_unmet_until_the_suite_has_recorded_a_pass(db):
    """Acceptance criterion 6: the pre-flight criterion reads the panel pre-flight suite's
    own record, so it stays unmet while any of the four checks is outstanding."""
    conn = db.connect("cli")

    assert criterion(db, "panel_preflight").measured == "no panel configuration"

    config = panel.create_config(conn, "operator", shortlist_n=50, w=0.2)
    run_id = scored_run(db, config)
    for name in preflight.CHECKS[:-1]:
        result = preflight.Result(name, True, "measured", "the check passes", {}, run_id)
        preflight.record(conn, config, result, "ada")
    outstanding = criterion(db, "panel_preflight")

    assert (outstanding.measured, outstanding.threshold, outstanding.met) == (
        f"config {config}, outstanding: dispersion",
        "the suite passed",
        False,
    )

    last = preflight.Result("dispersion", True, "measured", "the check passes", {}, run_id)
    preflight.record(conn, config, last, "ada")
    passed = criterion(db, "panel_preflight")

    assert (passed.measured, passed.met) == (f"config {config}, all four checks", True)


def test_the_mode_goes_live_only_once_every_criterion_is_met(db, monkeypatch, capsys):
    """Acceptance criterion 2 for the mode: `agb mode live` refuses while the paper record
    is short, refuses without an attestation, and records the move with the command."""
    code, out, _ = cli_main(db, monkeypatch, capsys, "mode")

    assert (code, out.splitlines()[0]) == (0, "Paper, with 8 criteria outstanding.")

    code, _, err = cli_main(db, monkeypatch, capsys, "mode", "live", "--attest", "sound")

    assert code == 1
    assert err.startswith("agb: refused: live is not earned yet: paper_rebalances,")
    assert err.endswith("outstanding in the paper record\n")

    earned_live(db)
    code, out, _ = cli_main(db, monkeypatch, capsys, "mode")

    assert out.splitlines()[0] == (
        "Paper has earned live. Switch it at the terminal with `agb mode live`."
    )
    assert all(c.met for c in ladder.live_status(db.connect("cli")))

    code, _, err = cli_main(db, monkeypatch, capsys, "mode", "live")

    assert (code, err) == (
        1,
        "agb: refused: a move to live needs your attestation that the paper record is sound\n",
    )

    attestation = "read every paper fill"
    code, out, _ = cli_main(
        db, monkeypatch, capsys, "mode", "live", "--attest", attestation, "--actor", "ada"
    )

    assert (code, out) == (0, f"the mode is live, at L0: {ladder.MEANING['L0']}.\n")
    assert rows(
        db,
        "SELECT e.mode, e.actor, e.reason, e.attestation, c.verb FROM agb.mode_event e"
        " JOIN agb.command c ON c.id = e.command_id",
    ) == [("live", "ada", "moved from paper by ada", attestation, "mode")]
    assert cli_main(db, monkeypatch, capsys, "mode")[1].splitlines()[0] == "Trading live."


# -- tightening, by itself -----------------------------------------------------------------


def test_a_rejection_at_l1_drops_the_system_back_to_l0(db):
    """Acceptance criterion 3: the human's rejection of a live rebalance is the end of L1,
    recorded as the system and naming the rebalance that caused it."""
    at_level(db, "L1")
    conn = db.connect("scheduler")

    assert ladder.demote(conn) is None

    settled(db, 1, outcome="rejected")
    dropped = ladder.demote(conn)

    assert dropped == f"dropped to L0 because the human rejected the live rebalance {PERIOD}r1"
    now = ladder.current(conn)
    assert (now.level, now.actor, now.attestation) == ("L0", "system", None)
    assert rows(
        db,
        "SELECT b.reference FROM agb.ladder_event e JOIN agb.basket b ON b.id = e.basket_id",
    ) == [(f"{PERIOD}r1",)]
    assert ladder.demote(conn) is None


def test_a_gate_breach_at_l2_drops_the_system_back_to_l0(db):
    """Acceptance criterion 3: a halt above L0 drops the level too, whatever the gate
    halted it over."""
    go_live(db)
    at_level(db, "L2")
    case = Case(target=replace(BASE, universe_size=1499))
    basket_id = insert(db, case.target)

    assert run(db, basket_id, case).state == "halted"

    assert (
        ladder.demote(db.connect("scheduler"))
        == f"dropped to L0 because the gate halted the live rebalance {PERIOD}"
    )
    assert ladder.current(db.connect("cli")).level == "L0"


def test_at_l0_there_is_nothing_to_drop(db):
    """L0 is the bottom: a rejection there changes nothing about the level."""
    settled(db, 1, outcome="rejected")

    assert ladder.demote(db.connect("scheduler")) is None
    assert rows(db, "SELECT count(*) FROM agb.ladder_event") == [(1,)]


# -- what each level does to a rebalance ---------------------------------------------------


def test_at_l1_a_clean_basket_proceeds_and_an_unusual_one_still_asks(db):
    """Acceptance criterion 4: at L1 a clean basket proceeds once the opt-out window has
    closed with no rejection, while a basket with unusual turnover is left to the human."""
    go_live(db)
    at_level(db, "L1")
    hold(db, {s: Decimal(300) for s in HELD})
    quiet = insert(db, BASE, reference=f"{PERIOD}r1")
    assert run(db, quiet, PRICED).state == "proposed"
    move(db, quiet, 2, "proposed", "pending", "bot", "telegram")
    busy = insert(db, BUSY.target, reference=f"{PERIOD}r2")
    assert run(db, busy, BUSY).state == "proposed"
    move(db, busy, 2, "proposed", "pending", "bot", "telegram")
    conn = db.connect("cli")
    assert approval.asks(conn, quiet) is None
    assert approval.asks(conn, busy) == (
        "turnover 50.0% is above the 30% a clean basket proceeds on"
    )

    proceeded = approval.proceed_all(db.connect("scheduler"), NOW, CALENDAR)

    assert proceeded == [f"{PERIOD}r1"]
    assert state(db, quiet) == ("approved", 3, "system", "scheduler")
    assert state(db, busy) == ("pending", 2, "bot", "telegram")


def test_a_flagged_basket_asks_whatever_its_turnover(db):
    """The panel's flags are the other reason a rebalance asks at L1."""
    flags = [("N01", "a late filing"), ("N02", "a restatement")]

    assert approval.ask_reason(flags, "0.0%") == "the panel flagged 2 of its names"
    assert approval.ask_reason([], "50.0%") == (
        "turnover 50.0% is above the 30% a clean basket proceeds on"
    )
    assert approval.ask_reason([], "no book") is None


def test_the_opt_out_window_is_the_submission_window(db):
    """The clean basket proceeds exactly when execution would otherwise have nothing to
    submit: the countdown closes as the submission window opens."""
    assert approval.OPT_OUT == execution.SUBMIT_WINDOW


def test_the_system_still_never_rejects_a_pending_basket(db):
    """L1 lets the system answer a pending basket one way only. A rejection is the human's
    word, so the loop's policy still refuses one however the mode and the level stand."""
    go_live(db)
    at_level(db, "L1")
    basket_id = insert(db, replace(BASE, orders=()))
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")

    with pytest.raises(errors.InsufficientPrivilege, match="only a human rejects"):
        move(db, basket_id, 3, "pending", "rejected", "bot", "telegram", actor="system")


def test_at_l2_the_approval_message_is_a_notification_with_no_prompt(db):
    """Acceptance criterion 5: the gate approves the basket at L2 and the message only says
    what is being sent."""
    go_live(db)
    at_level(db, "L2")
    hold(db, {s: Decimal(300) for s in HELD})
    basket_id = insert(db, BASE)
    assert run(db, basket_id, PRICED).state == "approved"
    assert state(db, basket_id) == ("approved", 2, "system", "pipeline")

    told = approval.notify(db.connect("bot"), NOW, CALENDAR)

    assert told == [PERIOD]
    [(body,)] = rows(db, "SELECT body FROM agb.alert WHERE kind = 'approval_request'")
    assert body.splitlines()[:2] == [
        f"REBALANCE {PERIOD} — proceeding",
        "LIVE · L2 · sending at 15:45 ET",
    ]
    assert body.endswith(
        "/basket   /level\nNothing is needed from you: the trades are sent at the close. "
        "To stop them: /pause"
    )
    assert "/approve" not in body
    assert approval.notify(db.connect("bot"), NOW, CALENDAR) == []


# -- /level --------------------------------------------------------------------------------


def test_level_shows_the_level_and_the_progress_and_offers_no_way_to_move_up(db):
    """Acceptance criterion 7: `/level` reports where the system stands and what the next
    level needs, and points at the terminal rather than at a command it could run."""
    clean(db, 3)

    [reply] = ask(db, "/level")

    assert reply.splitlines()[:2] == [
        f"Level L0 since {datetime.now(UTC):%Y-%m-%d}, set by system.",
        ladder.MEANING["L0"],
    ]
    assert "Towards L1:" in reply
    assert "  clean_rebalances: 3 (needs >= 6) — unmet" in reply
    assert "L1 is not earned yet. The ladder loosens only at the terminal." in reply
    assert "The pre-trade gate and the kill-switch apply at every level." in reply
    assert "agb level" not in reply

    clean(db, 3, first=4)
    [earned] = ask(db, "/level")

    assert "  clean_rebalances: 6 (needs >= 6) — met" in earned
    assert "L1 is earned. Raise it at the terminal with `agb level L1`." in earned
