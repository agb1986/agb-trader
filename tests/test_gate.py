"""The pre-trade gate: every check against a sound basket and against a fixture built to fail
it alone, then the gate in the database. Each check is recorded with what it measured, a single
failure halts the basket and opens the halt latch's `gate_breach` until a human clears it, the
gated basket and its result are frozen, the mode starts in paper, and only a basket that passed
every check in paper mode is approved, by the system. tests/test_basket.py runs the gate from
the pipeline end to end, and tests/test_registry.py on a basket ranked by a real promoted
model."""

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
import registry_fake
from market_fake import PERIOD, SIGNAL, TRADE
from psycopg import errors
from registry_fake import ARTIFACT

from agb import gate
from agb.gate import GateError

NAMES = [f"N{i:02d}" for i in range(1, 21)]
HELD = [f"H{i:02d}" for i in range(1, 11)]
MODEL = registry_fake.MODEL_ID
# The live model, as agb.promoted_model has it, and the same model voided.
PROMOTED = gate.Promoted(1, MODEL, ARTIFACT, False)
VOIDED = replace(PROMOTED, voided=True)
ZERO_HASH = "0" * 64
# The evening of the signal session, well before the trade session's MOC cutoff.
EVENING = datetime(2026, 9, 30, 22, 0, tzinfo=UTC)
# 15:45 ET on the trade session: the cutoff itself, which is too late.
LATE = datetime(2026, 10, 1, 19, 45, tzinfo=UTC)


def targets(symbols, weight=Decimal("0.05"), shares=Decimal(240)):
    return tuple(gate.Target(s, weight, shares) for s in symbols)


def buy(symbol, qty=Decimal(240), notional=Decimal(4800), tif="cls"):
    return gate.Order(symbol, "buy", qty, tif, notional)


def sell(symbol, qty=Decimal(300), notional=Decimal(6000)):
    return gate.Order(symbol, "sell", qty, "cls", notional)


# Twenty equal-weight buys at $20 a share into an empty book, $4,000 left in cash.
BASE = gate.Basket(
    id=0,
    period=PERIOD,
    session_date=TRADE,
    snapshot_date=SIGNAL,
    universe_size=2000,
    ranker=MODEL,
    model_sha256=ARTIFACT,
    capital=Decimal(100000),
    targets=targets(NAMES),
    orders=tuple(buy(s) for s in NAMES),
)
MEMBERS = frozenset(NAMES) | {"SPARE"}
MARKS = {s: Decimal(20) for s in NAMES}


@dataclass(frozen=True)
class Case:
    """A basket and what the gate reads beside it."""

    target: gate.Basket = BASE
    members: frozenset[str] = MEMBERS
    marks: dict[str, Decimal] = field(default_factory=lambda: dict(MARKS))
    book: dict[str, Decimal] = field(default_factory=dict)
    now: datetime = EVENING
    promoted: gate.Promoted | None = PROMOTED


SOUND = Case()


def evaluate(case=SOUND):
    return gate.evaluate(
        case.target,
        members=case.members,
        marks=case.marks,
        book=case.book,
        now=case.now,
        promoted=case.promoted,
    )


# Selling seven of ten held positions, $42,000 of a $60,000 book: 70% turnover.
TURNOVER = Case(
    replace(
        BASE,
        targets=targets(HELD[:3], shares=Decimal(300)) + targets(NAMES[:17]),
        orders=tuple(sell(s) for s in HELD[3:]) + tuple(buy(s) for s in NAMES[:17]),
    ),
    marks=MARKS | {s: Decimal(20) for s in HELD},
    book={s: Decimal(300) for s in HELD},
)

# Each fixture fails exactly the check it names, and nothing else.
FAILURES = [
    pytest.param(Case(replace(BASE, snapshot_date=date(2026, 9, 29))), "snapshot_date",
                 id="snapshot_date"),
    pytest.param(Case(replace(BASE, universe_size=1499)), "universe_size", id="universe_small"),
    pytest.param(Case(replace(BASE, universe_size=3501)), "universe_size", id="universe_large"),
    pytest.param(Case(replace(BASE, model_sha256="cd" * 32)), "model", id="model_hash"),
    pytest.param(Case(replace(BASE, model_sha256=None)), "model", id="model_uncertified"),
    pytest.param(Case(promoted=None), "model", id="model_unpromoted"),
    pytest.param(Case(promoted=VOIDED), "model", id="model_voided"),
    pytest.param(
        Case(replace(BASE, targets=targets(NAMES[:19], Decimal(1) / 19),
                     orders=tuple(buy(s) for s in NAMES[:19]))),
        "positions", id="positions",
    ),
    pytest.param(
        Case(replace(BASE, targets=targets(NAMES[:19]) + targets(["N20"], Decimal("0.06")))),
        "weight_sum", id="weight_sum",
    ),
    pytest.param(Case(replace(BASE, targets=targets(NAMES[:19] + ["N01"]))), "duplicates",
                 id="duplicates"),
    pytest.param(Case(replace(BASE, capital=Decimal(95000))), "cash", id="cash"),
    pytest.param(Case(marks={s: p for s, p in MARKS.items() if s != "N05"}), "cash",
                 id="cash_unmarked"),
    pytest.param(TURNOVER, "turnover", id="turnover"),
    pytest.param(Case(members=MEMBERS - {"N05"}), "universe", id="universe"),
    pytest.param(Case(book={"N05": Decimal(100)}), "side", id="side"),
    pytest.param(
        Case(replace(BASE, orders=(buy("N01", notional=Decimal(9000)), *BASE.orders[1:]))),
        "notional", id="notional",
    ),
    pytest.param(Case(replace(BASE, orders=(*BASE.orders[:19], buy("N20", tif="day")))), "tif",
                 id="tif"),
    pytest.param(Case(now=LATE), "clock", id="clock"),
]  # fmt: skip
# The schema refuses these baskets outright, so the gate only ever sees them in memory.
REFUSED_BY_SCHEMA = {"duplicates", "tif"}


# The checks.


def test_a_sound_basket_passes_every_check_and_each_says_what_it_measured():
    checks = evaluate()

    assert len(checks) == 3 + 5 + 5 * len(NAMES)
    assert all(c.passed for c in checks)
    assert [(c.level, c.name, c.symbol, c.measured, c.threshold) for c in checks[:8]] == [
        ("provenance", "snapshot_date", None, "2026-09-30", "2026-09-30"),
        ("provenance", "universe_size", None, "2000", "1500 to 3500"),
        ("provenance", "model", None, f"{MODEL} {ARTIFACT[:12]}", f"{MODEL} {ARTIFACT[:12]}"),
        ("basket", "positions", None, "20", "20"),
        ("basket", "weight_sum", None, "1.00", "1 within 0.0001"),
        ("basket", "duplicates", None, "none", "none"),
        ("basket", "cash", None, "4000.00", ">= 0"),
        ("basket", "turnover", None, "no book", "< 60%"),
    ]
    assert [(c.level, c.name, c.symbol, c.measured, c.threshold) for c in checks[8:13]] == [
        ("order", "universe", "N01", "in the universe", "in the universe, or held for a sell"),
        ("order", "side", "N01", "buy 240, holding 0",
         "a buy opens a position; a sell closes at most what is held"),
        ("order", "notional", "N01", "4.80% of 100000.00", "<= 8%"),
        ("order", "tif", "N01", "cls", "cls"),
        ("order", "clock", "N01", "2026-09-30 18:00 ET", "before 2026-10-01 15:45 ET"),
    ]  # fmt: skip
    assert [c.symbol for c in checks[8:]] == [s for s in NAMES for _ in range(5)]


@pytest.mark.parametrize(("case", "name"), FAILURES)
def test_each_check_has_a_fixture_that_fails_it_alone(case, name):
    assert {c.name for c in evaluate(case) if not c.passed} == {name}


def test_every_check_is_covered_by_a_failing_fixture():
    assert {p.values[1] for p in FAILURES} == {c.name for c in evaluate()}


def test_the_failing_checks_say_what_they_measured():
    def failed(case):
        return [(c.symbol, c.measured) for c in evaluate(case) if not c.passed]

    assert failed(TURNOVER) == [(None, "70.00%")]
    assert failed(Case(replace(BASE, capital=Decimal(95000)))) == [(None, "-1000.00")]
    assert failed(Case(marks={})) == [(None, f"no mark for {', '.join(NAMES)}")]
    assert failed(Case(book={"N05": Decimal(100)})) == [("N05", "buy 240, holding 100")]
    assert failed(Case(now=LATE)) == [(s, "2026-10-01 15:45 ET") for s in NAMES]


def test_a_held_name_that_left_the_universe_may_still_be_sold():
    checks = {(c.name, c.symbol): c for c in evaluate(TURNOVER)}
    assert "H10" not in TURNOVER.members
    assert checks["universe", "H10"].measured == "held, not in the universe"
    assert checks["universe", "H10"].passed


def test_a_sell_may_close_no_more_than_the_book_holds():
    orders = (sell("H04", qty=Decimal(301)), *TURNOVER.target.orders[1:])
    case = replace(TURNOVER, target=replace(TURNOVER.target, orders=orders))
    [side] = [c for c in evaluate(case) if c.name == "side" and c.symbol == "H04"]
    assert (side.measured, side.passed) == ("sell 301, holding 300", False)


def test_the_cutoff_moves_with_an_early_close():
    # The day after Thanksgiving closes at 13:00, so its cutoff is 12:45 ET.
    friday = date(2026, 11, 27)
    case = Case(replace(BASE, period="2026-11", session_date=friday,
                        snapshot_date=date(2026, 10, 30)),
                now=datetime(2026, 11, 27, 17, 50, tzinfo=UTC))  # fmt: skip
    [clock, *_] = [c for c in evaluate(case) if c.name == "clock"]
    assert (clock.measured, clock.threshold, clock.passed) == (
        "2026-11-27 12:50 ET",
        "before 2026-11-27 12:45 ET",
        False,
    )


# The gate in the database.


def rows(db, query, *params):
    return db.connect("cli").execute(query, params).fetchall()


def insert(db, target=BASE, reference=None, proposed=True):
    """Write `target` as the pipeline does, and propose it unless told otherwise. A basket with
    a model digest names the stand-in model's certification, which this records."""
    certification_id = registry_fake.certify(db) if target.model_sha256 else None
    conn = db.connect("pipeline")
    reference = reference or target.period
    with conn.transaction():
        [(basket_id,)] = conn.execute(
            "INSERT INTO agb.basket (reference, period, session_date, signal_date, snapshot_date,"
            " partition_hash, universe_size, ranker, model_certification_id, model_sha256,"
            " cost_model_version, capital_usd, actor)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '1', %s, 'pipeline') RETURNING id",
            (
                reference,
                target.period,
                target.session_date,
                target.snapshot_date,
                target.snapshot_date,
                ZERO_HASH,
                target.universe_size,
                target.ranker,
                certification_id,
                target.model_sha256,
                target.capital,
            ),
        ).fetchall()
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO agb.basket_target (basket_id, symbol, weight, shares, held)"
                " VALUES (%s, %s, %s, %s, false)",
                [(basket_id, t.symbol, t.weight, t.shares) for t in target.targets],
            )
            cur.executemany(
                "INSERT INTO agb.basket_order (basket_id, client_order_id, symbol, side, qty,"
                " reference_price, notional_usd, cost_model_version, floor_bps, impact_bps,"
                " fees_usd, modelled_cost_bps)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, '1', 0, 0, 0, 0)",
                [
                    (basket_id, f"{reference}-{o.symbol}", o.symbol, o.side, o.qty,
                     o.notional / o.qty, o.notional)
                    for o in target.orders
                ],
            )  # fmt: skip
        if proposed:
            conn.execute(
                "INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor,"
                " surface) VALUES (%s, 1, NULL, 'proposed', 'pipeline', 'pipeline')",
                (basket_id,),
            )
    return basket_id


def move(db, basket_id, seq, from_state, to_state, role, surface=None, actor=None):
    db.connect(role).execute(
        "INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface)"
        " VALUES (%s, %s, %s, %s, %s, %s)",
        (basket_id, seq, from_state, to_state, actor or role, surface or role),
    )


def hold(db, positions):
    """Last month's basket, filled, so the book holds `positions` (symbol: shares)."""
    held = replace(
        BASE,
        period="2026-09",
        session_date=date(2026, 9, 1),
        snapshot_date=date(2026, 8, 31),
        targets=tuple(gate.Target(s, Decimal("0.05"), n) for s, n in positions.items()),
        orders=(),
    )
    basket_id = insert(db, held)
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
    move(db, basket_id, 3, "pending", "approved", "bot", "telegram")
    move(db, basket_id, 4, "approved", "submitted", "execution")
    move(db, basket_id, 5, "submitted", "filled", "execution")
    return basket_id


def run(db, basket_id, case=SOUND):
    """Gate the basket, with the stand-in model promoted (and voided) as `case` says."""
    if case.promoted is not None:
        registry_fake.promote(db)
        if case.promoted.voided:
            registry_fake.void(db)
    return gate.run(
        db.connect("pipeline"), basket_id, members=case.members, marks=case.marks, now=case.now
    )


def state(db, basket_id):
    return rows(
        db, "SELECT state, seq, actor, surface FROM agb.basket_state WHERE id = %s", basket_id
    )[0]


def approve(db, basket_id, actor="system"):
    move(db, basket_id, 2, "proposed", "approved", "pipeline", actor=actor)


def test_a_pass_in_paper_records_every_check_and_the_system_approves(db):
    basket_id = insert(db)

    verdict = run(db, basket_id)

    assert (verdict.mode, verdict.state, verdict.passed, verdict.recorded) == (
        "paper",
        "approved",
        True,
        False,
    )
    assert rows(db, "SELECT basket_id, checks, mode, actor FROM agb.gate_run") == [
        (basket_id, 108, "paper", "pipeline")
    ]
    recorded = rows(
        db,
        "SELECT level, name, symbol, measured, threshold, passed FROM agb.gate_check ORDER BY id",
    )
    assert recorded == [
        (c.level, c.name, c.symbol, c.measured, c.threshold, c.passed) for c in evaluate()
    ]
    assert state(db, basket_id) == ("approved", 2, "system", "pipeline")
    assert rows(db, "SELECT reason FROM agb.basket_event WHERE seq = 2") == [
        ("passed the pre-trade gate in paper mode",)
    ]
    assert rows(db, "SELECT cause FROM agb.halt_open") == []


@pytest.mark.parametrize(("case", "name"), [p for p in FAILURES if p.id not in REFUSED_BY_SCHEMA])
def test_any_single_failure_halts_the_basket_and_opens_the_latch(db, case, name):
    if case.book:
        hold(db, case.book)
    basket_id = insert(db, case.target)

    verdict = run(db, basket_id, case)

    assert (verdict.state, {c.name for c in verdict.failed}) == ("halted", {name})
    assert state(db, basket_id) == ("halted", 2, "pipeline", "pipeline")
    assert {n for (n,) in rows(db, "SELECT name FROM agb.gate_check WHERE NOT passed")} == {name}
    [(cause, halted, actor, reason)] = rows(
        db, "SELECT cause, basket_id, actor, reason FROM agb.halt_open"
    )
    assert (cause, halted, actor) == ("gate_breach", basket_id, "pipeline")
    assert reason.startswith(f"pre-trade gate failed: {name}")
    assert rows(
        db, "SELECT reason FROM agb.basket_event WHERE basket_id = %s AND seq = 2", basket_id
    ) == [(reason,)]


def test_the_breach_stays_open_until_a_human_clears_it(db):
    run(db, insert(db), Case(now=LATE))
    open_causes = "SELECT cause FROM agb.halt_open"
    assert rows(db, open_causes) == [("gate_breach",)]

    # The pipeline sets the breach; it cannot clear it or set another cause.
    pipeline = db.connect("pipeline")
    for cause, action in [("gate_breach", "clear"), ("pause", "set"), ("liquidation", "set")]:
        with pytest.raises(errors.InsufficientPrivilege):
            pipeline.execute(
                "INSERT INTO agb.halt_event (cause, action, actor) VALUES (%s, %s, 'pipeline')",
                (cause, action),
            )
    # Execution sets its own cause, and nothing else.
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("execution").execute(
            "INSERT INTO agb.halt_event (cause, action, actor)"
            " VALUES ('gate_breach', 'clear', 'execution')"
        )
    for role in ("scheduler", "cli"):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect(role).execute(
                "INSERT INTO agb.halt_event (cause, action, actor) VALUES ('pause', 'set', %s)",
                (role,),
            )
    assert rows(db, open_causes) == [("gate_breach",)]

    db.connect("bot").execute(
        "INSERT INTO agb.halt_event (cause, action, actor, reason)"
        " VALUES ('gate_breach', 'clear', 'bot', '/resume')"
    )
    assert rows(db, open_causes) == []
    assert rows(db, "SELECT cause, action, actor FROM agb.halt_event ORDER BY id") == [
        ("gate_breach", "set", "pipeline"),
        ("gate_breach", "clear", "bot"),
    ]

    # Each cause latches on its own.
    db.connect("ui").execute(
        "INSERT INTO agb.halt_event (cause, action, actor) VALUES ('pause', 'set', 'ui')"
    )
    db.connect("execution").execute(
        "INSERT INTO agb.halt_event (cause, action, actor)"
        " VALUES ('execution_lease_expired', 'set', 'execution')"
    )
    db.connect("ui").execute(
        "INSERT INTO agb.halt_event (cause, action, actor) VALUES ('pause', 'clear', 'ui')"
    )
    assert rows(db, open_causes) == [("execution_lease_expired",)]


def test_the_halt_history_is_append_only(db):
    run(db, insert(db), Case(now=LATE))
    owner = db.connect("agb_owner")
    for statement in ("UPDATE agb.halt_event SET action = 'clear'", "DELETE FROM agb.halt_event"):
        with pytest.raises(errors.RaiseException):
            owner.execute(statement)
    assert rows(db, "SELECT cause FROM agb.halt_open") == [("gate_breach",)]


def go_live(db, reason="promoted"):
    """Move the mode to live as `agb mode live` records it: a command and an attestation."""
    conn = db.connect("agb_owner")
    [(command_id,)] = conn.execute(
        "INSERT INTO agb.command (actor, surface, raw_text, verb, outcome)"
        " VALUES ('ada', 'cli', 'agb mode live', 'mode', 'accepted') RETURNING id"
    ).fetchall()
    conn.execute(
        "INSERT INTO agb.mode_event (mode, actor, reason, attestation, command_id)"
        " VALUES ('live', 'cli', %s, 'the paper record is sound', %s)",
        (reason, command_id),
    )


def test_once_the_gate_has_run_the_basket_and_its_result_are_frozen(db):
    go_live(db)
    basket_id = insert(db)
    run(db, basket_id)
    assert state(db, basket_id)[0] == "proposed"
    pipeline = db.connect("pipeline")

    for statement in (
        "INSERT INTO agb.basket_rank (basket_id, symbol, rank, score) VALUES (%s, 'X', 99, 0)",
        "INSERT INTO agb.basket_target (basket_id, symbol, weight, shares, held)"
        " VALUES (%s, 'X', 0.05, 1, false)",
        "INSERT INTO agb.basket_order (basket_id, client_order_id, symbol, side, qty,"
        " reference_price, notional_usd, cost_model_version, floor_bps, impact_bps, fees_usd,"
        " modelled_cost_bps) VALUES (%s, 'late-X', 'X', 'buy', 1, 1, 1, '1', 0, 0, 0, 0)",
    ):
        with pytest.raises(errors.CheckViolation, match="content is frozen once the gate has run"):
            pipeline.execute(statement, (basket_id,))

    [(run_id,)] = rows(db, "SELECT id FROM agb.gate_run")
    with pytest.raises(errors.CheckViolation, match="its 108 checks are recorded"):
        pipeline.execute(
            "INSERT INTO agb.gate_check (run_id, level, name, measured, threshold, passed)"
            " VALUES (%s, 'basket', 'extra', 'x', 'y', true)",
            (run_id,),
        )
    with pytest.raises(errors.UniqueViolation):
        pipeline.execute(
            "INSERT INTO agb.gate_run (basket_id, checks, mode, actor)"
            " VALUES (%s, 1, 'paper', 'pipeline')",
            (basket_id,),
        )
    owner = db.connect("agb_owner")
    with pytest.raises(errors.RaiseException):
        owner.execute("UPDATE agb.gate_check SET passed = NOT passed")
    assert rows(db, "SELECT count(*) FROM agb.gate_check") == [(108,)]
    assert rows(db, "SELECT count(*) FROM agb.basket_target WHERE basket_id = %s", basket_id) == [
        (20,)
    ]


def test_a_second_run_reads_the_recorded_verdict_back(db):
    basket_id = insert(db)
    first = run(db, basket_id)

    # A clock that would now fail every order: the recorded verdict stands.
    again = run(db, basket_id, Case(now=LATE))

    assert (again.recorded, again.state, again.checks) == (True, "approved", first.checks)
    assert rows(db, "SELECT count(*) FROM agb.gate_run") == [(1,)]
    assert rows(db, "SELECT count(*) FROM agb.gate_check") == [(108,)]
    assert rows(db, "SELECT count(*) FROM agb.basket_event") == [(2,)]


def test_the_gate_runs_on_a_proposed_basket_only(db):
    with pytest.raises(GateError, match="is unproposed, not proposed"):
        run(db, insert(db, proposed=False))
    halted = insert(db, reference="2026-10r1")
    move(db, halted, 2, "proposed", "halted", "pipeline")
    with pytest.raises(GateError, match="is halted, not proposed"):
        run(db, halted)
    with pytest.raises(errors.CheckViolation, match="the gate runs on a proposed basket"):
        db.connect("pipeline").execute(
            "INSERT INTO agb.gate_run (basket_id, checks, mode, actor)"
            " VALUES (%s, 1, 'paper', 'pipeline')",
            (halted,),
        )
    assert rows(db, "SELECT count(*) FROM agb.gate_run") == [(0,)]


# The mode.


def test_the_system_starts_in_paper(db):
    assert rows(db, "SELECT mode, actor FROM agb.current_mode") == [("paper", "system")]
    assert rows(db, "SELECT mode, actor, reason FROM agb.mode_event") == [
        ("paper", "system", "the system starts in paper")
    ]


# The CLI's own move up to live, which needs an attestation and the command that asked for
# it, is tested in tests/test_ladder.py.
def test_only_the_system_moves_the_mode_and_only_down_to_paper(db):
    for role in ("scheduler", "execution"):
        conn = db.connect(role)
        conn.execute(
            "INSERT INTO agb.mode_event (mode, actor, reason) VALUES ('paper', 'system', %s)",
            (f"{role} drops to paper",),
        )
        for mode, actor in [("live", "system"), ("paper", role)]:
            with pytest.raises(errors.InsufficientPrivilege):
                conn.execute(
                    "INSERT INTO agb.mode_event (mode, actor) VALUES (%s, %s)", (mode, actor)
                )
    for role in ("pipeline", "bot", "ui"):
        with pytest.raises(errors.InsufficientPrivilege):
            db.connect(role).execute(
                "INSERT INTO agb.mode_event (mode, actor) VALUES ('paper', 'system')"
            )
    assert rows(db, "SELECT count(*) FROM agb.mode_event") == [(3,)]


def test_in_live_mode_a_pass_waits_for_the_human(db):
    go_live(db)
    basket_id = insert(db)

    verdict = run(db, basket_id)

    assert (verdict.mode, verdict.state, verdict.passed) == ("live", "proposed", True)
    assert rows(db, "SELECT mode FROM agb.gate_run") == [("live",)]
    assert state(db, basket_id) == ("proposed", 1, "pipeline", "pipeline")
    assert rows(db, "SELECT cause FROM agb.halt_open") == []
    unasked = "unasked only at L2, and the level is L0"
    with pytest.raises(errors.InsufficientPrivilege, match=unasked):
        approve(db, basket_id)

    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
    move(db, basket_id, 3, "pending", "approved", "bot", "telegram")
    assert state(db, basket_id) == ("approved", 3, "bot", "telegram")


def record(db, basket_id, declared, passed):
    """A gate run declaring `declared` checks, with one recorded check per entry of
    `passed`, written by hand so the basket stays proposed."""
    conn = db.connect("pipeline")
    [(run_id,)] = conn.execute(
        "INSERT INTO agb.gate_run (basket_id, checks, mode, actor)"
        " VALUES (%s, %s, 'paper', 'pipeline') RETURNING id",
        (basket_id, declared),
    ).fetchall()
    for i, ok in enumerate(passed):
        conn.execute(
            "INSERT INTO agb.gate_check (run_id, level, name, measured, threshold, passed)"
            " VALUES (%s, 'basket', %s, 'x', 'y', %s)",
            (run_id, f"check_{'abc'[i]}", ok),
        )


def test_the_system_approves_only_a_basket_that_passed_every_recorded_check(db):
    refused = errors.InsufficientPrivilege
    ungated = insert(db)
    with pytest.raises(refused, match="the gate has not run"):
        approve(db, ungated)
    with pytest.raises(refused, match="the gate approves as system, not pipeline"):
        approve(db, ungated, "pipeline")

    partial = insert(db, reference="2026-10r1")
    record(db, partial, 2, [True])
    with pytest.raises(refused, match="the gate recorded 1 of its 2 checks"):
        approve(db, partial)

    failed = insert(db, reference="2026-10r2")
    record(db, failed, 3, [True, False, False])
    with pytest.raises(refused, match="2 gate checks failed"):
        approve(db, failed)

    passed = insert(db, reference="2026-10r3")
    record(db, passed, 2, [True, True])
    with pytest.raises(refused, match="the gate approves as system, not pipeline"):
        approve(db, passed, "pipeline")
    approve(db, passed)
    assert state(db, passed) == ("approved", 2, "system", "pipeline")
    assert [s for (s,) in rows(db, "SELECT state FROM agb.basket_state ORDER BY id")] == [
        "proposed",
        "proposed",
        "proposed",
        "approved",
    ]
