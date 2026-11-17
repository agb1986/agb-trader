"""The pre-trade gate: every check against a sound basket and against a fixture built to fail
it alone, then the gate in the database. Each check is recorded with what it measured, a single
failure halts the basket and opens the halt latch's `gate_breach` until a human clears it, the
gated basket and its result are frozen, the mode starts in paper, and only a basket that passed
every check in paper mode is approved, by the system. tests/test_basket.py runs the gate from
the pipeline end to end, and tests/test_registry.py on a basket ranked by a real promoted
model."""

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
import registry_fake
from account_fake import read_book
from market_fake import PERIOD, SIGNAL, TRADE
from psycopg import errors
from registry_fake import ARTIFACT

from agb import approval, basket, corporate, gate, panel
from agb.gate import GateError
from agb.roles import OWNER, SERVICE_ROLES

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


def buy(symbol, qty=Decimal(240), notional=Decimal(4800), tif="cls", cost=Decimal(0)):
    return gate.Order(symbol, "buy", qty, tif, notional, cost)


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
    held: gate.Held | None = None


SOUND = Case()


def evaluate(case=SOUND):
    return gate.evaluate(
        case.target,
        members=case.members,
        marks=case.marks,
        book=case.book,
        now=case.now,
        promoted=case.promoted,
        held=case.held,
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

# Selling all ten held positions, $60,000 of a $60,000 book: a full rotation, 100% turnover.
ROTATION = Case(
    replace(BASE, orders=tuple(sell(s) for s in HELD) + BASE.orders),
    marks=MARKS | {s: Decimal(20) for s in HELD},
    book={s: Decimal(300) for s in HELD},
)
# The model behind the book: the one that ranked the basket, then three that are not it, or
# are not on the record as not being it.
SAME_MODEL = gate.Held(MODEL, ARTIFACT)
OTHER_RANKER = gate.Held("placeholder-momentum-252-21", ZERO_HASH)
RETRAINED = gate.Held(MODEL, ZERO_HASH)
UNRECORDED = gate.Held(MODEL, None)

# A kept name, N01, that a partial fill left at 100 shares: $2,000, 2% of the book, three
# points under its twentieth, with no order to top it up.
UNDERWEIGHT = Case(
    replace(
        BASE,
        targets=targets(["N01"], shares=Decimal(100)) + targets(NAMES[1:]),
        orders=BASE.orders[1:],
    ),
    book={"N01": Decimal(100)},
)
# The same name topped back up: 150 more shares bring it to 250, $5,000.
TOPPED_UP = replace(
    UNDERWEIGHT,
    target=replace(
        UNDERWEIGHT.target,
        targets=targets(["N01"], shares=Decimal(250)) + targets(NAMES[1:]),
        orders=(buy("N01", qty=Decimal(150), notional=Decimal(3000)), *BASE.orders[1:]),
    ),
)
# Shares at $2,600, one to a name: each name is short of its $5,000 by less than a share, so
# each passes, but the basket holds $52,000 of $100,000, far short of fully invested.
DEAR = {s: Decimal(2600) for s in NAMES}
UNDERINVESTED = Case(
    replace(
        BASE,
        targets=targets(NAMES, shares=Decimal(1)),
        orders=tuple(buy(s, qty=Decimal(1), notional=Decimal(2600)) for s in NAMES),
    ),
    marks=DEAR,
)
# Twenty $4,800 buys against $96,500 of equity, each modelled to cost $30: the targets fit,
# but the $600 the trading costs leaves the account $100 overdrawn.
OVERSPENT = Case(
    replace(
        BASE,
        capital=Decimal(96500),
        orders=tuple(buy(s, cost=Decimal(30)) for s in NAMES),
    )
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
        Case(replace(BASE, targets=targets(NAMES[:19]),
                     orders=tuple(buy(s) for s in NAMES[:19]))),
        "positions", id="positions",
    ),
    pytest.param(UNDERINVESTED, "weight_sum", id="weight_sum"),
    pytest.param(UNDERWEIGHT, "weights", id="weights_under"),
    pytest.param(
        Case(replace(BASE, targets=targets(["N01"], shares=Decimal(310)) + targets(NAMES[1:]),
                     orders=(buy("N01", qty=Decimal(310), notional=Decimal(6200)),
                             *BASE.orders[1:]))),
        "weights", id="weights_over",
    ),
    pytest.param(Case(replace(BASE, targets=targets(NAMES[:19] + ["N01"]))), "duplicates",
                 id="duplicates"),
    pytest.param(OVERSPENT, "cash", id="cash"),
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

    assert len(checks) == 3 + 6 + 5 * len(NAMES)
    assert all(c.passed for c in checks)
    assert [(c.level, c.name, c.symbol, c.measured, c.threshold) for c in checks[:9]] == [
        ("provenance", "snapshot_date", None, "2026-09-30", "2026-09-30"),
        ("provenance", "universe_size", None, "2000", "1500 to 3500"),
        ("provenance", "model", None, f"{MODEL} {ARTIFACT[:12]}", f"{MODEL} {ARTIFACT[:12]}"),
        (
            "basket",
            "positions",
            None,
            "20",
            "20, or fewer where a recorded forced event explains each one",
        ),
        ("basket", "weight_sum", None, "0.9600", "0.8000 to 1"),
        (
            "basket",
            "weights",
            None,
            "4.80% to 4.80%",
            "each name 5.00% within 1.00%, or under by at most one share;"
            " a kept name not bought may sit over, and a carried name is exempt",
        ),
        ("basket", "duplicates", None, "none", "none"),
        ("basket", "cash", None, "4000.00", ">= 0 after modelled costs"),
        ("basket", "turnover", None, "no book", "< 60%"),
    ]
    orders = checks[9:]
    assert [(c.level, c.name, c.symbol, c.measured, c.threshold) for c in orders[:5]] == [
        ("order", "universe", "N01", "in the universe", "in the universe, or held for a sell"),
        ("order", "side", "N01", "buy 240, holding 0",
         "a buy opens a position or tops a kept one up to its target;"
         " a sell closes at most what is held"),
        ("order", "notional", "N01", "4.80% of 100000.00", "<= 8%"),
        ("order", "tif", "N01", "cls", "cls"),
        ("order", "clock", "N01", "2026-09-30 18:00 ET", "before 2026-10-01 15:45 ET"),
    ]  # fmt: skip
    assert [c.symbol for c in orders] == [s for s in NAMES for _ in range(5)]


@pytest.mark.parametrize(("case", "name"), FAILURES)
def test_each_check_has_a_fixture_that_fails_it_alone(case, name):
    assert {c.name for c in evaluate(case) if not c.passed} == {name}


def test_every_check_is_covered_by_a_failing_fixture():
    assert {p.values[1] for p in FAILURES} == {c.name for c in evaluate()}


def test_the_failing_checks_say_what_they_measured():
    def failed(case):
        return [(c.symbol, c.measured) for c in evaluate(case) if not c.passed]

    assert failed(TURNOVER) == [(None, "70.00%")]
    assert failed(UNDERINVESTED) == [(None, "0.5200")]
    assert failed(UNDERWEIGHT) == [(None, "N01 2.00%")]
    assert failed(OVERSPENT) == [(None, "-100.00")]
    unmarked = f"no mark for {', '.join(NAMES)}"
    assert failed(Case(marks={})) == [
        (None, unmarked),
        (None, ", ".join(f"{s} no mark" for s in NAMES)),
        (None, unmarked),
    ]
    assert failed(Case(book={"N05": Decimal(100)})) == [("N05", "buy 240, holding 100")]
    assert failed(Case(now=LATE)) == [(s, "2026-10-01 15:45 ET") for s in NAMES]


def test_a_kept_name_topped_up_to_its_target_passes_the_gate():
    """A partial fill left N01 at 2% of the book. The buy that brings it back to its twentieth
    is a buy on a held name, which the gate admits because it lands on the target."""
    checks = {(c.name, c.symbol): c for c in evaluate(TOPPED_UP)}

    assert all(c.passed for c in checks.values())
    assert checks[("weights", None)].measured == "4.80% to 5.00%"
    assert checks[("side", "N01")].measured == "buy 150, holding 100"


def test_a_buy_on_a_held_name_that_misses_its_target_fails_the_side_check():
    short_of_target = replace(
        TOPPED_UP,
        target=replace(
            TOPPED_UP.target,
            orders=(buy("N01", qty=Decimal(100), notional=Decimal(2000)), *BASE.orders[1:]),
        ),
    )

    assert [(c.name, c.symbol) for c in evaluate(short_of_target) if not c.passed] == [
        ("side", "N01")
    ]


def test_a_basket_whose_buys_exceed_its_cash_fails_the_gate():
    """Twenty $4,800 buys fit $100,000 of equity with $4,000 to spare. Against $95,000 the same
    buys leave the account $1,000 short before any cost, and the cash check refuses them."""
    overdrawn = Case(replace(BASE, capital=Decimal(95000)))
    cash = {c.name: c for c in evaluate(overdrawn)}["cash"]

    assert (cash.measured, cash.passed) == ("-1000.00", False)


def test_a_changed_ranker_admits_a_full_rotation_and_the_check_records_what_admitted_it():
    """The limit catches a broken pipeline, not a bad month. Two rankings share no rank buffer,
    so a basket ranked by a model that did not buy the book has a declared reason to turn it
    over, and the check says which change let it through."""
    exempt = {c.name: c for c in evaluate(replace(ROTATION, held=OTHER_RANKER))}["turnover"]

    assert exempt.measured == "100.00%"
    assert exempt.threshold == (
        f"< 60%, or <= 100% on a changed model (placeholder-momentum-252-21 -> {MODEL})"
    )
    assert exempt.passed
    # The measured value stays a bare percentage, because approval.ask_reason parses it to
    # decide whether to put unusual turnover to the human, and the exemption must not silence
    # that on the months that rotate hardest.
    assert approval.ask_reason([], exempt.measured) is not None


def test_a_retrain_under_the_same_name_is_a_changed_model():
    """The ranker name is not the model. Two digests on the record that differ are a different
    artifact whatever it is called."""
    exempt = {c.name: c for c in evaluate(replace(ROTATION, held=RETRAINED))}["turnover"]

    assert exempt.threshold.endswith(f"({MODEL} {ZERO_HASH[:12]} -> {ARTIFACT[:12]})")
    assert exempt.passed


@pytest.mark.parametrize(
    ("held", "id"),
    [(None, "no model on the record"), (SAME_MODEL, "the same model"), (UNRECORDED, "no digest")],
    ids=["unknown", "same_model", "unrecorded_digest"],
)
def test_a_full_rotation_breaches_unless_the_change_is_on_the_record(held, id):
    """Fail closed: the exemption is positively evidenced or it does not apply. A book bought
    by the same model, a book no filled basket accounts for, and a model whose digest is
    missing all stay on the flat limit."""
    rotation = {c.name: c for c in evaluate(replace(ROTATION, held=held))}["turnover"]

    assert rotation.measured == "100.00%"
    assert rotation.threshold == "< 60%", id
    assert not rotation.passed, id


def test_a_changed_model_does_not_admit_selling_more_than_the_book_is_worth():
    """A plan cannot sell more than the book holds, so anything above a full rotation is
    arithmetic that does not add up, which is exactly what the check is for."""
    phantom = replace(ROTATION, held=OTHER_RANKER, book={"H01": Decimal(300)})

    turnover = {c.name: c for c in evaluate(phantom)}["turnover"]

    assert turnover.measured == "1000.00%"
    assert not turnover.passed


def test_a_changed_model_leaves_the_flat_limit_in_place_below_a_full_rotation():
    """The 70% fixture passes on a changed model because a full rotation does, not because the
    limit moved: the same basket under the same model still breaches."""
    changed = {c.name: c for c in evaluate(replace(TURNOVER, held=OTHER_RANKER))}["turnover"]
    unchanged = {c.name: c for c in evaluate(replace(TURNOVER, held=SAME_MODEL))}["turnover"]

    assert changed.measured == unchanged.measured == "70.00%"
    assert changed.passed
    assert not unchanged.passed


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


# Ten names held at $4,800 and one, H01, that has grown to $12,000, 12% of equity, and left the
# ranking: the rebalance sells it whole and buys the ten names the book lacks.
OVERSIZED = Case(
    replace(
        BASE,
        orders=(sell("H01", qty=Decimal(600), notional=Decimal(12000)),)
        + tuple(buy(s) for s in NAMES[10:]),
    ),
    members=MEMBERS | {"H01"},
    marks=MARKS | {"H01": Decimal(20)},
    book={"H01": Decimal(600)} | {s: Decimal(240) for s in NAMES[:10]},
)


def test_a_sell_of_a_holding_past_the_cap_passes_the_gate():
    """The cap bounds the exposure a buy adds. A sell takes exposure off, so a held name that
    has grown past 8% and dropped out of the ranking is sold whole rather than halting the
    rebalance every month until a human steps in."""
    checks = evaluate(OVERSIZED)
    [notional] = [c for c in checks if c.name == "notional" and c.symbol == "H01"]

    assert (notional.measured, notional.threshold) == ("12.00% of 100000.00", "none on a sell")
    assert notional.passed
    assert [c for c in checks if not c.passed] == []


def test_a_buy_past_the_cap_still_fails_the_gate():
    sold, _, *bought = OVERSIZED.target.orders
    orders = (sold, buy("N11", qty=Decimal(600), notional=Decimal(12000)), *bought)
    case = replace(OVERSIZED, target=replace(OVERSIZED.target, orders=orders))
    failed = [(c.name, c.symbol, c.measured, c.threshold) for c in evaluate(case) if not c.passed]

    assert failed == [("notional", "N11", "12.00% of 100000.00", "<= 8%")]


# A forced corporate event, recorded (agb.corporate): the halt whose incumbent the rebalance
# carried, and the cash acquisition that terminated a position and left the basket a name short.
HALTED = corporate.Event(
    PERIOD, "N20", corporate.Kind.HALT, SIGNAL, Decimal(240), "exchange notice, still suspended"
)
ACQUIRED = corporate.Event(
    PERIOD,
    "N20",
    corporate.Kind.CASH_ACQUISITION,
    SIGNAL,
    Decimal(240),
    "deal closed at $20",
    price_usd=Decimal(20),
)
# Twenty names, one of them carried through a halt with no order of its own.
CARRIED = Case(
    replace(
        BASE,
        targets=targets(NAMES[:19]) + (gate.Target("N20", Decimal("0.05"), Decimal(240), True),),
        orders=tuple(buy(s) for s in NAMES[:19]),
        forced=(HALTED,),
    ),
    book={"N20": Decimal(240)},
)
# Nineteen names, the twentieth terminated by the acquisition: its weight stays in cash.
SHORT = Case(
    replace(
        BASE,
        targets=targets(NAMES[:19]),
        orders=tuple(buy(s) for s in NAMES[:19]),
        forced=(ACQUIRED,),
    )
)


def test_a_carried_name_the_record_explains_keeps_the_basket_at_twenty():
    checks = {c.name: c for c in evaluate(CARRIED) if c.symbol is None}

    assert [c.name for c in evaluate(CARRIED) if not c.passed] == []
    assert checks["positions"].measured == "20"


def test_a_carried_name_with_nothing_on_the_record_halts_the_rebalance():
    """The judgement is a human's: a name that has quietly stopped printing looks exactly
    like a halted one, so an unexplained carry fails rather than being taken on trust."""
    case = replace(CARRIED, target=replace(CARRIED.target, forced=()))

    failed = [(c.name, c.measured) for c in evaluate(case) if not c.passed]

    assert failed == [("positions", "20, carrying N20 with nothing on the record")]


def test_a_short_basket_the_record_explains_passes_and_leaves_the_slot_in_cash():
    checks = {c.name: c for c in evaluate(SHORT) if c.symbol is None}

    assert [c.name for c in evaluate(SHORT) if not c.passed] == []
    assert (checks["positions"].measured, checks["weight_sum"].measured) == (
        "19, short 1 against 1 on the record",
        "0.9120",
    )
    assert checks["weight_sum"].threshold == "0.7600 to 1"
    assert checks["cash"].measured == "8800.00"


def test_a_basket_shorter_than_the_record_accounts_for_fails():
    case = replace(
        SHORT,
        target=replace(
            SHORT.target,
            targets=targets(NAMES[:18]),
            orders=tuple(buy(s) for s in NAMES[:18]),
        ),
    )

    failed = [(c.name, c.measured) for c in evaluate(case) if not c.passed]

    assert failed == [("positions", "18, short 2 against 1 on the record")]


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


# A basket that names a panel run but records none of its blend: the panel's name is on it and
# nothing it scored is.
BLENDLESS = gate.Panel(1, Decimal("0.2"), 0, 0, None)


def test_a_panel_basket_with_no_blend_rows_fails_with_a_named_cause():
    """Coverage of none of nothing would read as complete, so a missing blend is not measured
    for coverage or spread: it fails on its own check, which names the run it is missing."""
    checks = evaluate(Case(replace(BASE, panel=BLENDLESS)))

    assert [
        (c.level, c.name, c.measured, c.threshold, c.passed)
        for c in checks
        if c.name.startswith("panel")
    ] == [
        (
            "provenance",
            "panel_blend",
            "no blend rows for panel run 1",
            "a blend row for every shortlisted name",
            False,
        )
    ]
    assert {c.name for c in checks if not c.passed} == {"panel_blend"}


# The gate in the database.


def rows(db, query, *params):
    return db.connect("cli").execute(query, params).fetchall()


def insert(db, target=BASE, reference=None, proposed=True, panel_run=(None, None, None)):
    """Write `target` as the pipeline does, and propose it unless told otherwise. A basket with
    a model digest names the stand-in model's certification, which this records. `panel_run`
    is the panel configuration, run and weight it was blended with, none by default."""
    certification_id = registry_fake.certify(db) if target.model_sha256 else None
    conn = db.connect("pipeline")
    reference = reference or target.period
    with conn.transaction():
        [(basket_id,)] = conn.execute(
            "INSERT INTO agb.basket (reference, period, session_date, signal_date, snapshot_date,"
            " partition_hash, universe_size, ranker, model_certification_id, model_sha256,"
            " cost_model_version, capital_usd, actor, panel_config_id, panel_run_id, panel_w)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '1', %s, 'pipeline', %s, %s, %s)"
            " RETURNING id",
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
                *panel_run,
            ),
        ).fetchall()
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO agb.basket_target (basket_id, symbol, weight, shares, held,"
                " carried) VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (basket_id, t.symbol, t.weight, t.shares, t.carried, t.carried)
                    for t in target.targets
                ],
            )
            cur.executemany(
                "INSERT INTO agb.basket_order (basket_id, client_order_id, symbol, side, qty,"
                " reference_price, notional_usd, cost_model_version, floor_bps, impact_bps,"
                " fees_usd, modelled_cost_bps)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, '1', 0, 0, 0, %s)",
                [
                    (basket_id, f"{reference}-{o.symbol}", o.symbol, o.side, o.qty,
                     o.notional / o.qty, o.notional, o.cost / o.notional * 10000)
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


# Who each human surface decides as: the database binds the bot to the Telegram operator who
# answered and the UI to its operator (the actor_binding migration).
SPEAKS_FOR = {"bot": "telegram:7", "ui": "ui:operator"}


def actor_for(role, to_state):
    """The actor `role` writes a move to `to_state` as, when a test does not say: the bot puts
    a basket to the human as itself, a human surface decides as its operator, and a service
    writes as its own name."""
    if role == "bot" and to_state == "pending":
        return "bot"
    return SPEAKS_FOR.get(role, role)


def move(db, basket_id, seq, from_state, to_state, role, surface=None, actor=None):
    db.connect(role).execute(
        "INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface)"
        " VALUES (%s, %s, %s, %s, %s, %s)",
        (basket_id, seq, from_state, to_state, actor or actor_for(role, to_state), surface or role),
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
    snapshot(db, positions)
    return basket_id


def snapshot(db, positions, snapshot_date=date(2026, 8, 31)):
    """The broker's position read after that basket's close, which is the book the gate
    measures against: the targets above say what was planned, not what filled."""
    conn = db.connect("execution")
    [(snapshot_id,)] = conn.execute(
        "INSERT INTO agb.position_snapshot (snapshot_date, positions, actor)"
        " VALUES (%s, %s, 'execution') RETURNING id",
        (snapshot_date, len(positions)),
    ).fetchall()
    for symbol, shares in positions.items():
        conn.execute(
            "INSERT INTO agb.position (snapshot_id, symbol, qty) VALUES (%s, %s, %s)",
            (snapshot_id, symbol, shares),
        )
    return snapshot_id


def run(db, basket_id, case=SOUND):
    """Gate the basket, with the stand-in model promoted (and voided) as `case` says, and the
    account read at its signal session's close as execution would have read it."""
    [(session_date,)] = rows(db, "SELECT session_date FROM agb.basket WHERE id = %s", basket_id)
    read_book(db, basket.signal_session(session_date))
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
        (basket_id, 109, "paper", "pipeline")
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


def test_the_gate_measures_against_the_broker_read_and_not_the_declared_targets(db):
    """The 2026-11 halt: last month's basket reached filled with most of its orders expired,
    so its targets claim a book the account does not have. A sell of a name that was only ever
    a target closes nothing, and the gate sees that because it reads the broker, not the plan.
    """
    hold(db, TURNOVER.book)
    # The next read of the account: of the ten the basket declared, one order filled.
    snapshot(db, {"H01": Decimal(300)}, date(2026, 9, 30))
    basket_id = insert(db, TURNOVER.target)

    verdict = run(db, basket_id, TURNOVER)

    assert verdict.state == "halted"
    failed = {(c.name, c.symbol) for c in verdict.failed}
    # Every sell is of a name the account does not hold, and the turnover is measured against
    # the one position it does.
    assert {s for n, s in failed if n == "side"} == set(HELD[3:])
    [(measured,)] = rows(db, "SELECT measured FROM agb.gate_check WHERE name = 'turnover'")
    assert measured == "700.00%"


def test_a_book_read_before_the_signal_session_is_not_gated_against(db):
    """Execution reads the account at every close, so a latest read three days before the
    signal session means it has stopped reading. The gate waits rather than measuring turnover
    and the sells against positions that may since have moved."""
    snapshot(db, {"H01": Decimal(300)}, SIGNAL - timedelta(days=3))
    basket_id = insert(db)
    registry_fake.promote(db)

    with pytest.raises(basket.NotReady, match="2026-09-27, before the signal session 2026-09-30"):
        gate.run(
            db.connect("pipeline"), basket_id, members=SOUND.members, marks=SOUND.marks,
            now=SOUND.now,
        )  # fmt: skip

    assert rows(db, "SELECT count(*) FROM agb.gate_run") == [(0,)]
    assert state(db, basket_id)[0] == "proposed"


def fill(db, basket_id, symbol):
    """One of that basket's orders, taken by the broker and executed. A fill is the only
    evidence that the model which ranked the basket is in the book."""
    conn = db.connect("execution")
    [(basket_order_id, client_order_id, side, qty)] = conn.execute(
        "SELECT id, client_order_id, side, qty FROM agb.basket_order"
        " WHERE basket_id = %s AND symbol = %s",
        (basket_id, symbol),
    ).fetchall()
    [(order_id,)] = conn.execute(
        "INSERT INTO agb.broker_order (basket_id, basket_order_id, client_order_id, symbol,"
        " side, qty, cost_model_version, modelled_cost_bps, nbbo_bid, nbbo_ask, nbbo_mid,"
        " nbbo_at, actor) VALUES (%s, %s, %s, %s, %s, %s, '1', 0, 19.99, 20.01, 20, now(),"
        " 'execution') RETURNING id",
        (basket_id, basket_order_id, client_order_id, symbol, side, qty),
    ).fetchall()
    conn.execute(
        "INSERT INTO agb.fill (order_id, execution_id, qty, price, filled_at,"
        " fee_model_version, actor) VALUES (%s, %s, %s, 20, now(), '1', 'execution')",
        (order_id, f"x{order_id}", qty),
    )
    return order_id


def bought(db, positions, ranker, model_sha256=None, filled=True, period="2026-09"):
    """A live basket that reached `filled`, holding `positions`, ranked by `ranker`. With
    `filled` false its orders all expired unfilled, which is a state the basket reaches
    anyway: the session being over is not the same as having bought anything."""
    basket_id = insert(
        db,
        replace(
            BASE,
            period=period,
            session_date=date(2026, 9, 1),
            snapshot_date=date(2026, 8, 31),
            ranker=ranker,
            model_sha256=model_sha256,
            targets=tuple(gate.Target(s, Decimal("0.05"), n) for s, n in positions.items()),
            orders=tuple(buy(s, qty=n, notional=n * 20) for s, n in positions.items()),
        ),
    )
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
    move(db, basket_id, 3, "pending", "approved", "bot", "telegram")
    move(db, basket_id, 4, "approved", "submitted", "execution")
    move(db, basket_id, 5, "submitted", "filled", "execution")
    if filled:
        fill(db, basket_id, next(iter(positions)))
    return basket_id


def test_a_full_rotation_passes_the_gate_when_the_book_was_bought_by_another_model(db):
    """The 2026-11 case: last month's book came from a ranker this month's basket does not
    use, so there is no rank buffer between the two and the plan sells everything. The gate
    reads the change off the record and records what admitted the rotation."""
    bought(db, ROTATION.book, "placeholder-momentum-252-21")
    snapshot(db, ROTATION.book)
    basket_id = insert(db, ROTATION.target)

    verdict = run(db, basket_id, ROTATION)

    assert verdict.passed
    assert verdict.state == "approved"
    [(measured, threshold)] = rows(
        db, "SELECT measured, threshold FROM agb.gate_check WHERE name = 'turnover'"
    )
    assert measured == "100.00%"
    assert threshold == (
        f"< 60%, or <= 100% on a changed model (placeholder-momentum-252-21 -> {MODEL})"
    )


def test_the_model_behind_the_book_is_the_last_basket_that_filled_not_the_last_one_closed(db):
    """A basket reaches `filled` when its session is over, whether or not a single order
    executed. The model behind the book is the one that actually bought it, so a basket whose
    orders all expired unfilled is not the anchor even though it closed later."""
    bought(db, ROTATION.book, "placeholder-momentum-252-21")
    bought(db, ROTATION.book, MODEL, model_sha256=ARTIFACT, filled=False, period="2026-10")
    snapshot(db, ROTATION.book)
    basket_id = insert(db, ROTATION.target, reference="2026-11")

    verdict = run(db, basket_id, ROTATION)

    assert verdict.passed
    [(threshold,)] = rows(db, "SELECT threshold FROM agb.gate_check WHERE name = 'turnover'")
    assert "placeholder-momentum-252-21" in threshold


def test_a_full_rotation_halts_the_month_when_the_same_model_bought_the_book(db):
    """The check the limit was written for: nothing on the record explains selling the whole
    book, so the month halts."""
    bought(db, ROTATION.book, MODEL, model_sha256=ARTIFACT)
    snapshot(db, ROTATION.book)
    basket_id = insert(db, ROTATION.target)

    verdict = run(db, basket_id, ROTATION)

    assert verdict.state == "halted"
    assert {c.name for c in verdict.failed} == {"turnover"}
    [(threshold,)] = rows(db, "SELECT threshold FROM agb.gate_check WHERE name = 'turnover'")
    assert threshold == "< 60%"


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


# A name the pipeline might add to a basket's blend: one the panel did not score.
LATE_BLEND = (
    "INSERT INTO agb.basket_blend (basket_id, symbol, stage1_rank, stage1_rank_pct,"
    " llm_score_pct, neutral, final_score) VALUES (%s, 'X', 99, 1, 0.5, true, 0)"
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
        LATE_BLEND,
    ):
        with pytest.raises(errors.CheckViolation, match="content is frozen once the gate has run"):
            pipeline.execute(statement, (basket_id,))

    [(run_id,)] = rows(db, "SELECT id FROM agb.gate_run")
    with pytest.raises(errors.CheckViolation, match="its 109 checks are recorded"):
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
    assert rows(db, "SELECT count(*) FROM agb.gate_check") == [(109,)]
    assert rows(db, "SELECT count(*) FROM agb.basket_target WHERE basket_id = %s", basket_id) == [
        (20,)
    ]


def blended(db):
    """A panel configuration and a run of it as the pipeline records them, for a basket to
    name. The run scored nothing: what the basket records of it is up to the test."""
    config_id = panel.create_config(db.connect("cli"), "operator", shortlist_n=25, w=0.2)
    [(run_id,)] = (
        db.connect("pipeline")
        .execute(
            "INSERT INTO agb.panel_run (panel_config_id, as_of, actor)"
            " VALUES (%s, %s, 'pipeline') RETURNING id",
            (config_id, BASE.session_date),
        )
        .fetchall()
    )
    return config_id, run_id, Decimal("0.2")


def test_a_panel_basket_with_no_blend_rows_halts_on_the_missing_blend(db):
    """The panel's name on a basket with none of its blend: before, the join found no rows and
    read the basket as one no panel touched, so neither panel check ran and it passed."""
    panel_run = blended(db)
    basket_id = insert(db, panel_run=panel_run)

    assert gate.load(db.connect("pipeline"), basket_id).panel == gate.Panel(
        panel_run[1], Decimal("0.2"), 0, 0, None
    )
    verdict = run(db, basket_id)

    assert (verdict.state, {c.name for c in verdict.failed}) == ("halted", {"panel_blend"})
    assert rows(db, "SELECT name, measured FROM agb.gate_check WHERE NOT passed") == [
        ("panel_blend", f"no blend rows for panel run {panel_run[1]}")
    ]
    [(cause, reason)] = rows(db, "SELECT cause, reason FROM agb.halt_open")
    assert cause == "gate_breach"
    assert reason.startswith("pre-trade gate failed: panel_blend")


@pytest.mark.parametrize("role", SERVICE_ROLES)
def test_no_service_role_may_edit_or_delete_a_basket_blend(db, role):
    basket_id = insert(db)
    db.connect("pipeline").execute(LATE_BLEND, (basket_id,))
    conn = db.connect(role)

    for statement in (
        "UPDATE agb.basket_blend SET final_score = 1",
        "DELETE FROM agb.basket_blend",
    ):
        with pytest.raises(errors.InsufficientPrivilege):
            conn.execute(statement)


def test_the_owner_may_not_edit_or_delete_a_basket_blend_either(db):
    basket_id = insert(db)
    db.connect("pipeline").execute(LATE_BLEND, (basket_id,))
    owner = db.connect(OWNER)

    for op, statement in (
        ("UPDATE", "UPDATE agb.basket_blend SET final_score = 1"),
        ("DELETE", "DELETE FROM agb.basket_blend"),
    ):
        with pytest.raises(errors.RaiseException, match=f"append-only: {op} refused"):
            owner.execute(statement)
    assert rows(db, "SELECT symbol, final_score FROM agb.basket_blend") == [("X", 0.0)]


def test_a_second_run_reads_the_recorded_verdict_back(db):
    basket_id = insert(db)
    first = run(db, basket_id)

    # A clock that would now fail every order: the recorded verdict stands.
    again = run(db, basket_id, Case(now=LATE))

    assert (again.recorded, again.state, again.checks) == (True, "approved", first.checks)
    assert rows(db, "SELECT count(*) FROM agb.gate_run") == [(1,)]
    assert rows(db, "SELECT count(*) FROM agb.gate_check") == [(109,)]
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
    assert state(db, basket_id) == ("approved", 3, "telegram:7", "telegram")


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


def file_event(db, event):
    """A forced event on the record, as `agb corporate record` writes it."""
    source = approval.Source("ada", "cli", f"agb corporate record {event.kind} {event.symbol}")
    return corporate.record(db.connect("cli"), event, source)


def test_the_gate_reads_the_record_behind_a_short_basket(db):
    """Acceptance: the gate accepts nineteen names when a recorded forced event explains the
    missing one. The record is read from the database, not from the proposal."""
    file_event(db, ACQUIRED)
    basket_id = insert(db, SHORT.target)

    verdict = run(db, basket_id, SHORT)

    assert (verdict.state, verdict.passed) == ("approved", True)
    assert rows(
        db,
        "SELECT measured, threshold FROM agb.gate_check c JOIN agb.gate_run r ON r.id = c.run_id"
        " WHERE r.basket_id = %s AND c.name = 'positions'",
        basket_id,
    ) == [
        (
            "19, short 1 against 1 on the record",
            "20, or fewer where a recorded forced event explains each one",
        )
    ]


def test_a_short_basket_with_nothing_on_the_record_halts_in_the_database(db):
    basket_id = insert(db, SHORT.target)

    verdict = run(db, basket_id, SHORT)

    assert (verdict.state, verdict.passed) == ("halted", False)
    [(cause, halted, reason)] = rows(db, "SELECT cause, basket_id, reason FROM agb.halt_open")
    assert (cause, halted) == ("gate_breach", basket_id)
    assert reason.startswith("pre-trade gate failed: positions")


def test_a_carried_name_passes_the_gate_once_its_halt_is_on_the_record(db):
    hold(db, {"N20": Decimal(240)})
    file_event(db, HALTED)
    basket_id = insert(db, CARRIED.target)

    verdict = run(db, basket_id, CARRIED)

    assert (verdict.state, verdict.passed) == ("approved", True)
