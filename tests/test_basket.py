"""The monthly rebalance end to end: the pipeline turns the scheduler's `rebalance` job into a
proposed basket with its ranks, targets, order plan, provenance and first event, which the
pre-trade gate then approves in paper mode (tests/test_gate.py covers the gate itself). And the
basket's state machine: the CAS on `basket_event`, the transition trigger and the grants."""

import os
import re
import threading
import time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
import registry_fake
from market_fake import PERIOD, SIGNAL, TRADE, Market
from psycopg import errors
from registry_fake import ARTIFACT, MODEL_ID

from agb import approval, basket, corporate, gate, jobs, lake, schedule
from agb.corporate import Event, Kind
from agb.rebalance import SIZE
from agb.schedule import Expected, Planned
from agb.services.pipeline import RETRY, Pipeline, capital_from_env

MINUTE = timedelta(minutes=1)
CAPITAL = Decimal("100000")
# The fake market is small, so the gate's universe band is widened to fit it.
LIMITS = gate.Limits(universe_min=20)
# The evening of the signal session, well before the trade session's MOC cutoff.
EVENING = datetime(2026, 9, 30, 22, 0, tzinfo=UTC)
ZERO_HASH = "0" * 64


def plan_rebalance(db, period=PERIOD, session_date=TRADE):
    start = datetime.now(UTC) - timedelta(hours=1)
    due = start + timedelta(hours=15)
    job = Planned(
        schedule.REBALANCE,
        period,
        session_date,
        start,
        (Expected(schedule.PROPOSAL, period, due, True),),
    )
    schedule.write(db.connect("scheduler"), [job])
    return due


def run(db, root, **kwargs):
    """One pipeline cycle, ranking with the promoted stand-in model (tests/registry_fake.py)."""
    kwargs = {"limits": LIMITS, "clock": lambda: EVENING, **kwargs}
    ranker = registry_fake.ranker(db)
    Pipeline(root, "", ranker=ranker, capital=CAPITAL, **kwargs)(db.connect("pipeline"))


def rows(db, query, *params):
    return db.connect("cli").execute(query, params).fetchall()


def job_state(db):
    return rows(db, "SELECT state FROM agb.job_state WHERE kind = 'rebalance'")[0][0]


def late_proposals(db, at):
    return rows(db, "SELECT period FROM agb.overdue_at(%s) WHERE kind = 'proposal'", at)


def file_event(db, symbol, kind, shares, **extra):
    """A forced corporate event on the record, as `agb corporate record` writes it."""
    event = Event(PERIOD, symbol, kind, SIGNAL, Decimal(shares), "broker statement", **extra)
    source = approval.Source("ada", "cli", f"agb corporate record {kind} {symbol}")
    corporate.record(db.connect("cli"), event, source)
    return event


def gate_check(db, name):
    [(measured, passed)] = rows(
        db,
        "SELECT measured, passed FROM agb.gate_check c JOIN agb.gate_run r ON r.id = c.run_id"
        f" WHERE r.basket_id = {LATEST} AND c.name = %s AND c.symbol IS NULL",
        name,
    )
    return measured, passed


LATEST = "(SELECT max(id) FROM agb.basket)"


def targets(db):
    return rows(
        db,
        "SELECT symbol, weight, shares, held, carried, rank FROM agb.basket_target"
        f" WHERE basket_id = {LATEST} ORDER BY symbol",
    )


def orders(db):
    return rows(
        db,
        "SELECT symbol, side, qty, client_order_id, reference_price, notional_usd,"
        " cost_model_version, floor_bps, impact_bps, fees_usd, modelled_cost_bps"
        f" FROM agb.basket_order WHERE basket_id = {LATEST} ORDER BY side, symbol",
    )


# Proposing.


def test_the_rebalance_job_proposes_a_twenty_name_equal_weight_basket(db, tmp_path):
    due = plan_rebalance(db)
    root = Market(tmp_path).names(25).land()

    run(db, root)

    assert job_state(db) == "completed"
    assert late_proposals(db, due + MINUTE) == []
    [(job_id,)] = rows(db, "SELECT id FROM agb.job")
    [row] = rows(
        db,
        "SELECT kind, reference, period, job_id, session_date, signal_date, snapshot_date,"
        " partition_hash, universe_size, ranker, model_certification_id, model_sha256,"
        " cost_model_version, capital_usd, actor FROM agb.basket",
    )
    certification_id = registry_fake.certify(db)
    assert row == (
        "live", "2026-10", "2026-10", job_id, TRADE, SIGNAL, SIGNAL,
        lake.partition_digest(root, SIGNAL), 25, MODEL_ID, certification_id, ARTIFACT, "1",
        CAPITAL, "pipeline",
    )  # fmt: skip
    assert rows(db, "SELECT seq, to_state, actor, surface FROM agb.basket_event ORDER BY seq") == [
        (1, "proposed", "pipeline", "pipeline"),
        (2, "approved", "system", "pipeline"),  # the gate passed it, in paper mode
    ]

    ranked = rows(db, "SELECT symbol, score FROM agb.basket_rank ORDER BY rank")
    assert [s for s, _ in ranked] == [f"N{i:02d}" for i in range(1, 26)]
    assert all(score is not None for _, score in ranked)

    held = targets(db)
    assert [t[0] for t in held] == [f"N{i:02d}" for i in range(1, SIZE + 1)]
    assert {t[1:] for t in held} == {(Decimal("0.05"), 250, False, False, None)} - {None} or all(
        (weight, shares, was_held, carried) == (Decimal("0.05"), 250, False, False)
        and rank == int(symbol[1:])
        for symbol, weight, shares, was_held, carried, rank in held
    )

    plan = orders(db)
    assert [(o[0], o[1], o[2], o[3]) for o in plan] == [
        (f"N{i:02d}", "buy", 250, f"2026-10-N{i:02d}") for i in range(1, SIZE + 1)
    ]
    for _, _, qty, _, price, notional, version, floor, impact, fees, total in plan:
        assert price == 20 and notional == qty * price and version == "1"
        assert floor > 0 and impact >= 0 and fees == 0  # a buy pays no regulatory fees
        assert total == floor + impact


def test_the_basket_is_the_book_and_the_order_plan_moves_it(db, tmp_path):
    """From a filled book of N01, N04 (rank 22) and N30 (rank 25), both inside the buffer, OLD
    (ranked below the buffer) and HALT, which has no print on the signal session, the plan keeps
    the three, carries HALT, sells OLD and buys the sixteen open slots from the top."""
    market = Market(tmp_path).names(40)
    for ticker, growth in (
        ("N04", 0.01 - 22 * 0.0002 - 0.00001),
        ("N30", 0.01 - 24 * 0.0002 - 0.00001),
    ):
        for table, column in (("sep", 0), ("sf1", 0), ("tickers", 2)):
            market.rows[table] = [r for r in market.rows[table] if r[column] != ticker]
        market.name(ticker, growth=growth)
    root = market.name("OLD", growth=-0.01).name("HALT", halted={SIGNAL}, price=50).land()
    book = {"N01": 100, "N04": 200, "N30": 300, "OLD": 400, "HALT": 50}
    filled_basket(db, "2026-09", book)
    file_event(db, "HALT", Kind.HALT, 50)  # the carry is only legitimate on the record
    plan_rebalance(db)

    run(db, root)

    assert job_state(db) == "completed"
    held = targets(db)
    assert len(held) == SIZE
    by_symbol = {t[0]: t[1:] for t in held}
    assert by_symbol["N01"] == (Decimal("0.05"), 100, True, False, 1)
    assert by_symbol["N04"] == (Decimal("0.05"), 200, True, False, 22)
    assert by_symbol["N30"] == (Decimal("0.05"), 300, True, False, 25)
    assert by_symbol["HALT"] == (Decimal("0.05"), 50, True, True, None)
    assert "OLD" not in by_symbol
    bought = sorted(s for s, (_, _, was_held, _, _) in by_symbol.items() if not was_held)
    assert bought == [f"N{i:02d}" for i in range(2, 19) if i != 4]
    plan = orders(db)
    assert [(o[0], o[1], o[2]) for o in plan] == [
        *((f"N{i:02d}", "buy", 250) for i in range(2, 19) if i != 4),
        ("OLD", "sell", 400),
    ]
    assert plan[-1][9] > 0  # a sell pays the regulatory fees
    assert rows(db, "SELECT reference, state FROM agb.basket_state ORDER BY id") == [
        ("2026-09", "filled"),
        ("2026-10", "approved"),
    ]
    # Selling OLD turns over 8,000 of the book's 22,500, with HALT marked at its last close.
    [(turnover,)] = rows(db, "SELECT measured FROM agb.gate_check WHERE name = 'turnover'")
    assert turnover == "35.56%"
    # The book is the broker's latest snapshot, so the proposal has not moved it yet.
    assert basket.book(db.connect("pipeline")) == book


# The four forced corporate events, down the live path. Each is recorded against the period
# first, and the pipeline then rebalances through the same `rebalance.select` the backtest
# calls (tests/test_backtest.py runs the same four through a simulation).


def test_a_cash_acquisition_terminates_the_position_and_its_cash_waits_for_the_rebalance(
    db, tmp_path
):
    """GONE was taken out for cash mid-month. The position is gone from the book before
    anything is ranked, no sell of ours stands behind it, and its weight is spent at the
    rebalance like any other cash rather than mid-month."""
    root = Market(tmp_path).names(25).land()
    filled_basket(db, "2026-09", {"N01": 100, "GONE": 400})
    file_event(db, "GONE", Kind.CASH_ACQUISITION, 400, price_usd=Decimal("31.50"))
    plan_rebalance(db)

    run(db, root)

    assert job_state(db) == "completed"
    by_symbol = {t[0]: t[1:] for t in targets(db)}
    assert len(by_symbol) == SIZE and "GONE" not in by_symbol
    # Nineteen buys fill the slots N01 did not already hold, and nothing sells GONE.
    assert [(o[0], o[1], o[2]) for o in orders(db)] == [
        (f"N{i:02d}", "buy", 250) for i in range(2, SIZE + 1)
    ]
    assert gate_check(db, "positions") == ("20", True)


def test_a_stock_merger_sells_the_acquirer_shares_whatever_their_rank(db, tmp_path):
    """OLDCO was bought with ACQ stock, 200 shares for its 400. ACQ ranks first in the
    market, and is sold anyway: that position was never chosen."""
    root = Market(tmp_path).names(25).name("ACQ", growth=0.012).land()
    filled_basket(db, "2026-09", {"N01": 100, "OLDCO": 400})
    file_event(
        db,
        "OLDCO",
        Kind.STOCK_MERGER,
        400,
        into_symbol="ACQ",
        into_shares=Decimal(200),
    )
    plan_rebalance(db)

    run(db, root)

    assert job_state(db) == "completed"
    by_symbol = {t[0]: t[1:] for t in targets(db)}
    assert len(by_symbol) == SIZE
    assert "ACQ" not in by_symbol and "OLDCO" not in by_symbol
    assert ("ACQ", "sell", 200) in [(o[0], o[1], o[2]) for o in orders(db)]
    assert gate_check(db, "positions") == ("20", True)


def test_a_delisting_exits_at_the_last_listed_print_rather_than_being_dropped(db, tmp_path):
    """DARK left the listed market and has no print on the signal session. The exit is
    modelled at the price the operator recorded, not quietly forgotten."""
    root = Market(tmp_path).names(25).name("DARK", growth=0.005, halted={SIGNAL}).land()
    filled_basket(db, "2026-09", {"N01": 100, "DARK": 400})
    file_event(db, "DARK", Kind.DELISTING, 400, price_usd=Decimal("12.25"))
    plan_rebalance(db)

    run(db, root)

    assert job_state(db) == "completed"
    assert "DARK" not in {t[0] for t in targets(db)}
    [sell] = [o for o in orders(db) if o[1] == "sell"]
    assert (sell[0], sell[2], sell[4], sell[5]) == ("DARK", 400, Decimal("12.25"), Decimal("4900"))
    assert gate_check(db, "positions") == ("20", True)


def test_a_halt_across_the_rebalance_carries_the_position_and_skips_its_replacement(db, tmp_path):
    """STUCK is suspended across the signal session. It keeps its slot with no order of its
    own, its replacement is skipped, so nineteen names trade, and the basket still holds
    twenty because the record explains the carry."""
    root = Market(tmp_path).names(25).name("STUCK", growth=0.011, halted={SIGNAL}).land()
    filled_basket(db, "2026-09", {"STUCK": 50})
    file_event(db, "STUCK", Kind.HALT, 50)
    plan_rebalance(db)

    run(db, root)

    assert job_state(db) == "completed"
    by_symbol = {t[0]: t[1:] for t in targets(db)}
    assert len(by_symbol) == SIZE
    assert by_symbol["STUCK"] == (Decimal("0.05"), 50, True, True, None)
    plan = [(o[0], o[1], o[2]) for o in orders(db)]
    assert plan == [(f"N{i:02d}", "buy", 250) for i in range(1, SIZE)]
    assert gate_check(db, "positions") == ("20", True)
    assert gate_check(db, "weight_sum") == ("1.00", True)


def test_an_unrecorded_carry_halts_the_rebalance(db, tmp_path):
    """The same halt with nothing on the record. A name that has quietly stopped printing
    looks exactly like a suspended one, so the gate stops and asks for a human."""
    root = Market(tmp_path).names(25).name("STUCK", growth=0.011, halted={SIGNAL}).land()
    filled_basket(db, "2026-09", {"STUCK": 50})
    plan_rebalance(db)

    run(db, root)

    assert rows(db, "SELECT state FROM agb.basket_state WHERE reference = %s", PERIOD) == [
        ("halted",)
    ]
    assert gate_check(db, "positions") == ("20, carrying STUCK with nothing on the record", False)
    assert rows(db, "SELECT cause FROM agb.halt_open") == [("gate_breach",)]


def test_a_rebalance_that_fails_the_gate_is_halted_and_opens_the_latch(db, tmp_path):
    plan_rebalance(db)
    root = Market(tmp_path).names(25).land()
    cutoff = datetime(2026, 10, 1, 19, 45, tzinfo=UTC)  # 15:45 ET on the trade session

    run(db, root, clock=lambda: cutoff)

    # The job did its work: the basket is proposed and gated, and the gate halted it.
    assert job_state(db) == "completed"
    [(basket_id,)] = rows(db, "SELECT id FROM agb.basket")
    assert rows(db, "SELECT state, seq, actor, surface FROM agb.basket_state") == [
        ("halted", 2, "pipeline", "pipeline")
    ]
    assert rows(db, "SELECT DISTINCT name FROM agb.gate_check WHERE NOT passed") == [("clock",)]
    [(cause, halted, actor, reason)] = rows(
        db, "SELECT cause, basket_id, actor, reason FROM agb.halt_open"
    )
    assert (cause, halted, actor) == ("gate_breach", basket_id, "pipeline")
    assert "clock N01 measured 2026-10-01 15:45 ET, needs before 2026-10-01 15:45 ET" in reason


def test_a_proposal_already_made_is_kept_when_the_job_is_redone(db, tmp_path):
    plan_rebalance(db)
    root = Market(tmp_path).names(25).land()
    conn = db.connect("pipeline")
    claim = jobs.claim(conn, [schedule.REBALANCE], "pipeline", timedelta(hours=1))

    ranker = registry_fake.ranker(db)
    first = basket.propose(conn, claim, root, ranker, capital=CAPITAL)
    again = basket.propose(conn, claim, root, ranker, capital=CAPITAL)

    assert (again.basket_id, again.existing) == (first.basket_id, True)
    assert rows(db, "SELECT count(*) FROM agb.basket") == [(1,)]
    assert rows(db, "SELECT count(*) FROM agb.basket_event") == [(1,)]
    assert rows(db, "SELECT count(*) FROM agb.basket_target") == [(20,)]


def test_a_lease_that_expired_mid_proposal_is_reclaimed_and_completed(db, tmp_path):
    plan_rebalance(db)
    root = Market(tmp_path).names(25).land()
    conn = db.connect("pipeline")
    claim = jobs.claim(conn, [schedule.REBALANCE], "pipeline", timedelta(milliseconds=1))
    basket.propose(conn, claim, root, registry_fake.ranker(db), capital=CAPITAL)
    time.sleep(0.05)

    run(db, root)

    assert job_state(db) == "completed"
    assert rows(db, "SELECT count(*) FROM agb.basket") == [(1,)]


def test_a_lake_without_the_signal_session_keeps_the_job_and_tries_again(db, tmp_path):
    due = plan_rebalance(db)
    root = Market(tmp_path, end=date(2026, 9, 29)).names(25).land()

    run(db, root)

    assert job_state(db) == "claimed"
    [(left,)] = rows(db, "SELECT lease_until - now() FROM agb.job")
    assert timedelta(0) < left <= RETRY
    assert rows(db, "SELECT count(*) FROM agb.basket") == [(0,)]
    assert late_proposals(db, due + MINUTE) == [(PERIOD,)]


def test_a_name_whose_weight_buys_no_share_fails_the_job(db, tmp_path):
    plan_rebalance(db)
    root = Market(tmp_path).names(25).name("DEAR", growth=0.02, price=6000.0).land()

    run(db, root)

    assert job_state(db) == "failed"
    [(detail,)] = rows(db, "SELECT detail FROM agb.job_event WHERE event = 'failed'")
    assert "DEAR" in detail and "no whole share" in detail
    assert rows(db, "SELECT count(*) FROM agb.basket") == [(0,)]


def test_a_period_that_is_not_a_month_is_refused(db, tmp_path):
    plan_rebalance(db, period="2026-10-01")
    run(db, Market(tmp_path).names(25).land())
    assert job_state(db) == "failed"


def test_the_signal_session_is_the_last_session_before_the_trade_session():
    assert basket.signal_session(date(2026, 10, 1)) == date(2026, 9, 30)
    assert basket.signal_session(date(2026, 9, 8)) == date(2026, 9, 4)  # Labor Day between
    assert basket.signal_session(date(2026, 1, 2)) == date(2025, 12, 31)


def test_the_capital_comes_from_the_environment(monkeypatch):
    monkeypatch.delenv(basket.CAPITAL_ENV, raising=False)
    assert capital_from_env() == Decimal("100000")
    monkeypatch.setenv(basket.CAPITAL_ENV, "250000.50")
    assert capital_from_env() == Decimal("250000.50")
    monkeypatch.setenv(basket.CAPITAL_ENV, "lots")
    with pytest.raises(SystemExit, match="not a number"):
        capital_from_env()
    assert os.environ[basket.CAPITAL_ENV] == "lots"


# The state machine. A basket is inserted directly, as the pipeline does, so these need no lake.


def bare_basket(db, reference="2026-10"):
    """A basket row with no event yet: not even proposed."""
    [(basket_id,)] = (
        db.connect("pipeline")
        .execute(
            "INSERT INTO agb.basket (reference, period, session_date, signal_date, snapshot_date,"
            " partition_hash, universe_size, ranker, cost_model_version, capital_usd, actor)"
            " VALUES (%s, %s, %s, %s, %s, %s, 25, 'test', '1', 100000, 'pipeline') RETURNING id",
            (reference, reference[:7], TRADE, SIGNAL, SIGNAL, ZERO_HASH),
        )
        .fetchall()
    )
    return basket_id


def proposed_basket(db, reference="2026-10"):
    basket_id = bare_basket(db, reference)
    move(db, basket_id, 1, None, "proposed", "pipeline")
    return basket_id


def move(db, basket_id, seq, from_state, to_state, role, surface=None):
    db.connect(role).execute(
        "INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor, surface)"
        " VALUES (%s, %s, %s, %s, %s, %s)",
        (basket_id, seq, from_state, to_state, role, surface or role),
    )


def filled_basket(db, reference, positions, read=True):
    """A basket that went all the way to filled, holding `positions` (symbol: shares). The
    broker's snapshot is written too, because the book the rebalance reads is the broker's
    positions and not these targets: a basket can reach filled with orders that expired.
    With `read` false the account is never read, and the targets stand alone."""
    basket_id = proposed_basket(db, reference)
    conn = db.connect("pipeline")
    for symbol, shares in positions.items():
        conn.execute(
            "INSERT INTO agb.basket_target (basket_id, symbol, weight, shares, held, carried)"
            " VALUES (%s, %s, 0.05, %s, false, false)",
            (basket_id, symbol, shares),
        )
    move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
    move(db, basket_id, 3, "pending", "approved", "bot", "telegram")
    move(db, basket_id, 4, "approved", "submitted", "execution")
    move(db, basket_id, 5, "submitted", "filled", "execution")
    if read:
        snapshot(db, positions)
    return basket_id


def snapshot(db, positions, snapshot_date=None):
    """The broker's position read, as execution writes it after a close."""
    conn = db.connect("execution")
    [(last,)] = rows(db, "SELECT max(snapshot_date) FROM agb.position_snapshot")
    if snapshot_date is None:
        snapshot_date = last + timedelta(days=1) if last else date(2026, 9, 30)
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


def state(db, basket_id):
    return rows(db, "SELECT state, seq FROM agb.basket_state WHERE id = %s", basket_id)[0]


def test_a_basket_walks_from_proposed_to_filled_by_the_roles_allowed_each_step(db):
    basket_id = filled_basket(db, "2026-10", {"AAA": 1})
    assert state(db, basket_id) == ("filled", 5)
    assert rows(db, "SELECT seq, to_state, actor, surface FROM agb.basket_event ORDER BY seq") == [
        (1, "proposed", "pipeline", "pipeline"),
        (2, "pending", "bot", "telegram"),
        (3, "approved", "bot", "telegram"),
        (4, "submitted", "execution", "execution"),
        (5, "filled", "execution", "execution"),
    ]


def test_the_book_is_what_the_broker_holds_and_not_what_the_basket_declared(db):
    """A basket reaches filled when the session is over, whatever its orders did. The orders
    that expired leave their targets standing, so the targets of a filled basket are a plan
    and not a holding. The rebalance starts from the broker's own read."""
    filled_basket(db, "2026-09", {"AAA": 10, "BBB": 20})
    # BBB's MOC order expired unfilled, so the next read of the account holds AAA alone.
    snapshot(db, {"AAA": 10})

    assert basket.book(db.connect("pipeline")) == {"AAA": Decimal(10)}


def test_a_book_is_empty_until_the_broker_has_been_read(db):
    """Targets on their own are not a book. Until execution has read the account there is
    nothing to start a rebalance from, whatever a basket says it bought."""
    filled_basket(db, "2026-09", {"AAA": 10}, read=False)

    assert basket.book(db.connect("pipeline")) == {}


def test_an_account_that_holds_nothing_reads_as_an_empty_book(db):
    """The snapshot row says the read happened. No positions against it is an account holding
    nothing, which is a book, not a missing one."""
    snapshot(db, {})

    assert basket.book(db.connect("pipeline")) == {}


def test_two_transitions_from_the_same_state_let_exactly_one_through(db):
    """Two bots race to put the same proposal to the human. The second insert waits on the
    first's uncommitted row, then fails the unique index when the first commits."""
    basket_id = proposed_basket(db)
    first_in, release = threading.Event(), threading.Event()
    outcome = {}

    def first():
        conn = db.connect("bot")
        with conn.transaction():
            conn.execute(
                "INSERT INTO agb.basket_event (basket_id, seq, from_state, to_state, actor,"
                " surface) VALUES (%s, 2, 'proposed', 'pending', 'bot', 'telegram')",
                (basket_id,),
            )
            first_in.set()
            release.wait(5)

    def second():
        first_in.wait(5)
        try:
            move(db, basket_id, 2, "proposed", "pending", "bot", "telegram")
        except Exception as exc:  # noqa: BLE001
            outcome["error"] = exc
        else:
            outcome["error"] = None

    threads = [threading.Thread(target=first), threading.Thread(target=second)]
    for t in threads:
        t.start()
    first_in.wait(5)
    time.sleep(0.2)
    release.set()
    for t in threads:
        t.join(10)

    assert isinstance(outcome["error"], errors.UniqueViolation)
    assert rows(db, "SELECT count(*) FROM agb.basket_event WHERE seq = 2") == [(1,)]
    assert state(db, basket_id) == ("pending", 2)


@pytest.mark.parametrize(
    ("seq", "from_state", "message"),
    [
        (3, "proposed", "seq 3 does not follow its latest event (seq 1)"),
        (1, None, "seq 1 does not follow"),
        (2, "pending", "it is proposed and not pending"),
        (2, None, "it is proposed and not unproposed"),
    ],
)
def test_an_event_that_does_not_follow_the_latest_is_refused(db, seq, from_state, message):
    basket_id = proposed_basket(db)
    with pytest.raises(errors.CheckViolation, match=re.escape(message)):
        move(db, basket_id, seq, from_state, "pending", "bot", "telegram")
    assert state(db, basket_id) == ("proposed", 1)


@pytest.mark.parametrize(
    ("role", "from_state", "to_state"),
    [
        ("pipeline", "proposed", "pending"),  # the proposer cannot ask for approval
        ("bot", "proposed", "approved"),  # nothing skips the request
        ("scheduler", "proposed", "pending"),
        ("execution", "proposed", "pending"),
        ("ui", "proposed", "submitted"),
        ("agb_owner", "proposed", "pending"),  # the owner is not exempt
    ],
)
def test_a_transition_the_table_does_not_allow_the_role_is_refused(db, role, from_state, to_state):
    basket_id = proposed_basket(db)
    with pytest.raises(errors.InsufficientPrivilege, match=f"{role} may not move basket"):
        move(db, basket_id, 2, from_state, to_state, role, "cli")
    assert state(db, basket_id) == ("proposed", 1)


def test_only_the_pipeline_proposes(db):
    basket_id = bare_basket(db)
    for role in ("bot", "ui", "scheduler", "execution", "agb_owner"):
        with pytest.raises(errors.InsufficientPrivilege, match="may not move basket"):
            move(db, basket_id, 1, None, "proposed", role, "cli")
    assert rows(db, "SELECT count(*) FROM agb.basket_event") == [(0,)]


def test_the_cli_reads_baskets_but_writes_no_event(db):
    basket_id = proposed_basket(db)
    assert rows(db, "SELECT state FROM agb.basket_state") == [("proposed",)]
    with pytest.raises(errors.InsufficientPrivilege, match="permission denied"):
        move(db, basket_id, 2, "proposed", "pending", "cli")


def test_the_transition_table_and_events_are_append_only(db):
    basket_id = proposed_basket(db)
    owner = db.connect("agb_owner")
    for statement in (
        "UPDATE agb.basket_event SET to_state = 'approved'",
        "DELETE FROM agb.basket_event",
        "DELETE FROM agb.basket_transition",
        "UPDATE agb.basket_transition SET role = 'pipeline'",
        f"UPDATE agb.basket SET ranker = 'other' WHERE id = {basket_id}",
        "TRUNCATE agb.basket_target",
        # broker_order references basket_order, so a truncate must name it too (test_jobs
        # truncates agb.job the same way); the append-only trigger still refuses it.
        "TRUNCATE agb.basket_order CASCADE",
    ):
        with pytest.raises(errors.RaiseException, match="append-only"):
            owner.execute(statement)


def test_a_reference_is_a_month_or_a_rerun_of_one(db):
    conn = db.connect("pipeline")
    for reference in ("2026-10-01", "oct", "2026-10r"):
        with pytest.raises(errors.CheckViolation):
            conn.execute(
                "INSERT INTO agb.basket (reference, period, session_date, signal_date,"
                " snapshot_date, partition_hash, universe_size, ranker, cost_model_version,"
                " capital_usd, actor) VALUES (%s, '2026-10', %s, %s, %s, %s, 1, 't', '1', 1,"
                " 'pipeline')",
                (reference, TRADE, SIGNAL, SIGNAL, ZERO_HASH),
            )
    proposed_basket(db, "2026-10r1")
