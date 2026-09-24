"""Paper MOC execution against an in-memory broker: the broker checks, one order at a time
with its row committed before the call, a lost answer and a duplicate resolved by lookup, the
halt latch read before every order, the fills with the schedule's fees, the daily snapshot
and the mismatch view, and the service's clock. The broker is `alpaca_fake.FakeBroker`; the
baskets come from tests/test_gate.py, gated the way the pipeline gates them.
"""

import logging
import os
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
import yaml
from alpaca_fake import FakeBroker
from conftest import ROOT
from market_fake import PERIOD, TRADE
from psycopg import errors
from test_gate import (
    BASE,
    HELD,
    MARKS,
    NAMES,
    SOUND,
    Case,
    buy,
    hold,
    insert,
    move,
    rows,
    run,
    sell,
    state,
    targets,
)

from agb import cost, execution, health
from agb.alpaca import AlpacaError, Asset
from agb.execution import ExecutionError
from agb.nyse import NyseCalendar
from agb.services.execution import Execution

# 15:35 ET on the trade session: inside the submission window, which runs 15:30 to 15:45.
NOW = datetime(2026, 10, 1, 19, 35, tzinfo=UTC)
CLOSE_AT = datetime(2026, 10, 1, 20, 0, tzinfo=UTC)
AFTER = datetime(2026, 10, 1, 20, 5, tzinfo=UTC)
NEXT = date(2026, 10, 2)
CALENDAR = NyseCalendar(date(2026, 9, 1), date(2026, 12, 31))
MID = Decimal("20.00")
PAUSE = "INSERT INTO agb.halt_event (cause, action, actor) VALUES ('pause', 'set', 'bot')"


def broker(symbols=NAMES, **changes) -> FakeBroker:
    return FakeBroker(tuple(symbols), **changes)


def approved(db, case=SOUND) -> execution.Basket:
    """A basket in `approved`, through the gate as the pipeline runs it: the schema lets no
    one else move a basket there. `case.book` is held first, as last month's filled basket."""
    if case.book:
        hold(db, case.book)
    basket_id = insert(db, case.target)
    run(db, basket_id, case)
    assert state(db, basket_id)[:2] == ("approved", 2)
    return basket(db, basket_id)


def basket(db, basket_id, state="approved") -> execution.Basket:
    [found] = [b for b in execution.waiting(db.connect("cli"), state) if b.id == basket_id]
    return found


def events(db, order_id):
    return rows(
        db,
        "SELECT kind, status, broker_order_id FROM agb.order_event WHERE order_id = %s ORDER BY id",
        order_id,
    )


def order_id(db, client_order_id):
    [(found,)] = rows(
        db, "SELECT id FROM agb.broker_order WHERE client_order_id = %s", client_order_id
    )
    return found


def halts(db):
    return rows(db, "SELECT cause, basket_id, actor, reason FROM agb.halt_open ORDER BY at")


def alerts(db):
    return rows(db, "SELECT kind, subject, period, body FROM agb.alert ORDER BY id")


def expect_submission(db, period=PERIOD):
    db.connect("scheduler").execute(
        "INSERT INTO agb.expected_event (kind, period, due_at, created_by)"
        " VALUES ('submission', %s, %s, 'scheduler')",
        (period, CALENDAR.moc_cutoff(TRADE)),
    )


# -- submission ----------------------------------------------------------------------------


def test_the_approved_basket_passes_the_broker_checks_and_is_submitted_as_moc(db):
    """Acceptance criteria 1, 2 and 6: the placeholder-ranker basket that passed the gate
    passes the broker checks, every order row exists before its broker call, carries the
    NBBO mid, and the basket is submitted by execution before the cutoff."""
    conn = db.connect("execution")
    target = approved(db)
    expect_submission(db)
    seen = []

    def at_the_broker(fake, client_order_id):
        # The row is committed before the call, and nothing has been recorded about it yet.
        [(order_row, mid, tif, nbbo_at)] = rows(
            db,
            "SELECT id, nbbo_mid, tif, nbbo_at FROM agb.broker_order WHERE client_order_id = %s",
            client_order_id,
        )
        assert (mid, tif, nbbo_at) == (MID, "cls", fake.quotes[client_order_id[-3:]].at)
        assert events(db, order_row) == []
        seen.append(client_order_id)

    fake = broker(on_submit=at_the_broker)
    outcome = execution.submit(conn, fake, target, NOW)

    assert outcome == execution.Outcome(
        target.id, "submitted", 20, "20 orders answered by the broker at 19:35:00Z"
    )
    assert state(db, target.id) == ("submitted", 3, "execution", "execution")
    assert seen == fake.submits() == [f"{PERIOD}-{s}" for s in NAMES]
    assert len(fake.orders) == 20
    # The broker checks, recorded once like the gate's: four on the account, two per order.
    assert rows(db, "SELECT checks, actor FROM agb.broker_check_run") == [(44, "execution")]
    assert rows(db, "SELECT count(*) FROM agb.broker_check WHERE passed") == [(44,)]
    assert rows(
        db,
        "SELECT level, name, symbol, measured, threshold FROM agb.broker_check"
        " WHERE symbol = 'N01' OR level = 'account' ORDER BY id",
    ) == [
        ("account", "account_status", None, "ACTIVE", "ACTIVE"),
        ("account", "trading_blocked", None, "false", "false"),
        ("account", "account_blocked", None, "false", "false"),
        ("account", "suspend_trade", None, "false", "false"),
        ("order", "tradable", "N01", "true", "true"),
        ("order", "asset_status", "N01", "active", "active"),
    ]
    # Every order row: the plan's quantity and side, the NBBO around the mid, one ack each.
    assert rows(
        db,
        "SELECT count(*), min(nbbo_bid), max(nbbo_ask), min(nbbo_mid), max(nbbo_mid),"
        " min(qty), max(qty), bool_and(side = 'buy'), bool_and(tif = 'cls'),"
        " min(length(client_order_id)), max(length(client_order_id))"
        " FROM agb.broker_order",
    ) == [(20, Decimal("19.99"), Decimal("20.01"), MID, MID, 240, 240, True, True, 11, 11)]
    assert rows(db, "SELECT kind, status, count(*) FROM agb.order_event GROUP BY 1, 2") == [
        ("ack", "accepted", 20)
    ]
    assert rows(
        db,
        "SELECT count(*) FROM agb.order_state WHERE kind = 'ack' AND broker_order_id IS NOT NULL",
    ) == [(20,)]
    # The submission expected for the period is fulfilled by the basket event.
    assert rows(
        db,
        "SELECT c.ref_table, c.actor FROM agb.expected_event_completion c"
        " JOIN agb.expected_event e ON e.id = c.expected_event_id"
        " WHERE e.kind = 'submission' AND e.period = %s",
        PERIOD,
    ) == [("basket_event", "execution")]
    assert alerts(db) == []


def test_a_lost_answer_is_looked_up_under_the_client_order_id_and_never_resent(db):
    """Acceptance criterion 3, the crash case: the broker took the order but the answer was
    lost. The row exists without an event; the next cycle finds the order under its
    client_order_id and does not submit it again."""
    conn = db.connect("execution")
    target = approved(db)

    def lose_the_answer(fake, client_order_id):
        if client_order_id == f"{PERIOD}-N03":
            fake.accept("N03", Decimal(240), "buy", client_order_id)
            raise AlpacaError("POST /v2/orders: timed out")

    fake = broker(on_submit=lose_the_answer)
    with pytest.raises(ExecutionError, match=f"{PERIOD}-N03: POST /v2/orders: timed out"):
        execution.submit(conn, fake, target, NOW)

    assert state(db, target.id)[:2] == ("approved", 2)
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(3,)]
    assert events(db, order_id(db, f"{PERIOD}-N03")) == []

    fake.on_submit = None
    outcome = execution.submit(conn, fake, target, NOW)

    assert outcome.state == "submitted"
    assert events(db, order_id(db, f"{PERIOD}-N03")) == [("lookup", "accepted", "ord-0003")]
    assert fake.submits().count(f"{PERIOD}-N03") == 1
    assert ("order_by_client_id", f"{PERIOD}-N03") in fake.calls
    assert len(fake.orders) == 20
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(20,)]


def test_a_row_the_broker_never_saw_is_sent_under_the_same_client_order_id(db):
    """The crash landed before the call: the broker has no such order, so it is sent now,
    under the id the row already carries."""
    conn = db.connect("execution")
    target = approved(db)

    def crash_before_the_call(fake, client_order_id):
        if client_order_id == f"{PERIOD}-N03":
            raise AlpacaError("POST /v2/orders: connection reset")

    fake = broker(on_submit=crash_before_the_call)
    with pytest.raises(ExecutionError):
        execution.submit(conn, fake, target, NOW)
    fake.on_submit = None
    execution.submit(conn, fake, target, NOW)

    assert state(db, target.id)[0] == "submitted"
    assert events(db, order_id(db, f"{PERIOD}-N03")) == [("ack", "accepted", "ord-0003")]
    assert fake.submits().count(f"{PERIOD}-N03") == 2
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(20,)]


def test_a_duplicate_client_order_id_resolves_through_lookup_and_never_a_second_order(db):
    """Acceptance criterion 3: the broker already holds the client_order_id and refuses it
    with 422; the order is looked up and recorded under the broker's id, not resent."""
    conn = db.connect("execution")
    target = approved(db)
    fake = broker()
    held = fake.accept("N05", Decimal(240), "buy", f"{PERIOD}-N05")

    outcome = execution.submit(conn, fake, target, NOW)

    assert outcome.state == "submitted"
    assert events(db, order_id(db, f"{PERIOD}-N05")) == [("lookup", "accepted", held.id)]
    assert rows(db, "SELECT detail FROM agb.order_event WHERE kind = 'lookup'") == [
        (f"POST /v2/orders: HTTP 422: client_order_id must be unique ({PERIOD}-N05)",)
    ]
    assert fake.submits().count(f"{PERIOD}-N05") == 1
    assert len(fake.orders) == 20
    assert rows(db, "SELECT count(*) FROM agb.order_event WHERE kind = 'ack'") == [(19,)]


def test_a_halt_cause_opened_mid_submission_stops_the_remaining_orders(db):
    """Acceptance criterion 4: the latch is read before every order. A pause opened after
    the fifth order cancels those five, sends no sixth, and halts the basket."""
    conn = db.connect("execution")
    target = approved(db)

    def pause_after_five(fake, client_order_id):
        if len(fake.submits()) == 5:
            db.connect("bot").execute(PAUSE)

    fake = broker(on_submit=pause_after_five)
    outcome = execution.submit(conn, fake, target, NOW)

    reason = "halt open mid-submission (pause): 5 of 20 orders sent, 0 not cancelled"
    assert outcome == execution.Outcome(target.id, "halted", 5, reason)
    assert state(db, target.id) == ("halted", 3, "execution", "execution")
    assert rows(
        db, "SELECT reason FROM agb.basket_event WHERE basket_id = %s AND seq = 3", target.id
    ) == [(reason,)]
    assert fake.submits() == [f"{PERIOD}-{s}" for s in NAMES[:5]]
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(5,)]
    assert {o.status for o in fake.orders.values()} == {"canceled"}
    for symbol in NAMES[:5]:
        assert [e[0] for e in events(db, order_id(db, f"{PERIOD}-{symbol}"))] == [
            "ack",
            "cancel_requested",
            "cancelled",
        ]
    assert rows(db, "SELECT count(*) FROM agb.order_state WHERE kind = 'cancelled'") == [(5,)]
    assert [h[0] for h in halts(db)] == ["pause"]
    assert alerts(db) == []


def test_an_order_that_will_not_cancel_is_named_in_a_partial_rebalance_alert(db):
    conn = db.connect("execution")
    target = approved(db)

    def pause_after_two(fake, client_order_id):
        if len(fake.submits()) == 2:
            db.connect("bot").execute(PAUSE)

    fake = broker(on_submit=pause_after_two)
    fake.cancel_fails.add("ord-0002")
    outcome = execution.submit(conn, fake, target, NOW)

    assert outcome.state == "halted"
    assert outcome.reason == (
        "halt open mid-submission (pause): 2 of 20 orders sent, 1 not cancelled"
    )
    assert [e[0] for e in events(db, order_id(db, f"{PERIOD}-N02"))] == [
        "ack",
        "cancel_requested",
        "cancel_failed",
    ]
    assert fake.orders["ord-0001"].status == "canceled"
    assert fake.orders["ord-0002"].status == "accepted"
    assert alerts(db) == [
        (
            "partial_rebalance",
            PERIOD,
            PERIOD,
            f"basket {PERIOD} halted mid-submission; still live at the broker: buy 240 N02",
        )
    ]


def test_a_failed_broker_check_halts_the_basket_like_a_gate_failure(db):
    """Broker data is checked before the first order, recorded like the gate's checks, and a
    failure halts the whole basket and opens `gate_breach` naming the basket."""
    conn = db.connect("execution")
    target = approved(db)
    fake = broker(suspend_trade=True)
    fake.assets["N07"] = Asset("N07", "inactive", False, True)

    outcome = execution.submit(conn, fake, target, NOW)

    reason = (
        "broker checks failed: suspend_trade measured true, needs false;"
        " tradable N07 measured false, needs true; asset_status N07 measured inactive, needs active"
    )
    assert outcome == execution.Outcome(target.id, "halted", 0, reason)
    assert state(db, target.id) == ("halted", 3, "execution", "execution")
    assert halts(db) == [("gate_breach", target.id, "execution", reason)]
    assert rows(
        db,
        "SELECT level, name, symbol, measured, threshold FROM agb.broker_check"
        " WHERE NOT passed ORDER BY id",
    ) == [
        ("account", "suspend_trade", None, "true", "false"),
        ("order", "tradable", "N07", "false", "true"),
        ("order", "asset_status", "N07", "inactive", "active"),
    ]
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(0,)]
    assert fake.submits() == []
    # A later cycle reads the verdict back instead of checking again.
    calls = len(fake.calls)
    assert execution.check(conn, fake, target) == (False, reason)
    assert len(fake.calls) == calls
    assert rows(db, "SELECT count(*) FROM agb.broker_check_run") == [(1,)]
    # The bot sees the breach with the basket and the reason behind it.
    [finding] = health.open_halt_causes(db.connect("bot"))
    assert (finding.kind, finding.subject, finding.what) == (
        "halt",
        "gate_breach",
        "halt open: gate_breach",
    )
    assert finding.facts.endswith(f"by execution, basket {target.id}: {reason}")


def test_a_fractional_quantity_needs_a_fractionable_asset():
    fake = broker(["N01", "N02"])
    fake.assets["N02"] = Asset("N02", "active", True, False)
    plans = [
        execution.Planned(1, "x-N01", "N01", "buy", Decimal("240"), "1", Decimal(0)),
        execution.Planned(2, "x-N02", "N02", "buy", Decimal("240.5"), "1", Decimal(0)),
    ]

    checks = execution.broker_checks(fake, plans)

    assert [(c.name, c.symbol, c.passed) for c in checks if c.level == "order"] == [
        ("tradable", "N01", True),
        ("asset_status", "N01", True),
        ("tradable", "N02", True),
        ("asset_status", "N02", True),
        ("fractionable", "N02", False),
    ]


def test_an_unknown_asset_fails_the_tradable_check_rather_than_the_cycle():
    fake = broker(["N01"])
    plans = [execution.Planned(1, "x-GONE", "GONE", "buy", Decimal(10), "1", Decimal(0))]

    [check] = [c for c in execution.broker_checks(fake, plans) if c.level == "order"]

    assert (check.name, check.symbol, check.measured, check.passed) == (
        "tradable",
        "GONE",
        "unknown",
        False,
    )


def test_a_refused_order_is_recorded_as_rejected_and_the_rest_go_in(db):
    conn = db.connect("execution")
    target = approved(db)
    fake = broker()
    fake.refuse.add("N03")

    outcome = execution.submit(conn, fake, target, NOW)

    assert outcome.state == "submitted"
    assert events(db, order_id(db, f"{PERIOD}-N03")) == [("rejected", "rejected", None)]
    assert len(fake.orders) == 19
    assert alerts(db) == [
        (
            "partial_rebalance",
            PERIOD,
            PERIOD,
            f"basket {PERIOD} submitted with 1 of 20 orders refused by the broker: buy 240 N03",
        )
    ]


def test_only_execution_moves_a_basket_from_approved_to_submitted(db):
    target = approved(db)
    surfaces = (("pipeline", "pipeline"), ("bot", "telegram"), ("scheduler", "scheduler"))
    for role, surface in surfaces:
        with pytest.raises(errors.InsufficientPrivilege, match="may not move basket"):
            move(db, target.id, 3, "approved", "submitted", role, surface=surface)
    move(db, target.id, 3, "approved", "submitted", "execution")
    assert state(db, target.id) == ("submitted", 3, "execution", "execution")


def test_the_submission_window_ends_at_the_moc_cutoff_and_follows_an_early_close():
    assert execution.submit_window(CALENDAR, TRADE) == (
        datetime(2026, 10, 1, 19, 30, tzinfo=UTC),
        datetime(2026, 10, 1, 19, 45, tzinfo=UTC),
    )
    # The day after Thanksgiving closes at 13:00 ET.
    assert execution.submit_window(CALENDAR, date(2026, 11, 27)) == (
        datetime(2026, 11, 27, 17, 30, tzinfo=UTC),
        datetime(2026, 11, 27, 17, 45, tzinfo=UTC),
    )


# -- after the close -----------------------------------------------------------------------

# Eleven buys and the sale of one of ten held names, 200 shares each at $20: $4,000 of a
# $40,000 book, 10% turnover, and the nine names kept make up the twenty positions.
WITH_A_SALE = Case(
    replace(
        BASE,
        targets=targets(HELD[1:], shares=Decimal(200)) + targets(NAMES[:11]),
        orders=(sell("H01", Decimal(200), Decimal(4000)), *(buy(s) for s in NAMES[:11])),
    ),
    marks=MARKS | {s: Decimal(20) for s in HELD},
    book={s: Decimal(200) for s in HELD},
)
SYMBOLS = ("H01", *NAMES[:11])


def submitted(db, conn, fake, case=WITH_A_SALE):
    basket = approved(db, case)
    assert execution.submit(conn, fake, basket, NOW).state == "submitted"
    return basket


def test_each_partial_fill_is_its_own_row_with_the_schedules_fees(db):
    """Acceptance criterion 5: the sale fills in two pieces and the buy in two; each piece
    is a fill row, the sale's pieces carry the schedule's sell-side fees on that piece alone,
    and the buys carry none. The basket is filled once every order is done."""
    conn = db.connect("execution")
    fake = broker(SYMBOLS)
    fake._hold("H01", Decimal(200), MID)
    target = submitted(db, conn, fake)

    assert execution.reconcile(conn, fake, target, CLOSE_AT - timedelta(minutes=30)) == (
        execution.Outcome(target.id, "submitted", 12, f"waiting on {', '.join(SYMBOLS)}")
    )
    assert state(db, target.id)[0] == "submitted"

    price = Decimal("20.10")
    fake.close(
        CLOSE_AT,
        price,
        pieces={"H01": [Decimal(120), Decimal(80)], "N01": [Decimal(100), Decimal(140)]},
    )
    outcome = execution.reconcile(conn, fake, target, AFTER)

    assert outcome == execution.Outcome(
        target.id, "filled", 12, "12 of 12 orders filled in full at 2026-10-01T20:05:00Z"
    )
    assert state(db, target.id) == ("filled", 4, "execution", "execution")
    fills = rows(
        db,
        "SELECT o.symbol, f.qty, f.price, f.sec_fee_usd, f.taf_fee_usd, f.cat_fee_usd,"
        " f.commission_usd, f.fee_model_version, f.filled_at"
        " FROM agb.fill f JOIN agb.broker_order o ON o.id = f.order_id"
        " WHERE o.symbol IN ('H01', 'N01') ORDER BY o.symbol, f.id",
    )
    first = cost.fees(cost.Side.SELL, Decimal(120) * price, Decimal(120))
    second = cost.fees(cost.Side.SELL, Decimal(80) * price, Decimal(80))
    assert fills == [
        ("H01", 120, price, first.sec, first.taf, first.cat, 0, "1", CLOSE_AT),
        ("H01", 80, price, second.sec, second.taf, second.cat, 0, "1", CLOSE_AT),
        ("N01", 100, price, 0, 0, 0, 0, "1", CLOSE_AT),
        ("N01", 140, price, 0, 0, 0, 0, "1", CLOSE_AT),
    ]
    assert first.sec > 0 and first.taf > 0 and first.cat > 0
    assert rows(db, "SELECT count(*) FROM agb.fill") == [(14,)]
    # Every status change was appended, and the shortfall view sees the fills.
    assert rows(
        db, "SELECT kind, status, count(*) FROM agb.order_event GROUP BY 1, 2 ORDER BY 1"
    ) == [("ack", "accepted", 12), ("status", "filled", 12)]
    sale_fees = first.sec + first.taf + first.cat + second.sec + second.taf + second.cat
    assert rows(
        db,
        "SELECT symbol, filled_qty, avg_fill_price, shortfall_bps, fills, fees_usd"
        " FROM agb.shortfall WHERE symbol IN ('H01', 'N01') ORDER BY symbol",
    ) == [("H01", 200, price, -50, 2, sale_fees), ("N01", 240, price, 50, 2, 0)]
    # Reconciling again finds nothing new: the fills are keyed by the broker's execution id.
    assert execution.ingest_fills(conn, fake, CLOSE_AT - timedelta(hours=1)) == 0
    assert alerts(db) == []


def test_an_order_filled_short_is_named_in_a_partial_rebalance_alert(db):
    conn = db.connect("execution")
    fake = broker()
    target = submitted(db, conn, fake, SOUND)

    fake.close(CLOSE_AT, short={"N05": Decimal(200)})
    outcome = execution.reconcile(conn, fake, target, AFTER)

    assert outcome.state == "filled"
    assert outcome.reason == "19 of 20 orders filled in full at 2026-10-01T20:05:00Z"
    assert rows(db, "SELECT status, filled_qty FROM agb.order_state WHERE symbol = 'N05'") == [
        ("expired", 200)
    ]
    assert alerts(db) == [
        (
            "partial_rebalance",
            PERIOD,
            PERIOD,
            f"basket {PERIOD} filled short on 1 of 20 orders: buy 240 N05 filled 200",
        )
    ]


def test_the_daily_snapshot_records_positions_and_flags_a_discrepancy(db):
    """Acceptance criterion 7: after each session the broker's positions and account state
    are written; the mismatch view flags a position the fills do not explain."""
    conn = db.connect("execution")
    fake = broker()
    target = submitted(db, conn, fake, SOUND)
    fake.close(CLOSE_AT)
    execution.reconcile(conn, fake, target, AFTER)

    assert execution.snapshot(conn, fake, TRADE, CALENDAR) is True
    assert execution.snapshot(conn, fake, TRADE, CALENDAR) is False

    assert rows(db, "SELECT snapshot_date, positions, actor FROM agb.position_snapshot") == [
        (TRADE, 20, "execution")
    ]
    assert rows(
        db,
        "SELECT p.symbol, p.qty, p.avg_entry_price, p.market_value_usd"
        " FROM agb.position p ORDER BY p.symbol LIMIT 2",
    ) == [("N01", 240, MID, 4800), ("N02", 240, MID, 4800)]
    assert rows(db, "SELECT count(*) FROM agb.position_mismatch") == [(0,)]
    assert rows(db, "SELECT count(*) FROM agb.broker_state") == [(1,)]

    # The next session: the broker's book says N03 has 200 shares, and no fill explains it.
    fake.book["N03"] = replace(fake.book["N03"], qty=Decimal(200))
    fake.now = CALENDAR.close_at(NEXT)
    assert execution.snapshot(conn, fake, NEXT, CALENDAR) is True

    [(snapshot_id,)] = rows(
        db, "SELECT id FROM agb.position_snapshot WHERE snapshot_date = %s", NEXT
    )
    assert rows(
        db,
        "SELECT snapshot_id, snapshot_date, symbol, expected_qty, observed_qty"
        " FROM agb.position_mismatch",
    ) == [(snapshot_id, NEXT, "N03", 240, 200)]
    assert (
        rows(
            db,
            "SELECT account_status, suspend_trade, trading_blocked, equity_usd"
            " FROM agb.broker_state ORDER BY id",
        )
        == [("ACTIVE", False, False, 100000)] * 2
    )


def test_the_observed_suspend_trade_is_compared_with_the_pause_latch(db):
    """Spec 04: the broker's `suspend_trade` is recorded, and the bot alerts on a mismatch
    with the pause cause in either direction."""
    conn = db.connect("execution")
    bot = db.connect("bot")
    fake = broker()
    assert health.broker_mismatch(bot) == []

    assert execution.record_state(conn, fake) == (False, False)
    assert health.broker_mismatch(bot) == []

    fake.suspend_trade = True
    assert execution.record_state(conn, fake) == (True, False)
    [finding] = health.broker_mismatch(bot)
    [(observed_at,)] = rows(db, "SELECT observed_at FROM agb.broker_state_now")
    assert finding == health.Finding(
        "broker",
        "suspend_trade",
        observed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "broker suspend_trade: on",
        f"no pause open; observed {health.et(observed_at)}",
    )

    bot.execute(PAUSE)
    assert health.broker_mismatch(bot) == []

    fake.suspend_trade = False
    execution.record_state(conn, fake)
    [finding] = health.broker_mismatch(bot)
    assert finding.what == "broker suspend_trade: off"
    assert finding.facts.startswith("pause open; observed ")
    assert rows(db, "SELECT suspend_trade FROM agb.broker_state ORDER BY id") == [
        (False,),
        (True,),
        (False,),
    ]


# -- the service ---------------------------------------------------------------------------


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def test_the_service_submits_in_the_window_and_reconciles_after_the_close(db, caplog):
    conn = db.connect("execution")
    target = approved(db)
    fake = broker()
    clock = Clock(datetime(2026, 10, 1, 19, 29, tzinfo=UTC))
    service = Execution(fake, clock=clock)

    snapshots = "SELECT snapshot_date, positions FROM agb.position_snapshot ORDER BY 1"

    # Before the window: the account is observed and the last closed session snapshotted
    # (an empty book: the service has never traded); nothing is sent.
    service(conn)
    assert state(db, target.id)[0] == "approved"
    assert fake.submits() == []
    assert rows(db, "SELECT count(*) FROM agb.broker_state") != [(0,)]
    assert rows(db, snapshots) == [(date(2026, 9, 30), 0)]

    # In the window: submitted.
    clock.now = NOW
    service(conn)
    assert state(db, target.id) == ("submitted", 3, "execution", "execution")
    assert len(fake.submits()) == 20

    # Right after the close the broker is given time to settle.
    clock.now = CLOSE_AT + timedelta(minutes=1)
    fake.close(CLOSE_AT)
    service(conn)
    assert state(db, target.id)[0] == "submitted"
    assert rows(db, snapshots) == [(date(2026, 9, 30), 0)]

    # Then the fills are read back, the basket is filled and the session snapshotted.
    clock.now = AFTER
    service(conn)
    assert state(db, target.id) == ("filled", 4, "execution", "execution")
    assert rows(db, snapshots) == [(date(2026, 9, 30), 0), (TRADE, 20)]
    assert rows(db, "SELECT count(*) FROM agb.fill") == [(20,)]

    # The next cycle has nothing to do, and does not snapshot the session twice.
    service(conn)
    assert rows(db, snapshots) == [(date(2026, 9, 30), 0), (TRADE, 20)]
    assert "ERROR" not in caplog.text


def test_a_basket_whose_window_has_closed_stays_approved_with_a_warning(db, caplog):
    conn = db.connect("execution")
    target = approved(db)
    fake = broker()
    service = Execution(fake, clock=Clock(datetime(2026, 10, 1, 19, 46, tzinfo=UTC)))

    with caplog.at_level(logging.WARNING):
        service(conn)

    assert state(db, target.id)[0] == "approved"
    assert fake.submits() == []
    assert (
        f"basket {PERIOD}: its submission window closed at 2026-10-01 15:45 ET; leaving it approved"
    ) in caplog.text


def test_an_error_at_the_broker_ends_the_cycle_and_the_next_one_carries_on(db, caplog):
    conn = db.connect("execution")
    target = approved(db)

    def drop_the_line(fake, client_order_id):
        if client_order_id == f"{PERIOD}-N10":
            raise AlpacaError("POST /v2/orders: connection reset")

    fake = broker(on_submit=drop_the_line)
    service = Execution(fake, clock=Clock(NOW))

    with caplog.at_level(logging.ERROR):
        service(conn)
    assert f"cycle stopped: {PERIOD}-N10: POST /v2/orders: connection reset" in caplog.text
    assert state(db, target.id)[0] == "approved"
    assert len(fake.orders) == 9

    fake.on_submit = None
    service(conn)
    assert state(db, target.id)[0] == "submitted"
    assert len(fake.orders) == 20


def test_without_keys_the_service_beats_only(db, caplog):
    conn = db.connect("execution")
    approved(db)
    service = Execution(None, clock=Clock(NOW))

    with caplog.at_level(logging.WARNING):
        service(conn)
        service(conn)

    assert caplog.text.count("ALPACA_API_KEY and ALPACA_SECRET_KEY are not set") == 1
    assert rows(db, "SELECT count(*) FROM agb.broker_state") == [(0,)]
    assert rows(db, "SELECT count(*) FROM agb.broker_order") == [(0,)]


def test_the_account_is_observed_every_five_minutes(db):
    conn = db.connect("execution")
    fake = broker()
    clock = Clock(datetime(2026, 10, 1, 14, 0, tzinfo=UTC))
    service = Execution(fake, clock=clock)

    def observations():
        [(n,)] = rows(db, "SELECT count(*) FROM agb.broker_state")
        return n

    # The first cycle observes the account, and again with the snapshot of the last session.
    service(conn)
    assert observations() == 2
    service(conn)
    clock.now += timedelta(minutes=4, seconds=59)
    service(conn)
    assert observations() == 2
    clock.now += timedelta(seconds=1)
    service(conn)
    assert observations() == 3


# -- where the keys live -------------------------------------------------------------------


def test_only_the_execution_env_file_names_the_alpaca_keys():
    """Acceptance criterion 8: the keys exist only in execution's env file, and only the
    execution service reads that file."""
    examples = sorted((ROOT / "env").glob("*.env.example"))
    assert [p.name for p in examples if "ALPACA" in p.read_text()] == ["execution.env.example"]
    text = (ROOT / "env" / "execution.env.example").read_text()
    assert "ALPACA_API_KEY=" in text and "ALPACA_SECRET_KEY=" in text

    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]

    def env_files(service):
        files = service.get("env_file") or []
        return [files] if isinstance(files, str) else files

    readers = [
        name
        for name, service in services.items()
        if any(os.path.basename(f) == "execution.env" for f in env_files(service))
    ]
    assert readers == ["execution"]
    assert services["execution"]["environment"]["PGUSER"] == "execution"
    assert not any(
        "ALPACA" in key
        for service in services.values()
        for key in (service.get("environment") or {})
    )
