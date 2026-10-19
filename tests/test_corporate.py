"""Forced corporate events (T19, issue #32): the four cases the ranker defines, the rules both
the backtest and the live pipeline read them through, and the record they are kept in.

The rules themselves are pure, so most of this needs no database. What needs one is the
record: an event is written by a human with a command behind it, a second event of the same
kind for the same name and period is refused, nothing may edit one afterwards, and the daily
snapshot check reads the share deltas back so a recorded action stops the mismatch alert and
an unrecorded one does not.

tests/test_rebalance.py covers the selection a forced event feeds, tests/test_gate.py the
short basket it makes legitimate, tests/test_basket.py the live rebalance end to end, and
tests/test_backtest.py the same four cases in a simulation.
"""

from dataclasses import replace
from datetime import date
from decimal import Decimal

import pytest
from psycopg import errors
from test_execution import AFTER, CALENDAR, CLOSE_AT, MID, NEXT, SOUND, TRADE, broker, submitted

from agb import approval, cli, corporate, execution, health
from agb.alpaca import Position
from agb.corporate import CorporateError, Event, Kind
from agb.roles import OWNER

PERIOD = "2026-11"
EFFECTIVE = date(2026, 10, 20)
SHARES = Decimal(240)

ACQUIRED = Event(PERIOD, "N01", Kind.CASH_ACQUISITION, EFFECTIVE, SHARES, "deal closed at $31",
                 price_usd=Decimal("31.00"))  # fmt: skip
MERGED = Event(PERIOD, "N02", Kind.STOCK_MERGER, EFFECTIVE, SHARES, "0.5 ACQ a share",
               into_symbol="ACQ", into_shares=Decimal(120))  # fmt: skip
DELISTED = Event(PERIOD, "N03", Kind.DELISTING, EFFECTIVE, SHARES, "moved to OTC",
                 price_usd=Decimal("4.20"))  # fmt: skip
HALTED = Event(PERIOD, "N04", Kind.HALT, EFFECTIVE, SHARES, "T12 halt, still suspended")
ALL_FOUR = (ACQUIRED, MERGED, DELISTED, HALTED)
SOURCE = approval.Source("ada", "cli", "agb corporate record halt N04 ...")


# The rules.


REFUSED = {
    "a period that is not a month": (ACQUIRED, {"period": "2026-1"}, "not a period"),
    "a kind nothing handles": (HALTED, {"kind": "spinoff"}, "is not a forced event"),
    "no symbol": (HALTED, {"symbol": ""}, "needs a symbol"),
    "a holding of nothing": (HALTED, {"shares": Decimal(0)}, "must be positive"),
    "a negative holding": (HALTED, {"shares": Decimal(-1)}, "must be positive"),
    "no source for the fact": (HALTED, {"detail": ""}, "say where the fact came from"),
    "an acquirer with no shares": (
        MERGED,
        {"into_shares": None},
        "name the acquirer and the shares",
    ),
    "shares with no acquirer": (
        MERGED,
        {"into_symbol": None},
        "name the acquirer and the shares",
    ),
    "a halt that pays in another name": (
        HALTED,
        {"into_symbol": "ACQ", "into_shares": Decimal(10)},
        "only a stock_merger pays in another name",
    ),
    "a merger that pays in nothing": (
        MERGED,
        {"into_shares": Decimal(0)},
        "positive number of shares",
    ),
    "an acquisition without its deal price": (
        ACQUIRED,
        {"price_usd": None},
        "needs its deal price",
    ),
    "a halt that settled in cash": (HALTED, {"price_usd": Decimal(5)}, "settles at no price"),
    "a price of nothing": (DELISTED, {"price_usd": Decimal(0)}, "must be positive"),
}


@pytest.mark.parametrize(("base", "changes", "why"), REFUSED.values(), ids=REFUSED)
def test_an_event_the_rules_will_not_take(base, changes, why):
    """The rules are the table's checks, mirrored, so a bad event never reaches the database."""
    with pytest.raises(CorporateError, match=why):
        replace(base, **changes)


def test_an_acquisition_and_a_merger_terminate_the_position_and_a_delisting_and_a_halt_do_not():
    assert [e.terminates for e in ALL_FOUR] == [True, True, False, False]
    assert [e.shares_after for e in ALL_FOUR] == [Decimal(0), Decimal(0), SHARES, SHARES]


def test_a_merger_has_an_exchange_ratio_and_the_other_three_do_not():
    assert MERGED.ratio == Decimal("0.5")
    for event in (ACQUIRED, DELISTED, HALTED):
        with pytest.raises(CorporateError, match="no exchange ratio"):
            event.ratio  # noqa: B018


def test_the_events_of_one_rebalance_are_read_in_the_order_they_took_effect():
    earlier = replace(HALTED, effective_date=date(2026, 10, 2))
    other_month = replace(DELISTED, period="2026-12")

    assert corporate.in_period([ACQUIRED, earlier, other_month], PERIOD) == (earlier, ACQUIRED)
    assert corporate.in_period([ACQUIRED, other_month], "2026-12") == (other_month,)


def test_a_halt_and_a_terminated_position_cannot_trade_and_a_delisting_can():
    assert corporate.untradeable(ALL_FOUR) == frozenset({"N01", "N02", "N04"})


def test_a_delisted_stub_and_the_acquirer_shares_are_divested():
    assert corporate.divest(ALL_FOUR) == frozenset({"N03", "ACQ"})


def test_every_name_an_event_accounts_for_is_explained_subjects_and_acquirers_alike():
    assert corporate.explained(ALL_FOUR) == frozenset({"N01", "N02", "N03", "N04", "ACQ"})
    assert corporate.explained(()) == frozenset()


def test_the_settled_prices_are_the_deal_price_and_the_last_listed_print():
    assert corporate.exits(ALL_FOUR) == {"N01": Decimal("31.00"), "N03": Decimal("4.20")}


def test_only_the_two_terminating_kinds_are_applied_on_their_effective_date():
    assert corporate.terminations(ALL_FOUR) == {"N01": ACQUIRED, "N02": MERGED}


def test_the_book_the_rebalance_starts_from_has_the_events_folded_in():
    book = {"N01": SHARES, "N02": SHARES, "N03": SHARES, "N04": SHARES, "N05": SHARES}

    assert corporate.applied(book, ALL_FOUR) == {
        "N03": SHARES,
        "N04": SHARES,
        "N05": SHARES,
        "ACQ": Decimal(120),
    }


def test_an_event_against_a_name_another_book_holds_is_ignored():
    assert corporate.applied({"N05": SHARES}, ALL_FOUR) == {"N05": SHARES}


def test_acquirer_shares_add_to_a_holding_of_the_acquirer_already_there():
    book = {"N02": SHARES, "ACQ": Decimal(80)}
    assert corporate.applied(book, [MERGED]) == {"ACQ": Decimal(200)}


def test_every_kind_is_counted_for_the_report_whether_it_happened_or_not():
    assert corporate.counts([MERGED, MERGED]) == {
        "cash_acquisition_events": 0,
        "stock_merger_events": 2,
        "delisting_events": 0,
        "halt_events": 0,
    }
    assert set(corporate.counts(()).values()) == {0}


def test_each_kind_reads_as_a_line():
    assert [corporate.line(e) for e in ALL_FOUR] == [
        "N01 cash_acquisition 2026-10-20 (240 shares): terminated at 31.00 a share;"
        " 2026-11 rebalance",
        "N02 stock_merger 2026-10-20 (240 shares): converted into 120 ACQ; 2026-11 rebalance",
        "N03 delisting 2026-10-20 (240 shares): delisted, last listed print 4.20;"
        " 2026-11 rebalance",
        "N04 halt 2026-10-20 (240 shares): halted across the rebalance, carried; 2026-11 rebalance",
    ]


# The record.


def rows(db, query, *params):
    return db.connect("cli").execute(query, params).fetchall()


def test_an_event_is_recorded_with_the_command_that_carried_it_and_reads_back(db):
    conn = db.connect("cli")

    ids = [corporate.record(conn, event, SOURCE) for event in ALL_FOUR]

    assert len(set(ids)) == 4
    assert corporate.for_period(conn, PERIOD) == ALL_FOUR
    assert corporate.for_period(conn, "2026-12") == ()
    assert rows(
        db,
        "SELECT e.symbol, e.kind, e.shares_before, e.shares_after, c.actor, c.surface, c.verb,"
        " c.outcome FROM agb.forced_event e JOIN agb.command c ON c.id = e.command_id"
        " ORDER BY e.id",
    ) == [
        ("N01", "cash_acquisition", Decimal(240), Decimal(0), "ada", "cli", "corporate",
         "accepted"),
        ("N02", "stock_merger", Decimal(240), Decimal(0), "ada", "cli", "corporate", "accepted"),
        ("N03", "delisting", Decimal(240), Decimal(240), "ada", "cli", "corporate", "accepted"),
        ("N04", "halt", Decimal(240), Decimal(240), "ada", "cli", "corporate", "accepted"),
    ]  # fmt: skip


def test_a_second_event_of_the_same_kind_for_the_same_name_and_period_is_refused(db):
    conn = db.connect("cli")
    corporate.record(conn, HALTED, SOURCE)

    with pytest.raises(CorporateError, match="N04 already has a halt recorded for 2026-11"):
        corporate.record(conn, replace(HALTED, detail="filed twice"), SOURCE)

    assert rows(db, "SELECT count(*) FROM agb.forced_event") == [(1,)]
    # The refusal names the day the first filing was recorded, which is the day the test runs,
    # so it is read back from the row rather than written in here.
    filed = rows(db, "SELECT at::date FROM agb.forced_event")[0][0]
    assert rows(db, "SELECT outcome, reason FROM agb.command ORDER BY id")[1] == (
        "refused",
        f"N04 already has a halt recorded for 2026-11, by ada on {filed:%Y-%m-%d}",
    )


def test_the_same_name_can_have_a_second_event_of_another_kind(db):
    conn = db.connect("cli")
    corporate.record(conn, HALTED, SOURCE)
    corporate.record(conn, replace(DELISTED, symbol="N04"), SOURCE)

    assert [e.kind for e in corporate.for_period(conn, PERIOD)] == [Kind.HALT, Kind.DELISTING]


@pytest.mark.parametrize("statement", [
    "UPDATE agb.forced_event SET detail = 'edited'",
    "DELETE FROM agb.forced_event",
    "TRUNCATE agb.forced_event",
])  # fmt: skip
def test_a_recorded_event_cannot_be_changed_afterwards_even_by_the_owner(db, statement):
    """An operator who filed a wrong event records the right one; the wrong row stays."""
    corporate.record(db.connect("cli"), HALTED, SOURCE)

    with pytest.raises(errors.RaiseException, match="append-only"):
        db.connect(OWNER).execute(statement)


def test_no_service_may_record_an_event(db):
    conn = db.connect("pipeline")
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(
            "INSERT INTO agb.forced_event (period, symbol, kind, effective_date, shares_before,"
            " shares_after, detail, actor, surface, command_id)"
            " VALUES ('2026-11', 'N04', 'halt', '2026-10-20', 240, 240, 'x', 'pipeline',"
            " 'cli', 1)"
        )


def test_a_recorded_event_is_in_the_audit_log(db):
    corporate.record(db.connect("cli"), MERGED, SOURCE)

    assert rows(
        db,
        "SELECT source, actor, event, subject, detail FROM agb.audit_log"
        " WHERE source = 'forced_event'",
    ) == [("forced_event", "ada", "forced stock_merger", "N02 2026-11", "0.5 ACQ a share")]


# The operator's command.


MERGER = (
    "corporate", "record", "stock_merger", "N02",
    "--period", PERIOD,
    "--effective", EFFECTIVE.isoformat(),
    "--shares", "240",
    "--detail", "0.5 ACQ a share",
    "--into", "ACQ",
    "--into-shares", "120",
    "--actor", "ada",
)  # fmt: skip


def run(db, monkeypatch, capsys, *argv):
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)
    code = cli.main(list(argv))
    out, err = capsys.readouterr()
    return code, out, err


def test_the_command_records_an_event_and_shows_it_back(db, monkeypatch, capsys):
    code, out, _ = run(db, monkeypatch, capsys, *MERGER)

    assert code == 0
    assert out.startswith("forced_event ")
    assert out.endswith(f": {corporate.line(MERGED)}\n")
    assert rows(db, "SELECT symbol, kind, actor, surface FROM agb.forced_event") == [
        ("N02", "stock_merger", "ada", "cli")
    ]
    assert rows(db, "SELECT verb, outcome, raw_text FROM agb.command") == [
        (
            "corporate",
            "accepted",
            "agb corporate record stock_merger N02 --period 2026-11 --effective 2026-10-20"
            " --shares 240 --detail '0.5 ACQ a share' --into ACQ --into-shares 120",
        )
    ]

    code, out, _ = run(db, monkeypatch, capsys, "corporate", "show", PERIOD)

    assert code == 0
    assert out == f"{corporate.line(MERGED)}\n"


def test_a_period_with_nothing_recorded_says_so_rather_than_printing_nothing(
    db, monkeypatch, capsys
):
    code, out, _ = run(db, monkeypatch, capsys, "corporate", "show", PERIOD)

    assert code == 0
    assert out == "no forced events recorded for 2026-11\n"


def test_the_command_refuses_an_event_the_rules_reject_and_records_nothing(db, monkeypatch, capsys):
    """A cash acquisition with no deal price never reaches the table, so there is nothing to
    undo and nothing to explain a short basket with."""
    code, _, err = run(
        db, monkeypatch, capsys,
        "corporate", "record", "cash_acquisition", "N01",
        "--period", PERIOD,
        "--effective", EFFECTIVE.isoformat(),
        "--shares", "240",
        "--detail", "deal closed",
        "--actor", "ada",
    )  # fmt: skip

    assert code == 1
    assert err == "agb: refused: N01: a cash_acquisition needs its deal price\n"
    assert rows(db, "SELECT count(*) FROM agb.forced_event") == [(0,)]


# The daily snapshot check.


@pytest.fixture
def snapshotted(db):
    """A book of twenty positions of 240 shares, snapshot after the trade session, as
    tests/test_execution.py leaves it."""
    conn = db.connect("execution")
    fake = broker()
    target = submitted(db, conn, fake, SOUND)
    fake.close(CLOSE_AT)
    execution.reconcile(conn, fake, target, AFTER)
    assert execution.snapshot(conn, fake, TRADE, CALENDAR) is True
    return conn, fake


def mismatches(db):
    return rows(
        db, "SELECT symbol, expected_qty, observed_qty FROM agb.position_mismatch ORDER BY symbol"
    )


def next_session(conn, fake):
    fake.now = CALENDAR.close_at(NEXT)
    assert execution.snapshot(conn, fake, NEXT, CALENDAR) is True


def test_an_unrecorded_corporate_action_raises_the_mismatch(db, snapshotted):
    conn, fake = snapshotted
    # N01 was acquired for cash overnight: the broker's book has lost it and no fill says why.
    fake.book["N01"] = replace(fake.book["N01"], qty=Decimal(0))

    next_session(conn, fake)

    assert mismatches(db) == [("N01", Decimal(240), Decimal(0))]


def test_the_alert_names_the_unrecorded_action_and_leaves_the_recorded_one_out(db, snapshotted):
    conn, fake = snapshotted
    # N01 was acquired for cash and filed; N05 lost shares with nothing on the record.
    fake.book["N01"] = replace(fake.book["N01"], qty=Decimal(0))
    fake.book["N05"] = replace(fake.book["N05"], qty=Decimal(200))
    corporate.record(db.connect("cli"), replace(ACQUIRED, effective_date=NEXT), SOURCE)

    next_session(conn, fake)

    [finding] = health.position_mismatch(db.connect("bot"))
    assert finding == health.Finding(
        "position_mismatch",
        "N05",
        NEXT.isoformat(),
        "position mismatch: N05",
        f"broker holds 200, expected 240 from the previous snapshot plus fills; snapshot {NEXT}",
    )


def test_a_recorded_acquisition_keeps_the_mismatch_quiet(db, snapshotted):
    conn, fake = snapshotted
    fake.book["N01"] = replace(fake.book["N01"], qty=Decimal(0))
    corporate.record(db.connect("cli"), replace(ACQUIRED, effective_date=NEXT), SOURCE)

    next_session(conn, fake)

    assert mismatches(db) == []


def test_a_recorded_merger_accounts_for_the_acquirer_shares_that_arrived(db, snapshotted):
    conn, fake = snapshotted
    fake.book["N02"] = replace(fake.book["N02"], qty=Decimal(0))
    fake.book["ACQ"] = Position("ACQ", Decimal(120), MID, Decimal(120) * MID)
    corporate.record(db.connect("cli"), replace(MERGED, effective_date=NEXT), SOURCE)

    next_session(conn, fake)

    assert mismatches(db) == []


def test_an_event_on_the_record_quiets_only_the_name_it_names(db, snapshotted):
    conn, fake = snapshotted
    fake.book["N01"] = replace(fake.book["N01"], qty=Decimal(0))
    fake.book["N05"] = replace(fake.book["N05"], qty=Decimal(200))
    corporate.record(db.connect("cli"), replace(ACQUIRED, effective_date=NEXT), SOURCE)

    next_session(conn, fake)

    assert mismatches(db) == [("N05", Decimal(240), Decimal(200))]


def test_recording_the_event_after_the_snapshot_was_read_quiets_it_then_too(db, snapshotted):
    conn, fake = snapshotted
    fake.book["N03"] = replace(fake.book["N03"], qty=Decimal(0))
    next_session(conn, fake)
    assert mismatches(db) == [("N03", Decimal(240), Decimal(0))]

    # A delisting takes nothing away by itself, so the exit is the ordinary sell that follows;
    # the acquisition is what the snapshot of a vanished position needs on the record.
    corporate.record(
        db.connect("cli"),
        replace(ACQUIRED, symbol="N03", effective_date=NEXT, detail="acquired for cash"),
        SOURCE,
    )

    assert mismatches(db) == []


def test_an_event_that_took_effect_before_the_previous_snapshot_is_not_counted_twice(
    db, snapshotted
):
    conn, fake = snapshotted
    corporate.record(db.connect("cli"), replace(ACQUIRED, effective_date=TRADE), SOURCE)

    next_session(conn, fake)

    # The book still holds N01, and the event belongs to the window before this snapshot.
    assert mismatches(db) == []
