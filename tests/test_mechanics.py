"""The paper record's three arithmetic checks (docs/architecture/04-risk-and-approval.md).

Each check is measured over what paper trading actually recorded, so every fixture here runs a
basket through the gate and the broker rather than writing fills by hand, except where the
point is a record the broker would never produce: a fee that does not reproduce, or a short
fill that raised no alert.

Nothing passes by vacuity. A check with too little to measure raises `MechanicsError`, which
leaves the criterion reading "not run" in the paper-to-live view.
"""

from dataclasses import replace
from decimal import Decimal

import pytest
from market_fake import TRADE, Market
from test_approval_loop import cli_main
from test_execution import (
    AFTER,
    CLOSE_AT,
    MID,
    NOW,
    PAUSE,
    SYMBOLS,
    WITH_A_SALE,
    approved,
    broker,
)
from test_gate import SOUND, move, rows

from agb import cost, execution, lake, mechanics

_FILL = """
    INSERT INTO agb.fill (order_id, execution_id, qty, price, filled_at, sec_fee_usd,
        taf_fee_usd, cat_fee_usd, commission_usd, fee_model_version, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'execution')
"""


def filled(db, conn, fake, case=SOUND, price=MID, short=None):
    """One paper rebalance, gated and filled the way the services run it."""
    target = approved(db, case)
    assert execution.submit(conn, fake, target, NOW).state == "submitted"
    fake.close(CLOSE_AT, price, short=short)
    assert execution.reconcile(conn, fake, target, AFTER).state == "filled"
    return target


def with_a_sale(db, conn):
    """A rebalance with a sale in it, so the regulatory fees have something to fall on."""
    fake = broker(SYMBOLS)
    fake._hold("H01", Decimal(200), MID)
    return filled(db, conn, fake, WITH_A_SALE)


def closes(tmp_path):
    """A lake whose last session is the trade session, every name closing at 20.00."""
    return lake.connect(Market(tmp_path, end=TRADE).names(25).land())


def by_hand(db, basket_id, short=None):
    """The fills of a submitted basket, written as the broker's reconcile would write them
    but without its alerting, and the basket settled. Used where the record under test is one
    execution itself would not produce."""
    conn = db.connect("agb_owner")
    orders = conn.execute(
        "SELECT id, symbol, side, qty FROM agb.broker_order WHERE basket_id = %s ORDER BY id",
        (basket_id,),
    ).fetchall()
    for order_id, symbol, side, qty in orders:
        got = (short or {}).get(symbol, qty)
        if got == 0:
            continue
        fees = cost.fees(side, got * MID, got)
        conn.execute(
            _FILL,
            (
                order_id,
                f"hand-{order_id}",
                got,
                MID,
                CLOSE_AT,
                fees.sec,
                fees.taf,
                fees.cat,
                fees.commission,
                cost.CURRENT.version,
            ),
        )
    move(db, basket_id, 4, "submitted", "filled", "execution")


def alerts(db):
    return rows(db, "SELECT kind, subject FROM agb.alert ORDER BY id")


# -- fill tolerance ------------------------------------------------------------------------


def test_every_paper_fill_prints_at_the_official_close(db, tmp_path):
    """Acceptance criterion 1: the check measures each fill against the lake's close for the
    session it was for. A market-on-close order fills at the close, so a sound record is
    nought basis points away from it."""
    conn = db.connect("execution")
    filled(db, conn, broker())

    result = mechanics.fill_tolerance(conn, closes(tmp_path))

    assert result.passed is True
    assert result.measured == "20 fills, worst 0.0 bps from the close"
    assert result.threshold == "every fill within 5 bps"
    assert result.detail["breaches"] == []
    assert str(result) == "fill_tolerance: passed, 20 fills, worst 0.0 bps from the close"


def test_a_fill_away_from_the_close_fails_and_names_the_offenders(db, tmp_path):
    """A fill ten cents off a twenty-dollar close is fifty basis points away, well outside
    the rounding room the check allows, and every offending fill is named."""
    conn = db.connect("execution")
    filled(db, conn, broker(), price=Decimal("20.10"))

    result = mechanics.fill_tolerance(conn, closes(tmp_path))

    assert result.passed is False
    assert result.measured == "20 fills, worst 50.0 bps from the close, 20 outside tolerance"
    assert len(result.detail["breaches"]) == 20
    assert result.detail["breaches"][0] == {
        "reference": "2026-10",
        "symbol": "N01",
        "side": "buy",
        "qty": "240",
        "price": "20.10",
        "close": "20.0",
        "bps": "50.0",
    }
    assert mechanics.detail_text(result).splitlines()[0] == (
        "  N01    2026-10  buy at 20.10 against the close 20.0: 50.0 bps"
    )


def test_too_few_paper_fills_refuses_rather_than_passing(db, tmp_path):
    """A pass over a handful of fills would mean nothing, so the check raises instead. The
    lake is never read: the record is too thin before the first price is looked up."""
    conn = db.connect("execution")
    with_a_sale(db, conn)

    with pytest.raises(mechanics.MechanicsError) as refusal:
        mechanics.fill_tolerance(conn, lake.connect(tmp_path))

    assert str(refusal.value) == (
        "12 paper fills on record, fewer than the 20 this check measures over"
    )


def test_a_session_the_lake_has_no_prices_for_refuses(db, tmp_path):
    """The check cannot measure what it cannot price, and will not quietly skip the fill."""
    conn = db.connect("execution")
    filled(db, conn, broker())

    with pytest.raises(mechanics.MechanicsError) as refusal:
        mechanics.fill_tolerance(conn, lake.connect(Market(tmp_path).names(25).land()))

    assert str(refusal.value) == "the lake has no prices for session 2026-10-01"


# -- fee arithmetic ------------------------------------------------------------------------


def test_every_recorded_fee_reproduces_under_the_schedule(db):
    """Acceptance criterion 1: each fill's fees are recomputed from the schedule of the
    cost-model version the fill names, and the paper record agrees with them."""
    conn = db.connect("execution")
    with_a_sale(db, conn)

    result = mechanics.fee_arithmetic(conn)

    assert result.passed is True
    assert result.measured == "12 fills, 1 of them sells, every fee reproduces"
    assert result.threshold == "every fee reproduces under its cost-model version"
    assert result.detail["model_version"] == cost.CURRENT.version
    assert result.detail["wrong"] == []


def test_a_fee_that_does_not_reproduce_and_an_unknown_version_both_fail(db):
    """A fee the schedule does not produce fails, and so does a fill priced under a version
    the cost model does not know, because its fees cannot be checked at all."""
    conn = db.connect("execution")
    target = with_a_sale(db, conn)
    [(sale,)] = rows(
        db, "SELECT id FROM agb.broker_order WHERE basket_id = %s AND side = 'sell'", target.id
    )
    owner = db.connect("agb_owner")
    fees = cost.fees(cost.Side.SELL, Decimal(100) * MID, Decimal(100))
    owner.execute(
        _FILL,
        (
            sale,
            "wrong-fee",
            100,
            MID,
            CLOSE_AT,
            fees.sec + Decimal("0.05"),
            fees.taf,
            fees.cat,
            fees.commission,
            cost.CURRENT.version,
        ),
    )
    owner.execute(
        _FILL,
        (
            sale,
            "wrong-version",
            100,
            MID,
            CLOSE_AT,
            fees.sec,
            fees.taf,
            fees.cat,
            fees.commission,
            "0",
        ),
    )

    result = mechanics.fee_arithmetic(conn)

    assert result.passed is False
    assert result.measured == "14 fills, 3 of them sells, 2 fees do not reproduce"
    assert [row["fee"] for row in result.detail["wrong"]] == ["sec_fee_usd", "fee_model_version"]
    assert result.detail["wrong"][0]["recorded"] == str(fees.sec + Decimal("0.05"))
    assert result.detail["wrong"][0]["expected"] == str(fees.sec)
    assert result.detail["wrong"][1]["expected"] == (f"one of {', '.join(sorted(cost.MODELS))}")


def test_fills_on_both_sides_of_a_fee_change_each_reproduce_under_their_own_version(
    db, monkeypatch
):
    """A fee change puts a new version in force, and fills made before it still name the old
    one. Each is checked under its own version's schedule, so neither side fails; a fill
    recorded under one version but priced under the other does."""
    conn = db.connect("execution")
    target = with_a_sale(db, conn)
    [(sale,)] = rows(
        db, "SELECT id FROM agb.broker_order WHERE basket_id = %s AND side = 'sell'", target.id
    )
    old = cost.CURRENT
    dearer = old.sec_fee_per_million + 5
    pinned = cost.fingerprint({**old.constants(), "sec_fee_per_million": repr(dearer)})
    monkeypatch.setattr(cost, "VERSIONS", {**cost.VERSIONS, "2": pinned})
    new = replace(old, version="2", sec_fee_per_million=dearer)
    monkeypatch.setattr(cost, "MODELS", {**cost.MODELS, "2": new})
    monkeypatch.setattr(cost, "CURRENT", new)
    owner = db.connect("agb_owner")

    def sell(execution_id, model, version):
        fees = cost.fees(cost.Side.SELL, Decimal(100) * MID, Decimal(100), model)
        owner.execute(
            _FILL,
            (
                sale,
                execution_id,
                100,
                MID,
                CLOSE_AT,
                fees.sec,
                fees.taf,
                fees.cat,
                fees.commission,
                version,
            ),
        )
        return fees

    sell("under-the-old-fee", old, "1")
    sell("under-the-new-fee", new, "2")
    assert (
        cost.fees(cost.Side.SELL, Decimal(100) * MID, Decimal(100), old).sec
        != cost.fees(cost.Side.SELL, Decimal(100) * MID, Decimal(100), new).sec
    )

    result = mechanics.fee_arithmetic(conn)

    assert result.passed is True
    assert result.detail["wrong"] == []
    assert result.detail["versions"] == ["1", "2"]
    assert result.detail["model_version"] == "2"

    mispriced = sell("new-fee-named-old", new, "1")

    result = mechanics.fee_arithmetic(conn)

    assert result.passed is False
    [row] = result.detail["wrong"]
    assert row["fee"] == "sec_fee_usd"
    assert row["recorded"] == str(mispriced.sec)


def test_a_paper_record_with_no_sale_refuses(db):
    """The regulatory fees fall on sells, so a record of buys alone has nothing to check and
    the check says so rather than passing."""
    conn = db.connect("execution")
    filled(db, conn, broker())

    with pytest.raises(mechanics.MechanicsError) as refusal:
        mechanics.fee_arithmetic(conn)

    assert str(refusal.value) == (
        "no paper sell fill on record, and the regulatory fees fall on sells only"
    )


# -- partial fills -------------------------------------------------------------------------


def test_a_short_fill_that_alerted_passes(db):
    """Acceptance criterion 1: the check wants a short fill to have happened and to have been
    said out loud. Execution raises the partial-rebalance alert, so this record passes."""
    conn = db.connect("execution")
    filled(db, conn, broker(), short={"N05": Decimal(200)})

    result = mechanics.partial_fills(conn)

    assert result.passed is True
    assert result.measured == "1 short fills in 1 rebalances, each alerted"
    assert result.threshold == (
        "every rebalance with a short fill raises partial_rebalance_short_fill, "
        "or partial_rebalance_halted or partial_rebalance_paused if it was halted"
    )
    assert result.detail == {"orders": 1, "rebalances": ["2026-10"], "unalerted": []}
    assert alerts(db) == [("partial_rebalance_short_fill", "2026-10")]


def test_a_short_fill_that_passed_in_silence_fails(db):
    """A rebalance that filled short and said nothing is the thing this criterion exists to
    catch, so the check names the order and fails."""
    conn = db.connect("execution")
    target = approved(db, SOUND)
    assert execution.submit(conn, broker(), target, NOW).state == "submitted"
    by_hand(db, target.id, short={"N05": Decimal(200)})

    result = mechanics.partial_fills(conn)

    assert result.passed is False
    assert result.measured == "1 short fills in 1 rebalances, 1 of them raised no alert"
    assert result.detail["unalerted"] == [
        {
            "reference": "2026-10",
            "symbol": "N05",
            "side": "buy",
            "ordered": "240",
            "filled": "200",
        }
    ]
    assert mechanics.detail_text(result) == ("  N05    2026-10  buy 200 of 240 filled, no alert")
    assert alerts(db) == []


def test_a_short_fill_alerted_only_at_submission_fails(db):
    """The submission's refusal alert is another cause's. A basket that raised it and then
    filled short without saying so has not alerted the short fill, so the check fails."""
    conn = db.connect("execution")
    target = approved(db, SOUND)
    fake = broker()
    fake.refuse.add("N03")
    assert execution.submit(conn, fake, target, NOW).state == "submitted"
    by_hand(db, target.id, short={"N03": Decimal(0), "N05": Decimal(200)})

    result = mechanics.partial_fills(conn)

    assert alerts(db) == [("partial_rebalance_refused", "2026-10")]
    assert result.passed is False
    assert result.measured == "2 short fills in 1 rebalances, 1 of them raised no alert"
    assert [row["symbol"] for row in result.detail["unalerted"]] == ["N03", "N05"]


def _legacy(db, reference, period, body):
    """An alert under the one kind every partial-rebalance cause shared before they were
    split, as execution raised it then."""
    db.connect("execution").execute(
        "INSERT INTO agb.alert (kind, subject, period, body, raised_by)"
        " VALUES ('partial_rebalance', %s, %s, %s, 'execution')",
        (reference, period, f"basket {reference} {body}"),
    )


def test_a_short_fill_alerted_under_the_shared_kind_passes_and_a_refusal_under_it_does_not(db):
    """Alerts raised before the causes were split all carry `partial_rebalance`. The one the
    close raised still says the short fill was alerted; the submission's, which took the key
    and hid the close's, does not."""
    conn = db.connect("execution")
    target = approved(db, SOUND)
    assert execution.submit(conn, broker(), target, NOW).state == "submitted"
    by_hand(db, target.id, short={"N05": Decimal(200)})

    refused = "submitted with 1 of 20 orders refused by the broker: N03"
    _legacy(db, target.reference, target.reference, refused)
    assert mechanics.partial_fills(conn).passed is False

    # A second period only so the row is not dropped on the key the first one holds.
    _legacy(db, target.reference, "close", "filled short on 1 of 20 orders: N05")
    assert mechanics.partial_fills(conn).passed is True


def test_a_halted_basket_with_an_order_still_live_passes_on_its_halt_alert(db):
    """A basket halted mid-submission is left short by the halt, not by the close, and the
    halt's alert is what names what stayed live."""
    conn = db.connect("execution")
    target = approved(db, SOUND)

    def pause_after_two(fake, client_order_id):
        if len(fake.submits()) == 2:
            db.connect("bot").execute(PAUSE)

    fake = broker(on_submit=pause_after_two)
    fake.cancel_fails.add("ord-0002")
    assert execution.submit(conn, fake, target, NOW).state == "halted"

    result = mechanics.partial_fills(conn)

    assert alerts(db) == [("partial_rebalance_halted", "2026-10")]
    assert result.passed is True
    assert result.measured == "2 short fills in 1 rebalances, each alerted"


def test_a_record_with_no_short_fill_refuses(db):
    """Paper that filled everything in full proves nothing about the handling of a partial
    fill, so the criterion stays unmet until one happens."""
    conn = db.connect("execution")
    filled(db, conn, broker())

    with pytest.raises(mechanics.MechanicsError) as refusal:
        mechanics.partial_fills(conn)

    assert str(refusal.value) == (
        "every paper order filled in full, so there is no partial fill to check the handling of"
    )


# -- recording -----------------------------------------------------------------------------


def test_a_result_is_appended_and_the_newest_one_is_what_the_view_reads(db, tmp_path):
    """Acceptance criterion 6: `paper_check` is append-only, so re-running a check records a
    second row, and the paper-to-live view reads the newest result of each name."""
    conn = db.connect("execution")
    filled(db, conn, broker())
    cli = db.connect("cli")
    bad = mechanics.fill_tolerance(conn, closes(tmp_path))
    first = mechanics.record(cli, replace(bad, passed=False, measured="one fill off"), "ada")
    second = mechanics.record(cli, bad, "ada")

    assert second > first
    assert rows(db, "SELECT id, name, passed, measured FROM agb.paper_check ORDER BY id") == [
        (first, "fill_tolerance", False, "one fill off"),
        (second, "fill_tolerance", True, "20 fills, worst 0.0 bps from the close"),
    ]
    assert rows(db, "SELECT measured, met FROM agb.live_status WHERE name = 'fill_tolerance'") == [
        ("20 fills, worst 0.0 bps from the close", True)
    ]


def test_the_cli_runs_a_check_and_records_what_it_found(db, monkeypatch, tmp_path, capsys):
    """`agb paper-check` runs one check, prints the result and appends it, and exits non-zero
    when the check failed so a script notices."""
    conn = db.connect("execution")
    filled(db, conn, broker(), price=Decimal("20.10"))
    monkeypatch.setenv("AGB_LAKE_DIR", str(Market(tmp_path, end=TRADE).names(25).land()))

    code, out, _ = cli_main(
        db, monkeypatch, capsys, "paper-check", "fill-tolerance", "--actor", "ada"
    )

    assert code == 1
    first, second = out.splitlines()[:2]
    assert first == (
        "fill_tolerance: FAILED, 20 fills, worst 50.0 bps from the close, 20 outside tolerance"
    )
    assert second == "  N01    2026-10  buy at 20.10 against the close 20.0: 50.0 bps"
    assert out.splitlines()[-1] == "paper_check 1: fill_tolerance recorded"
    assert rows(db, "SELECT name, passed, actor FROM agb.paper_check") == [
        ("fill_tolerance", False, "ada")
    ]


def test_the_cli_refuses_a_check_the_record_cannot_support(db, monkeypatch, tmp_path, capsys):
    """A check with too little to measure records nothing: the criterion stays "not run"."""
    conn = db.connect("execution")
    with_a_sale(db, conn)
    monkeypatch.setenv("AGB_LAKE_DIR", str(tmp_path))

    code, _, err = cli_main(db, monkeypatch, capsys, "paper-check", "fill-tolerance")

    assert code == 1
    assert err == (
        "agb: refused: 12 paper fills on record, fewer than the 20 this check measures over\n"
    )
    assert rows(db, "SELECT count(*) FROM agb.paper_check") == [(0,)]
