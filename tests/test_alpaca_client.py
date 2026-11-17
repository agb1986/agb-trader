"""The broker client's surface: how each call reads its answer, what a malformed or empty
answer becomes, how a refusal is told from silence, and how a duplicate client_order_id shows.

The opener is a fake serving one queued answer per attempt, so no request leaves the machine
and no wait is taken.
"""

import io
import json
import urllib.error
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from agb.alpaca import (
    PAGE,
    Alpaca,
    AlpacaError,
    Closed,
    Order,
)

KEY = "key-aaa"
SECRET = "secret-bbb"
ORDER = {
    "id": "ord-1",
    "client_order_id": "2026-10-AAA",
    "symbol": "AAA",
    "side": "buy",
    "qty": "5",
    "status": "new",
    "filled_qty": "0",
    "filled_avg_price": None,
    "updated_at": "2026-10-01T19:59:59.123456789Z",
}


class Unreadable(io.BytesIO):
    """An error body the connection drops while it is being read."""

    def read(self, *args):
        raise OSError("connection reset")


class FakeBroker:
    """Serves `answers` one per attempt and records each request as `(method, url, body)`.

    An answer is a JSON-able value (a 200 with that body), `None` (a 200 with no body), a
    `bytes` (a 200 with exactly those bytes), an `Exception` (raised), or a
    `(status, body)` pair, which is the broker refusing.
    """

    def __init__(self, *answers):
        self.answers = list(answers)
        self.requests: list[tuple[str, str, bytes | None]] = []

    def open(self, request, timeout=None):
        self.requests.append((request.get_method(), request.full_url, request.data))
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, tuple):
            status, body = answer
            raise urllib.error.HTTPError(request.full_url, status, "refused", {}, body)
        if answer is None:
            return io.BytesIO(b"")
        if isinstance(answer, bytes):
            return io.BytesIO(answer)
        return io.BytesIO(json.dumps(answer).encode())


def refusal(status: int, message: str = "no", code: int | None = None):
    body = {"message": message} | ({"code": code} if code is not None else {})
    return (status, io.BytesIO(json.dumps(body).encode()))


def client(*answers) -> tuple[Alpaca, FakeBroker]:
    broker = FakeBroker(*answers)
    return Alpaca(KEY, SECRET, opener=broker, sleep=lambda seconds: None), broker


# --- construction -------------------------------------------------------------------------


@pytest.mark.parametrize("key, secret", [("", SECRET), (KEY, ""), ("", "")])
def test_a_client_needs_both_a_key_and_a_secret(key, secret):
    with pytest.raises(ValueError, match="are not set"):
        Alpaca(key, secret)


def test_the_client_names_its_trading_url_without_a_trailing_slash():
    alpaca = Alpaca(KEY, SECRET, trading="https://paper-api.alpaca.markets/")
    assert alpaca.trading == "https://paper-api.alpaca.markets"


def test_the_environment_says_whether_the_client_is_configured(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)
    assert not Alpaca.configured()
    monkeypatch.setenv("ALPACA_API_KEY", KEY)
    assert not Alpaca.configured()
    monkeypatch.setenv("ALPACA_SECRET_KEY", SECRET)
    assert Alpaca.configured()


def test_a_client_built_from_the_environment_reads_its_urls(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", KEY)
    monkeypatch.setenv("ALPACA_SECRET_KEY", SECRET)
    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets/")
    assert Alpaca.from_env().trading == "https://api.alpaca.markets"


# --- the error ----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status, code, transient, duplicate, not_found, refused",
    [
        (None, None, True, False, False, False),
        (429, None, True, False, False, False),
        (503, None, True, False, False, False),
        (404, None, False, False, True, True),
        (403, None, False, False, False, True),
        (422, 40010001, False, True, False, True),
        (422, 40010000, False, False, False, True),
        (500, 40010001, True, False, False, False),
    ],
)
def test_an_error_says_what_kind_of_failure_it_was(
    status, code, transient, duplicate, not_found, refused
):
    error = AlpacaError("x", status=status, code=code)
    assert error.transient is transient
    assert error.duplicate is duplicate
    assert error.not_found is not_found
    assert error.refused is refused


@pytest.mark.parametrize(
    "status, terminal",
    [
        ("filled", True),
        ("canceled", True),
        ("expired", True),
        ("rejected", True),
        ("done_for_day", True),
        ("new", False),
        ("partially_filled", False),
        ("accepted", False),
    ],
)
def test_an_order_is_terminal_once_the_broker_changes_it_no_more(status, terminal):
    order = Order("o", "c", "AAA", "buy", Decimal(1), status, Decimal(0), None, None)
    assert order.terminal is terminal


# --- answers read -------------------------------------------------------------------------


def test_an_account_is_read_with_its_amounts_as_decimals():
    alpaca, broker = client(
        {
            "status": "ACTIVE",
            "trading_blocked": True,
            "account_blocked": False,
            "equity": "100000.50",
            "cash": "",
            "buying_power": "200001.00",
        }
    )
    account = alpaca.account()
    assert account.status == "ACTIVE"
    assert account.trading_blocked and not account.account_blocked
    assert account.equity == Decimal("100000.50")
    assert account.cash is None
    assert account.buying_power == Decimal("200001.00")
    assert broker.requests[0][:2] == ("GET", "https://paper-api.alpaca.markets/v2/account")


def test_the_configuration_is_read_and_set():
    alpaca, broker = client(
        {"suspend_trade": False, "no_shorting": True},
        {"suspend_trade": True, "no_shorting": True},
    )
    assert not alpaca.configuration().suspend_trade
    assert alpaca.suspend(True).suspend_trade
    method, url, body = broker.requests[1]
    assert (method, url) == ("PATCH", "https://paper-api.alpaca.markets/v2/account/configurations")
    assert json.loads(body) == {"suspend_trade": True}


def test_an_asset_is_read_and_its_symbol_is_quoted_in_the_path():
    alpaca, broker = client({"symbol": "BRK.B", "status": "active", "tradable": True})
    asset = alpaca.asset("BRK.B")
    assert (asset.symbol, asset.status, asset.tradable, asset.fractionable) == (
        "BRK.B",
        "active",
        True,
        False,
    )
    assert broker.requests[0][1].endswith("/v2/assets/BRK.B")


def test_an_asset_answer_without_a_symbol_falls_back_to_the_one_asked_for():
    alpaca, _ = client({"status": "inactive"})
    assert alpaca.asset("ZZZ").symbol == "ZZZ"


def test_no_symbols_is_no_quote_request():
    alpaca, broker = client()
    assert alpaca.latest_quotes([]) == {}
    assert broker.requests == []


def test_a_quote_without_a_usable_bid_ask_or_time_is_left_out():
    alpaca, _ = client(
        {
            "quotes": {
                "GOOD": {"bp": 9.99, "ap": 10.01, "t": "2026-10-01T19:34:59Z"},
                "NOBID": {"bp": 0, "ap": 10.01, "t": "2026-10-01T19:34:59Z"},
                "NOASK": {"bp": 9.99, "t": "2026-10-01T19:34:59Z"},
                "NOTIME": {"bp": 9.99, "ap": 10.01},
            }
        }
    )
    quotes = alpaca.latest_quotes(["GOOD", "NOBID", "NOASK", "NOTIME"])
    assert set(quotes) == {"GOOD"}
    assert quotes["GOOD"].mid == Decimal("10.00")


def test_an_answer_with_no_quotes_is_no_quotes():
    alpaca, _ = client({"quotes": None})
    assert alpaca.latest_quotes(["AAA"]) == {}


def test_a_quote_time_without_an_offset_is_a_broker_error():
    """A time read in this machine's zone could be hours off, so it is refused."""
    alpaca, _ = client({"quotes": {"AAA": {"bp": 9, "ap": 10, "t": "2026-10-01T19:34:59"}}})
    with pytest.raises(AlpacaError, match="no UTC offset"):
        alpaca.latest_quotes(["AAA"])


def test_nanosecond_times_are_read_to_the_microsecond_in_utc():
    alpaca, _ = client(ORDER | {"updated_at": "2026-10-01T15:59:59.123456789-04:00"})
    assert alpaca.order("ord-1").updated_at == datetime(2026, 10, 1, 19, 59, 59, 123456, tzinfo=UTC)


# --- orders -------------------------------------------------------------------------------


def test_a_market_on_close_order_is_sent_with_its_client_order_id():
    alpaca, broker = client(ORDER)
    order = alpaca.submit("AAA", Decimal("5"), "buy", "2026-10-AAA")
    method, url, body = broker.requests[0]
    assert (method, url) == ("POST", "https://paper-api.alpaca.markets/v2/orders")
    assert json.loads(body) == {
        "symbol": "AAA",
        "qty": "5",
        "side": "buy",
        "type": "market",
        "time_in_force": "cls",
        "client_order_id": "2026-10-AAA",
    }
    assert (order.id, order.client_order_id, order.qty) == ("ord-1", "2026-10-AAA", Decimal(5))
    assert order.filled_avg_price is None and not order.terminal


def test_a_fractional_quantity_is_sent_in_fixed_point_never_in_exponent_form():
    alpaca, broker = client(ORDER)
    alpaca.submit("AAA", Decimal("1E-7"), "buy", "c")
    assert json.loads(broker.requests[0][2])["qty"] == "0.0000001"


def test_a_client_order_id_the_broker_has_seen_is_a_duplicate_and_is_not_sent_again():
    alpaca, broker = client(refusal(422, "client_order_id must be unique", code=40010001), ORDER)
    with pytest.raises(AlpacaError) as caught:
        alpaca.submit("AAA", Decimal(5), "buy", "2026-10-AAA")
    assert caught.value.duplicate
    assert caught.value.refused
    assert not caught.value.transient
    assert len(broker.requests) == 1


def test_the_order_behind_a_duplicate_is_found_by_its_client_order_id():
    alpaca, broker = client(ORDER)
    order = alpaca.order_by_client_id("2026-10-AAA")
    assert order.id == "ord-1"
    assert broker.requests[0][1] == (
        "https://paper-api.alpaca.markets/v2/orders:by_client_order_id?client_order_id=2026-10-AAA"
    )


def test_an_order_is_read_by_its_id_and_a_missing_one_is_not_found():
    alpaca, broker = client(
        ORDER | {"status": "filled", "filled_qty": "5", "filled_avg_price": "9.5"}
    )
    order = alpaca.order("ord-1")
    assert order.terminal
    assert (order.filled_qty, order.filled_avg_price) == (Decimal(5), Decimal("9.5"))

    alpaca, broker = client(refusal(404, "order not found"))
    with pytest.raises(AlpacaError) as caught:
        alpaca.order("nope")
    assert caught.value.not_found
    assert len(broker.requests) == 1


def test_an_order_is_cancelled_with_one_delete_and_no_retry():
    alpaca, broker = client(None)
    assert alpaca.cancel("ord-1") is None
    assert broker.requests == [("DELETE", "https://paper-api.alpaca.markets/v2/orders/ord-1", None)]

    alpaca, broker = client(urllib.error.URLError("timed out"), None)
    with pytest.raises(AlpacaError):
        alpaca.cancel("ord-1")
    assert len(broker.requests) == 1


# --- positions and close-all ---------------------------------------------------------------


def test_positions_are_read_and_a_missing_amount_is_none_or_zero():
    alpaca, _ = client(
        [
            {"symbol": "AAA", "qty": "10", "avg_entry_price": "9.5", "market_value": "100"},
            {"symbol": "BBB"},
        ]
    )
    first, second = alpaca.positions()
    assert (first.symbol, first.qty, first.avg_entry_price) == ("AAA", Decimal(10), Decimal("9.5"))
    assert first.market_value == Decimal(100)
    assert (second.qty, second.avg_entry_price, second.market_value) == (Decimal(0), None, None)


def test_no_positions_is_an_empty_list_whether_the_body_is_empty_or_an_empty_array():
    assert client(None)[0].positions() == []
    assert client([])[0].positions() == []


def test_close_all_cancels_orders_and_reports_each_symbol_by_its_own_status():
    alpaca, broker = client(
        [
            {"symbol": "AAA", "status": 200, "body": {"id": "ord-9", "symbol": "AAA"}},
            {"symbol": "BBB", "status": 403, "body": {"message": "halted"}},
            {"symbol": "CCC", "status": 500, "body": {}},
            {"status": "x", "body": {"symbol": "DDD", "message": "odd"}},
            {"status": 200, "body": "not an object"},
        ]
    )
    closed = alpaca.close_all()
    assert broker.requests[0][:2] == (
        "DELETE",
        "https://paper-api.alpaca.markets/v2/positions?cancel_orders=true",
    )
    assert closed == [
        Closed("AAA", 200, "ord-9", None),
        Closed("BBB", 403, None, "halted"),
        Closed("CCC", 500, None, "no message"),
        Closed("DDD", 0, None, "odd"),
        Closed("?", 200, None, None),
    ]
    assert [c.ok for c in closed] == [True, False, False, False, True]


def test_close_all_with_nothing_to_close_is_an_empty_list():
    assert client(None)[0].close_all() == []


# --- fills --------------------------------------------------------------------------------


def fill(n: int, **extra):
    return {
        "id": f"f{n:04d}",
        "order_id": "ord-1",
        "symbol": "AAA",
        "side": "buy",
        "qty": "1",
        "price": "10",
        "transaction_time": "2026-10-01T19:59:59Z",
        "type": "fill",
    } | extra


def test_fills_follow_the_page_token_until_a_short_page():
    first = [fill(n) for n in range(PAGE)]
    alpaca, broker = client(first, [fill(PAGE, type="partial_fill")])
    fills = alpaca.fills(datetime(2026, 10, 1, 12, tzinfo=UTC))
    assert len(fills) == PAGE + 1
    assert fills[-1].partial and not fills[0].partial
    assert "after=2026-10-01T12%3A00%3A00Z" in broker.requests[0][1]
    assert "page_token" not in broker.requests[0][1]
    assert f"page_token=f{PAGE - 1:04d}" in broker.requests[1][1]


def test_a_fill_without_a_quantity_price_or_time_is_skipped():
    alpaca, _ = client(
        [fill(1), fill(2, qty=None), fill(3, price=""), fill(4, transaction_time=None)]
    )
    assert [f.id for f in alpaca.fills(datetime(2026, 10, 1, tzinfo=UTC))] == ["f0001"]


def test_a_broker_that_never_ends_the_pages_is_an_error_not_a_partial_answer(monkeypatch):
    monkeypatch.setattr("agb.alpaca.FILL_PAGES", 3)
    page = [fill(n) for n in range(PAGE)]
    alpaca, _ = client(page, page, page, page)
    with pytest.raises(AlpacaError):
        alpaca.fills(datetime(2026, 10, 1, tzinfo=UTC))


# --- answers that are not what was asked for ---------------------------------------------------


def test_an_empty_answer_where_an_object_is_needed_is_a_broker_error():
    alpaca, _ = client(None)
    with pytest.raises(AlpacaError, match="account: the broker's answer is malformed"):
        alpaca.account()


def test_an_answer_that_is_not_json_is_a_broker_error_naming_the_call():
    alpaca, _ = client(b"<html>bad gateway</html>")
    with pytest.raises(AlpacaError, match="GET /v2/account: the answer is not JSON") as caught:
        alpaca.account()
    assert caught.value.status is None and caught.value.transient


def test_a_refusal_whose_body_cannot_be_read_still_carries_its_status():
    alpaca, _ = client((403, Unreadable()))
    with pytest.raises(AlpacaError) as caught:
        alpaca.asset("AAA")
    assert caught.value.status == 403
    assert "no message" in str(caught.value)
    assert caught.value.refused


def test_a_refusal_whose_body_is_not_json_or_not_an_object_has_no_message_or_code():
    for body in (b"gateway", b"[1, 2]", b'{"message": 5, "code": "x"}'):
        alpaca, _ = client((400, io.BytesIO(body)))
        with pytest.raises(AlpacaError) as caught:
            alpaca.asset("AAA")
        assert caught.value.status == 400
        assert caught.value.code is None


def test_a_refusal_message_never_carries_a_key():
    alpaca, _ = client(refusal(401, f"bad key {KEY} and secret {SECRET}"))
    with pytest.raises(AlpacaError) as caught:
        alpaca.account()
    assert KEY not in str(caught.value) and SECRET not in str(caught.value)
    assert "<secret>" in str(caught.value)


def test_a_request_carries_the_key_headers_and_a_json_body_says_so():
    seen = {}

    class Capture(FakeBroker):
        def open(self, request, timeout=None):
            seen.update(request.header_items())
            return super().open(request, timeout)

    broker = Capture(ORDER)
    Alpaca(KEY, SECRET, opener=broker).submit("AAA", Decimal(1), "buy", "c")
    assert seen["Apca-api-key-id"] == KEY
    assert seen["Apca-api-secret-key"] == SECRET
    assert seen["Content-type"] == "application/json"
