"""The broker client's numbers, instants and endpoint (issue #152): a quantity reaches Alpaca in
fixed-point notation, an instant without an offset is a malformed answer, a fill history has a
page limit, and the execution service will not start with a trading endpoint that is not the
one for the trading mode in force.

tests/test_alpaca_retry.py covers the retry and the malformed answers the client already
refused; tests/test_execution.py covers the service's cycle.
"""

import io
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from test_execution import broker
from test_gate import go_live

from agb import alpaca
from agb.alpaca import LIVE_TRADING_API, TRADING_API, Alpaca, AlpacaError, BaseUrlError
from agb.services import execution as service
from agb.services.execution import Execution

KEY = "key-aaa"
SECRET = "secret-bbb"
ORDER = {
    "id": "o-1",
    "client_order_id": "c-1",
    "symbol": "N01",
    "side": "buy",
    "qty": "0.0000001",
    "status": "accepted",
    "filled_qty": "0",
    "updated_at": "2026-11-13T21:00:00.123456789Z",
}


class Recorder:
    """An opener that answers each request with the next of `answers`, as JSON, and keeps
    every request it was sent."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.requests = []

    def open(self, request, timeout=None):
        self.requests.append(request)
        return io.BytesIO(json.dumps(self.answers.pop(0)).encode())


def client(opener: Recorder) -> Alpaca:
    return Alpaca(KEY, SECRET, opener=opener)


def fill(n: int, at: str = "2026-11-13T21:00:01Z") -> dict:
    return {"id": f"f-{n}", "order_id": "o-1", "symbol": "N01", "side": "buy", "qty": "1",
            "price": "10", "transaction_time": at, "type": "fill"}  # fmt: skip


# Quantities.


@pytest.mark.parametrize(
    "qty, sent",
    [
        (Decimal("1E-7"), "0.0000001"),
        (Decimal("1E+3"), "1000"),
        (Decimal("12.50"), "12.50"),
        (Decimal(240), "240"),
    ],
)
def test_a_quantity_is_sent_in_fixed_point_notation(qty, sent):
    opener = Recorder(ORDER)

    client(opener).submit("N01", qty, "buy", "c-1")

    assert json.loads(opener.requests[0].data)["qty"] == sent


# Instants.


def test_an_instant_is_read_in_utc_whatever_offset_it_carries():
    opener = Recorder([fill(1, "2026-11-13T16:00:01-05:00")])

    [read] = client(opener).fills(datetime(2026, 11, 13, tzinfo=UTC))

    assert read.at == datetime(2026, 11, 13, 21, 0, 1, tzinfo=UTC)


def test_an_order_whose_instant_has_no_offset_is_a_malformed_answer():
    opener = Recorder({**ORDER, "updated_at": "2026-11-13T21:00:00"})

    with pytest.raises(AlpacaError, match="order: .* has no UTC offset"):
        client(opener).order("o-1")


def test_a_fill_whose_instant_has_no_offset_is_a_malformed_answer_not_a_guess():
    opener = Recorder([fill(1, "2026-11-13T21:00:01")])

    with pytest.raises(AlpacaError, match="fills: .* has no UTC offset"):
        client(opener).fills(datetime(2026, 11, 13, tzinfo=UTC))


# The fill history's pages.


def test_the_fill_history_is_read_page_by_page_until_a_short_one(monkeypatch):
    monkeypatch.setattr(alpaca, "PAGE", 2)
    opener = Recorder([fill(1), fill(2)], [fill(3)])

    fills = client(opener).fills(datetime(2026, 11, 13, tzinfo=UTC))

    assert [f.id for f in fills] == ["f-1", "f-2", "f-3"]
    assert "page_token=f-2" in opener.requests[1].full_url


def test_a_fill_history_that_never_ends_stops_at_the_page_limit(monkeypatch):
    monkeypatch.setattr(alpaca, "PAGE", 2)
    monkeypatch.setattr(alpaca, "FILL_PAGES", 3)
    opener = Recorder(*([fill(1), fill(2)] for _ in range(10)))

    with pytest.raises(AlpacaError, match="more than 3 pages of 2"):
        client(opener).fills(datetime(2026, 11, 13, tzinfo=UTC))
    assert len(opener.requests) == 3


# The trading endpoint.


@pytest.mark.parametrize(
    "url, base",
    [
        ("https://paper-api.alpaca.markets", TRADING_API),
        ("https://paper-api.alpaca.markets/", TRADING_API),
        ("https://api.alpaca.markets", LIVE_TRADING_API),
        ("https://api.alpaca.markets/", LIVE_TRADING_API),
    ],
)
def test_alpaca_s_two_trading_endpoints_are_accepted(url, base):
    assert alpaca.trading_url(url) == base


@pytest.mark.parametrize(
    "url",
    [
        "http://paper-api.alpaca.markets",
        "https://paper-api.alpaca.markets.example.com",
        "https://paper-api.alpaca.markets:8443",
        "https://paper-api.alpaca.markets/v2",
        "https://user@paper-api.alpaca.markets",
        "https://data.alpaca.markets",
        "https://example.com",
        "paper-api.alpaca.markets",
    ],
)
def test_anything_else_is_refused_by_name(url):
    with pytest.raises(BaseUrlError, match=f"ALPACA_BASE_URL '{url}' is not an Alpaca"):
        alpaca.trading_url(url)


def test_the_client_from_the_environment_refuses_an_endpoint_that_is_not_alpaca_s(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", KEY)
    monkeypatch.setenv("ALPACA_SECRET_KEY", SECRET)
    monkeypatch.setenv("ALPACA_BASE_URL", "http://paper-api.alpaca.markets")

    with pytest.raises(BaseUrlError, match="http://paper-api.alpaca.markets"):
        Alpaca.from_env()


def test_the_client_from_the_environment_reaches_paper_unless_told_otherwise(monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", KEY)
    monkeypatch.setenv("ALPACA_SECRET_KEY", SECRET)
    monkeypatch.delenv("ALPACA_BASE_URL", raising=False)

    assert Alpaca.from_env().trading == TRADING_API


@pytest.mark.parametrize(
    "url, mode",
    [(TRADING_API, "paper"), (TRADING_API + "/", "paper"), (LIVE_TRADING_API, "live")],
)
def test_the_endpoint_of_the_mode_in_force_passes(url, mode):
    alpaca.check_mode(url, mode)


@pytest.mark.parametrize(
    "url, mode, endpoint",
    [(LIVE_TRADING_API, "paper", TRADING_API), (TRADING_API, "live", LIVE_TRADING_API)],
)
def test_the_endpoint_of_the_other_mode_is_refused_naming_both(url, mode, endpoint):
    why = f"ALPACA_BASE_URL is '{url}' but agb.current_mode is '{mode}', whose endpoint is "
    with pytest.raises(BaseUrlError, match=why + endpoint):
        alpaca.check_mode(url, mode)


def test_no_mode_on_record_is_refused():
    with pytest.raises(BaseUrlError, match="the trading mode is None"):
        alpaca.check_mode(TRADING_API, None)


# The execution service's start.


def environment(monkeypatch, db, url: str) -> None:
    for name, value in db.pg_env("execution").items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("ALPACA_API_KEY", KEY)
    monkeypatch.setenv("ALPACA_SECRET_KEY", SECRET)
    monkeypatch.setenv("ALPACA_BASE_URL", url)


def beats(db) -> list[tuple]:
    return db.connect("cli").execute("SELECT service FROM agb.heartbeat").fetchall()


def test_a_live_endpoint_in_paper_mode_refuses_to_start_before_the_first_heartbeat(db, monkeypatch):
    environment(monkeypatch, db, LIVE_TRADING_API)
    started = []
    monkeypatch.setattr(service, "serve", lambda *args: started.append(args))

    with pytest.raises(BaseUrlError, match=f"'{LIVE_TRADING_API}' but agb.current_mode is 'paper'"):
        service.main()

    assert started == []
    assert beats(db) == []


def test_the_paper_endpoint_in_paper_mode_starts(db, monkeypatch):
    environment(monkeypatch, db, TRADING_API + "/")
    started = []
    monkeypatch.setattr(service, "serve", lambda *args: started.append(args))

    service.main()

    [(_, _, _, execution)] = started
    assert execution.base_url == TRADING_API


def test_the_paper_endpoint_in_live_mode_refuses_to_start(db, monkeypatch):
    go_live(db)
    environment(monkeypatch, db, TRADING_API)
    monkeypatch.setattr(service, "serve", lambda *args: pytest.fail("the service started"))

    with pytest.raises(BaseUrlError, match=f"'{TRADING_API}' but agb.current_mode is 'live'"):
        service.main()


def test_a_mode_that_changes_under_a_running_service_ends_it(db):
    conn = db.connect("execution")
    running = Execution(broker(), base_url=TRADING_API)
    running.check_mode(conn)

    go_live(db)

    with pytest.raises(BaseUrlError, match="agb.current_mode is 'live'"):
        running(conn)


def test_a_broker_that_is_not_alpaca_s_has_no_endpoint_to_check(db):
    go_live(db)
    Execution(broker()).check_mode(db.connect("execution"))
