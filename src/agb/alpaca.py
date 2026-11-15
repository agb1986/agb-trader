"""The broker: Alpaca's trading and market-data APIs, through the standard library.

The execution service is the only process that holds the keys, and this is the only module
that uses them. Every answer is turned into a plain value with the fields the service needs,
and every failure is an `AlpacaError` that carries the HTTP status and Alpaca's own error
code, so the service can tell a duplicate `client_order_id` (422, code 40010001) or an order
it cannot find (404) from a broken network. No error message ever carries a key. An answer
that parses as JSON but lacks a field the service needs, or carries one of the wrong shape, is
an `AlpacaError` too, with no status: the broker's answer cannot be read, so for a write it
is as if no answer came.

A read that does not get an answer the broker stands by -- a timeout, a dropped connection,
a 429 or a 5xx -- is attempted again, twice at most, with a doubling backoff and the
broker's own `Retry-After` when it sends one. Nothing else is: a write that times out may
have reached the broker, so it gets one attempt and the caller decides what to do with the
silence.

Alpaca sends numbers as strings, so quantities and prices are Decimals here. Fractional
quantities reach the broker as written; Alpaca accepts them on day orders only, and refuses a
fractional market-on-close order, which the service records as a rejection.
"""

import functools
import http.client
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

log = logging.getLogger(__name__)

TRADING_API = "https://paper-api.alpaca.markets"
DATA_API = "https://data.alpaca.markets"
TIMEOUT = 15.0
# How many extra attempts a read gets when the broker does not answer or answers that it could
# not serve the request. Reads only, so an attempt that may have reached the broker is never
# repeated. Three attempts of TIMEOUT each is the worst case for one read, well inside the
# five-minute staleness window the services are judged by.
RETRIES = 2
# The wait before the first retry, doubling for each one after it.
BACKOFF = 0.5
# A `Retry-After` longer than this is taken as the broker asking for more than a cycle's worth
# of patience: the call gives up and the service tries again on its own schedule.
RETRY_AFTER_CAP = 5.0
# The statuses worth another attempt. 429 is Alpaca's rate limit; the rest are its own
# failures, and none of them means the request was carried out.
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
# Alpaca's error code for a client_order_id it has seen before.
DUPLICATE_CLIENT_ORDER_ID = 40010001
# The market-on-close time in force.
MOC = "cls"
# Order states the broker changes no more.
TERMINAL = frozenset({"filled", "canceled", "expired", "rejected", "done_for_day"})
# Activities come in pages of at most this many.
PAGE = 100


class AlpacaError(Exception):
    """A call the broker refused or that never completed."""

    def __init__(
        self,
        message: str,
        status: int | None = None,
        code: int | None = None,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.status = status
        self.code = code
        # What the broker's `Retry-After` header asked for, in seconds, when it sent one.
        self.retry_after = retry_after

    @property
    def transient(self) -> bool:
        """The call did not get an answer the broker stands by, and repeating it reads the
        same state rather than doing anything twice. A refusal is not transient: the broker
        understood and said no, and asking again says no again."""
        return self.status is None or self.status in RETRY_STATUS

    @property
    def duplicate(self) -> bool:
        return self.status == 422 and self.code == DUPLICATE_CLIENT_ORDER_ID

    @property
    def not_found(self) -> bool:
        return self.status == 404

    @property
    def refused(self) -> bool:
        """The broker understood the request and said no, as opposed to not answering."""
        return self.status is not None and 400 <= self.status < 500 and self.status != 429


@dataclass(frozen=True)
class Account:
    status: str
    trading_blocked: bool
    account_blocked: bool
    equity: Decimal | None
    cash: Decimal | None
    buying_power: Decimal | None


@dataclass(frozen=True)
class Configuration:
    suspend_trade: bool
    no_shorting: bool


@dataclass(frozen=True)
class Asset:
    symbol: str
    status: str
    tradable: bool
    fractionable: bool


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: Decimal
    ask: Decimal
    at: datetime

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2


@dataclass(frozen=True)
class Order:
    id: str
    client_order_id: str
    symbol: str
    side: str
    qty: Decimal
    status: str
    filled_qty: Decimal
    filled_avg_price: Decimal | None
    updated_at: datetime | None

    @property
    def terminal(self) -> bool:
        return self.status in TERMINAL


@dataclass(frozen=True)
class Fill:
    """One execution, as the account's FILL activity reports it."""

    id: str
    order_id: str
    symbol: str
    side: str
    qty: Decimal
    price: Decimal
    at: datetime
    partial: bool


@dataclass(frozen=True)
class Position:
    symbol: str
    qty: Decimal
    avg_entry_price: Decimal | None
    market_value: Decimal | None


@dataclass(frozen=True)
class Closed:
    """One symbol of a close-all: the HTTP status Alpaca gave it inside the 207 Multi-Status
    answer, the closing order it placed when it could, and its message when it could not."""

    symbol: str
    status: int
    order_id: str | None
    message: str | None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class Broker(Protocol):
    """What the execution service asks of the broker. Alpaca answers it; tests fake it."""

    def account(self) -> Account: ...
    def configuration(self) -> Configuration: ...
    def suspend(self, suspend_trade: bool) -> Configuration: ...
    def asset(self, symbol: str) -> Asset: ...
    def latest_quotes(self, symbols: Sequence[str]) -> dict[str, Quote]: ...
    def submit(self, symbol: str, qty: Decimal, side: str, client_order_id: str) -> Order: ...
    def order_by_client_id(self, client_order_id: str) -> Order: ...
    def order(self, order_id: str) -> Order: ...
    def cancel(self, order_id: str) -> None: ...
    def positions(self) -> list[Position]: ...
    def fills(self, after: datetime) -> list[Fill]: ...
    def close_all(self) -> list[Closed]: ...


_FRACTION = re.compile(r"(\.\d{6})\d+")


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _seconds(value: Any) -> float | None:
    """A `Retry-After` header as seconds. The HTTP date form is not read: a broker that
    answers with a date is asking for a wait measured in minutes, which is longer than one
    call should hold a cycle open, and `None` leaves the plain backoff in charge."""
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def _when(value: Any) -> datetime | None:
    """An RFC 3339 instant, which Alpaca writes with up to nine fractional digits."""
    if not value:
        return None
    text = _FRACTION.sub(r"\1", str(value)).replace("Z", "+00:00")
    return datetime.fromisoformat(text).astimezone(UTC)


def _closed(raw: Any) -> list[Closed]:
    """The per-symbol entries of a close-all's 207 Multi-Status answer. Each carries the
    symbol, its own HTTP status, and a body that is the closing order when the status is a
    success and Alpaca's error otherwise."""
    closed = []
    for entry in raw or []:
        body = entry.get("body")
        body = body if isinstance(body, dict) else {}
        try:
            status = int(entry.get("status"))
        except (TypeError, ValueError):
            status = 0
        ok = 200 <= status < 300
        order_id = body.get("id") if ok else None
        message = None if ok else body.get("message")
        closed.append(
            Closed(
                symbol=str(entry.get("symbol") or body.get("symbol") or "?"),
                status=status,
                order_id=str(order_id) if order_id else None,
                message=str(message) if message else (None if ok else "no message"),
            )
        )
    return closed


def _order(raw: dict[str, Any]) -> Order:
    return Order(
        id=str(raw["id"]),
        client_order_id=str(raw.get("client_order_id") or ""),
        symbol=str(raw["symbol"]),
        side=str(raw["side"]),
        qty=_decimal(raw.get("qty")) or Decimal(0),
        status=str(raw["status"]),
        filled_qty=_decimal(raw.get("filled_qty")) or Decimal(0),
        filled_avg_price=_decimal(raw.get("filled_avg_price")),
        updated_at=_when(raw.get("updated_at")),
    )


# What reading a broker's answer raises when a field is missing or of the wrong shape: a key
# that is not there, a None or a list where an object was expected, a number that does not
# parse.
_MALFORMED = (KeyError, IndexError, TypeError, ValueError, AttributeError, ArithmeticError)


def _parsed(method):
    """Turn a malformed answer into an `AlpacaError` naming the call, so the service sees one
    kind of broker failure however the answer went wrong."""

    @functools.wraps(method)
    def wrapper(self: "Alpaca", *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        except _MALFORMED as exc:
            raise AlpacaError(
                self._scrub(f"{method.__name__}: the broker's answer is malformed: {exc!r}")
            ) from None

    return wrapper


class Alpaca:
    def __init__(
        self,
        key: str,
        secret: str,
        *,
        trading: str = TRADING_API,
        data: str = DATA_API,
        timeout: float = TIMEOUT,
        opener: urllib.request.OpenerDirector | None = None,
        retries: int = RETRIES,
        backoff: float = BACKOFF,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not key or not secret:
            raise ValueError("ALPACA_API_KEY and ALPACA_SECRET_KEY are not set")
        self._secrets = (key, secret)
        self._headers = {
            "APCA-API-KEY-ID": key,
            "APCA-API-SECRET-KEY": secret,
            "Accept": "application/json",
        }
        self._trading = trading.rstrip("/")
        self._data = data.rstrip("/")
        self._timeout = timeout
        self._retries = max(0, retries)
        self._backoff = backoff
        self._sleep = sleep
        # Execution reaches the broker directly, never through a proxy, whatever the
        # environment says.
        self._opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))

    @classmethod
    def from_env(cls) -> "Alpaca":
        return cls(
            os.environ.get("ALPACA_API_KEY", ""),
            os.environ.get("ALPACA_SECRET_KEY", ""),
            trading=os.environ.get("ALPACA_BASE_URL") or TRADING_API,
            data=os.environ.get("ALPACA_DATA_URL") or DATA_API,
        )

    @staticmethod
    def configured() -> bool:
        return bool(os.environ.get("ALPACA_API_KEY") and os.environ.get("ALPACA_SECRET_KEY"))

    def _scrub(self, text: str) -> str:
        for secret in self._secrets:
            text = text.replace(secret, "<secret>")
        return text

    def _call(
        self,
        method: str,
        base: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        """One request, and for a read one or two more when the first does not get an answer
        the broker stands by.

        Only `GET` is ever repeated. A `POST`, `PATCH` or `DELETE` that times out may have
        reached the broker and done what it asked for, and a second attempt would do it
        again, so those get exactly one attempt and the caller decides. For a read, the same
        call reads the same state, so repeating it is free of consequence and is what carries
        the service through a rate limit or a broker that answers 502 for a few seconds.
        """
        url = base + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = json.dumps(body).encode() if body is not None else None
        headers = dict(self._headers)
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        raw = self._read(request, method, path)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            raise AlpacaError(f"{method} {path}: the answer is not JSON") from None

    def _read(self, request: urllib.request.Request, method: str, path: str) -> bytes:
        """Attempt the request, and for a read attempt it again while it is worth it."""
        retries = self._retries if method == "GET" else 0
        wait = self._backoff
        for attempt in range(retries + 1):
            try:
                return self._attempt(request, method, path)
            except AlpacaError as exc:
                after = self._wait_for(exc, wait)
                if attempt == retries or not exc.transient or after is None:
                    raise
                log.warning("%s %s: %s; trying again in %.1fs", method, path, exc, after)
                self._sleep(after)
                wait *= 2
        raise AssertionError("unreachable: the loop returns or raises")

    @staticmethod
    def _wait_for(exc: AlpacaError, backoff: float) -> float | None:
        """How long to wait before trying again, or `None` to give up. The broker's own
        `Retry-After` wins when it sent one, because it knows when its rate limit resets;
        past `RETRY_AFTER_CAP` it is asking for longer than this call should hold a cycle
        open."""
        if exc.retry_after is None:
            return backoff
        return exc.retry_after if exc.retry_after <= RETRY_AFTER_CAP else None

    def _attempt(self, request: urllib.request.Request, method: str, path: str) -> bytes:
        # Every error is re-raised `from None`, so no traceback carries the request.
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            try:
                answer = json.loads(exc.read())
            except (OSError, ValueError, http.client.HTTPException):
                answer = {}
            message = answer.get("message") if isinstance(answer, dict) else None
            code = answer.get("code") if isinstance(answer, dict) else None
            raise AlpacaError(
                self._scrub(f"{method} {path}: HTTP {exc.code}: {message or 'no message'}"),
                status=exc.code,
                code=code if isinstance(code, int) else None,
                retry_after=_seconds(exc.headers.get("Retry-After") if exc.headers else None),
            ) from None
        except (OSError, ValueError, http.client.HTTPException) as exc:
            raise AlpacaError(self._scrub(f"{method} {path}: {exc}")) from None

    @_parsed
    def account(self) -> Account:
        raw = self._call("GET", self._trading, "/v2/account")
        return Account(
            status=str(raw.get("status", "")),
            trading_blocked=bool(raw.get("trading_blocked")),
            account_blocked=bool(raw.get("account_blocked")),
            equity=_decimal(raw.get("equity")),
            cash=_decimal(raw.get("cash")),
            buying_power=_decimal(raw.get("buying_power")),
        )

    @_parsed
    def configuration(self) -> Configuration:
        raw = self._call("GET", self._trading, "/v2/account/configurations")
        return Configuration(
            suspend_trade=bool(raw.get("suspend_trade")), no_shorting=bool(raw.get("no_shorting"))
        )

    @_parsed
    def suspend(self, suspend_trade: bool) -> Configuration:
        """Set or unset the account's `suspend_trade`, which blocks new orders at the broker.
        Returns the configuration the broker answers with."""
        raw = self._call(
            "PATCH",
            self._trading,
            "/v2/account/configurations",
            body={"suspend_trade": suspend_trade},
        )
        return Configuration(
            suspend_trade=bool(raw.get("suspend_trade")), no_shorting=bool(raw.get("no_shorting"))
        )

    @_parsed
    def asset(self, symbol: str) -> Asset:
        raw = self._call("GET", self._trading, f"/v2/assets/{urllib.parse.quote(symbol)}")
        return Asset(
            symbol=str(raw.get("symbol", symbol)),
            status=str(raw.get("status", "")),
            tradable=bool(raw.get("tradable")),
            fractionable=bool(raw.get("fractionable")),
        )

    @_parsed
    def latest_quotes(self, symbols: Sequence[str]) -> dict[str, Quote]:
        """The latest NBBO quote per symbol. A symbol with no quote is absent."""
        if not symbols:
            return {}
        raw = self._call(
            "GET", self._data, "/v2/stocks/quotes/latest", query={"symbols": ",".join(symbols)}
        )
        quotes = {}
        for symbol, q in (raw.get("quotes") or {}).items():
            bid, ask, at = _decimal(q.get("bp")), _decimal(q.get("ap")), _when(q.get("t"))
            if bid and ask and at and bid > 0 and ask > 0:
                quotes[symbol] = Quote(symbol, bid, ask, at)
        return quotes

    @_parsed
    def submit(self, symbol: str, qty: Decimal, side: str, client_order_id: str) -> Order:
        """Send a market-on-close order. A client_order_id the broker has seen raises an
        `AlpacaError` whose `duplicate` is true; the caller looks the order up instead."""
        body = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": "market",
            "time_in_force": MOC,
            "client_order_id": client_order_id,
        }
        return _order(self._call("POST", self._trading, "/v2/orders", body=body))

    @_parsed
    def order_by_client_id(self, client_order_id: str) -> Order:
        raw = self._call(
            "GET",
            self._trading,
            "/v2/orders:by_client_order_id",
            query={"client_order_id": client_order_id},
        )
        return _order(raw)

    @_parsed
    def order(self, order_id: str) -> Order:
        return _order(
            self._call("GET", self._trading, f"/v2/orders/{urllib.parse.quote(order_id)}")
        )

    def cancel(self, order_id: str) -> None:
        self._call("DELETE", self._trading, f"/v2/orders/{urllib.parse.quote(order_id)}")

    @_parsed
    def positions(self) -> list[Position]:
        raw = self._call("GET", self._trading, "/v2/positions") or []
        return [
            Position(
                symbol=str(p["symbol"]),
                qty=_decimal(p.get("qty")) or Decimal(0),
                avg_entry_price=_decimal(p.get("avg_entry_price")),
                market_value=_decimal(p.get("market_value")),
            )
            for p in raw
        ]

    @_parsed
    def close_all(self) -> list[Closed]:
        """Close every position at market, cancelling every open order first. Alpaca answers
        207 Multi-Status with one entry per symbol, and a symbol it could not close (a halted
        one, say) is an entry with a failing status, not an error of the call."""
        return _closed(
            self._call("DELETE", self._trading, "/v2/positions", query={"cancel_orders": "true"})
        )

    @_parsed
    def fills(self, after: datetime) -> list[Fill]:
        """Every execution after `after`, oldest first, across as many pages as it takes."""
        fills: list[Fill] = []
        query = {
            "after": after.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "direction": "asc",
            "page_size": str(PAGE),
        }
        while True:
            page = self._call("GET", self._trading, "/v2/account/activities/FILL", query=query)
            page = page or []
            for f in page:
                qty, price, at = (
                    _decimal(f.get("qty")),
                    _decimal(f.get("price")),
                    _when(f.get("transaction_time")),
                )
                if qty is None or price is None or at is None:
                    continue
                fills.append(
                    Fill(
                        id=str(f["id"]),
                        order_id=str(f.get("order_id") or ""),
                        symbol=str(f.get("symbol") or ""),
                        side=str(f.get("side") or ""),
                        qty=qty,
                        price=price,
                        at=at,
                        partial=f.get("type") == "partial_fill",
                    )
                )
            if len(page) < PAGE:
                return fills
            query["page_token"] = str(page[-1]["id"])
