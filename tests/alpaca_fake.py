"""An in-memory broker in the shape of `agb.alpaca.Broker`, for the execution tests.

It behaves as Alpaca paper does at the edges the service cares about: a `client_order_id` it
has seen is refused with 422 and Alpaca's duplicate code, a fractional quantity on a
market-on-close order is refused, an order it does not hold is a 404, and a cancel of an order
that can no longer be cancelled is refused. A cancel of an id in `cancel_pending` is taken but
not carried out, leaving the order `pending_cancel`. `close` fills the accepted orders, those
still pending a cancel among them, as the closing auction would, in as many pieces as the
test asks for, and reports each piece as a FILL activity. `close_all` answers as Alpaca's 207
Multi-Status does, one entry per position, and a symbol in `close_fails` is an entry with a
failing status and the broker's message rather than an error of the call. The `on_submit`
hook runs before each order is accepted, so a test can look at the database at that moment,
open the halt latch, or lose the broker's answer; `on_close_all` does the same before a
close-all.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from itertools import count

from agb.alpaca import (
    DUPLICATE_CLIENT_ORDER_ID,
    Account,
    AlpacaError,
    Asset,
    Closed,
    Configuration,
    Fill,
    Order,
    Position,
    Quote,
)

QUOTED_AT = datetime(2026, 10, 1, 19, 34, 59, tzinfo=UTC)


def duplicate(client_order_id: str) -> AlpacaError:
    return AlpacaError(
        f"POST /v2/orders: HTTP 422: client_order_id must be unique ({client_order_id})",
        status=422,
        code=DUPLICATE_CLIENT_ORDER_ID,
    )


def refused(message: str) -> AlpacaError:
    return AlpacaError(f"POST /v2/orders: HTTP 422: {message}", status=422, code=40010000)


def not_found(what: str) -> AlpacaError:
    return AlpacaError(f"GET {what}: HTTP 404: not found", status=404, code=40410000)


@dataclass
class FakeBroker:
    symbols: tuple[str, ...]
    bid: Decimal = Decimal("19.99")
    ask: Decimal = Decimal("20.01")
    status: str = "ACTIVE"
    trading_blocked: bool = False
    account_blocked: bool = False
    suspend_trade: bool = False
    no_shorting: bool = False
    equity: Decimal = Decimal("100000")
    cash: Decimal = Decimal("100000")
    buying_power: Decimal = Decimal("200000")
    # Symbols whose orders the broker refuses, and broker order ids whose cancel fails.
    refuse: set[str] = field(default_factory=set)
    cancel_fails: set[str] = field(default_factory=set)
    # Broker order ids whose cancel is accepted but not yet carried out: the order is left
    # `pending_cancel`, so the closing auction can still fill it.
    cancel_pending: set[str] = field(default_factory=set)
    # When set, a change to the configuration is refused with this error.
    suspend_fails: AlpacaError | None = None
    # Symbols a close-all cannot close, with the broker's message for each, and an error
    # the whole close-all call fails with.
    close_fails: dict[str, str] = field(default_factory=dict)
    close_all_fails: AlpacaError | None = None
    on_close_all: Callable[["FakeBroker"], None] | None = None
    # Called as on_submit(broker, client_order_id) before an order is accepted.
    on_submit: Callable[["FakeBroker", str], None] | None = None
    calls: list[tuple] = field(default_factory=list)
    orders: dict[str, Order] = field(default_factory=dict)
    by_client: dict[str, str] = field(default_factory=dict)
    book: dict[str, Position] = field(default_factory=dict)
    activities: list[Fill] = field(default_factory=list)
    now: datetime = QUOTED_AT

    def __post_init__(self) -> None:
        self.assets = {s: Asset(s, "active", True, True) for s in self.symbols}
        self.quotes = {s: Quote(s, self.bid, self.ask, QUOTED_AT) for s in self.symbols}
        self._ids = count(1)

    # -- account -----------------------------------------------------------------------

    def account(self) -> Account:
        self.calls.append(("account",))
        return Account(
            self.status,
            self.trading_blocked,
            self.account_blocked,
            self.equity,
            self.cash,
            self.buying_power,
        )

    def configuration(self) -> Configuration:
        self.calls.append(("configuration",))
        return Configuration(self.suspend_trade, self.no_shorting)

    def suspend(self, suspend_trade: bool) -> Configuration:
        self.calls.append(("suspend", suspend_trade))
        if self.suspend_fails is not None:
            raise self.suspend_fails
        self.suspend_trade = suspend_trade
        return Configuration(self.suspend_trade, self.no_shorting)

    def asset(self, symbol: str) -> Asset:
        self.calls.append(("asset", symbol))
        if symbol not in self.assets:
            raise not_found(f"/v2/assets/{symbol}")
        return self.assets[symbol]

    def latest_quotes(self, symbols) -> dict[str, Quote]:
        self.calls.append(("latest_quotes", tuple(symbols)))
        return {s: self.quotes[s] for s in symbols if s in self.quotes}

    # -- orders ------------------------------------------------------------------------

    def submit(self, symbol: str, qty: Decimal, side: str, client_order_id: str) -> Order:
        self.calls.append(("submit", client_order_id))
        if self.on_submit is not None:
            self.on_submit(self, client_order_id)
        if client_order_id in self.by_client:
            raise duplicate(client_order_id)
        if symbol in self.refuse:
            raise refused(f"{symbol} is not tradable at this time")
        if qty != qty.to_integral_value():
            raise refused("fractional orders must be day orders")
        return self.accept(symbol, qty, side, client_order_id)

    def accept(self, symbol: str, qty: Decimal, side: str, client_order_id: str) -> Order:
        """Hold an order as if it had been submitted, without going through `submit`."""
        order = Order(
            id=f"ord-{next(self._ids):04d}",
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            qty=qty,
            status="accepted",
            filled_qty=Decimal(0),
            filled_avg_price=None,
            updated_at=self.now,
        )
        self.orders[order.id] = order
        self.by_client[client_order_id] = order.id
        return order

    def order_by_client_id(self, client_order_id: str) -> Order:
        self.calls.append(("order_by_client_id", client_order_id))
        if client_order_id not in self.by_client:
            raise not_found("/v2/orders:by_client_order_id")
        return self.orders[self.by_client[client_order_id]]

    def order(self, order_id: str) -> Order:
        self.calls.append(("order", order_id))
        if order_id not in self.orders:
            raise not_found(f"/v2/orders/{order_id}")
        return self.orders[order_id]

    def cancel(self, order_id: str) -> None:
        self.calls.append(("cancel", order_id))
        if order_id not in self.orders:
            raise not_found(f"/v2/orders/{order_id}")
        if order_id in self.cancel_fails or self.orders[order_id].terminal:
            raise AlpacaError(
                f"DELETE /v2/orders/{order_id}: HTTP 422: order is not cancelable",
                status=422,
                code=42210000,
            )
        self._set(
            order_id, status="pending_cancel" if order_id in self.cancel_pending else "canceled"
        )

    def _set(self, order_id: str, **changes) -> Order:
        order = self.orders[order_id]
        updated = Order(**{**order.__dict__, **changes, "updated_at": self.now})
        self.orders[order_id] = updated
        return updated

    # -- the close ---------------------------------------------------------------------

    def close(
        self,
        at: datetime,
        price: Decimal | None = None,
        *,
        pieces: dict[str, list[Decimal]] | None = None,
        short: dict[str, Decimal] | None = None,
    ) -> None:
        """The closing auction: every accepted order fills at `price` (the mid by default) at
        `at`, in the `pieces` the test names per symbol, or one piece. A symbol in `short`
        fills only that many shares and the order expires with the rest unfilled."""
        pieces = pieces or {}
        short = short or {}
        self.now = at
        for order_id, order in list(self.orders.items()):
            if order.terminal:
                continue
            fill_price = price if price is not None else self.quotes[order.symbol].mid
            wanted = short.get(order.symbol, order.qty)
            # A symbol short at zero fills nothing: no FILL activity, and the order expires.
            parts = pieces.get(order.symbol) or ([wanted] if wanted else [])
            assert sum(parts) == wanted, f"{order.symbol}: pieces do not add up to {wanted}"
            filled = Decimal(0)
            for part in parts:
                filled += part
                self.activities.append(
                    Fill(
                        id=f"act-{len(self.activities) + 1:04d}",
                        order_id=order_id,
                        symbol=order.symbol,
                        side=order.side,
                        qty=part,
                        price=fill_price,
                        at=at,
                        partial=filled < order.qty,
                    )
                )
                self._hold(order.symbol, part if order.side == "buy" else -part, fill_price)
            self._set(
                order_id,
                status="filled" if filled == order.qty else "expired",
                filled_qty=filled,
                filled_avg_price=fill_price if filled else None,
            )

    def _hold(self, symbol: str, delta: Decimal, price: Decimal) -> None:
        held = self.book.get(symbol)
        qty = (held.qty if held else Decimal(0)) + delta
        self.book[symbol] = Position(symbol, qty, price, qty * price)

    def positions(self) -> list[Position]:
        self.calls.append(("positions",))
        return [p for p in self.book.values() if p.qty != 0]

    def close_all(self) -> list[Closed]:
        """`DELETE /v2/positions?cancel_orders=true`: cancel every open order, then place a
        market order against each position, or report why it could not."""
        self.calls.append(("close_all",))
        if self.on_close_all is not None:
            self.on_close_all(self)
        if self.close_all_fails is not None:
            raise self.close_all_fails
        for order_id, order in list(self.orders.items()):
            if not order.terminal:
                self._set(order_id, status="canceled")
        closed = []
        for position in self.positions():
            if position.symbol in self.close_fails:
                closed.append(Closed(position.symbol, 403, None, self.close_fails[position.symbol]))
                continue
            side = "sell" if position.qty > 0 else "buy"
            order = self.accept(
                position.symbol, abs(position.qty), side, f"close-{position.symbol}"
            )
            closed.append(Closed(position.symbol, 200, order.id, None))
        return closed

    def fills(self, after: datetime) -> list[Fill]:
        self.calls.append(("fills", after))
        return [f for f in self.activities if f.at > after]

    # -- what the test asks ------------------------------------------------------------

    def submits(self) -> list[str]:
        return [c[1] for c in self.calls if c[0] == "submit"]
