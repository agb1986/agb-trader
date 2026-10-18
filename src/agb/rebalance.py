"""The rebalance rules: which names a basket holds after a rebalance, at what weight, and the
orders that get it there.

Research and live both call this module, so a backtest rebalances exactly the way the live
pipeline does. It imports only the standard library. The rules are the ones in
docs/architecture/02-classical-ranker.md:

  * **Twenty positions, equal weight.** Each name is 1/20 of capital. The weight is fixed, so
    a month that runs fewer than twenty names leaves the rest in cash rather than spreading it.
  * **Buy from the top 20, hold until out of the top 30.** An incumbent ranked 21 to 30 is
    kept; one ranked below 30, or out of the ranking, is sold. The buffer halves turnover
    against a hard top-20 cut.
  * **Tie-breaks.** Slots are filled from the top of the ranking down. When the incumbents
    inside the buffer outnumber the slots, the lowest-ranked incumbent is dropped first.
  * **Fewer than twenty when a forced event requires it.** An incumbent that cannot trade on
    the signal session (a halt spanning the rebalance) is carried: it keeps its slot, gets no
    order and is not replaced, so the basket runs nineteen names that month. A ranking with
    too few tradeable names leaves slots empty.
  * **A name a forced event divests leaves whatever its rank.** A delisted stub and the
    acquirer shares a merger paid in were never chosen, so they are sold at the next
    rebalance even where the ranking would have kept them (`agb.corporate`).

Orders take a name in or out whole. Incumbents kept are not traded back to weight; drift is
left alone between rebalances, which is the spec's no-intra-month-action rule applied at the
rebalance too. Buys are sized from the capital the basket is run against, at the signal
session's close, in whole shares.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum

SIZE = 20
HOLD_THROUGH = 30


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class Selection:
    """The outcome of one rebalance: every name is in exactly one of the four groups."""

    keep: tuple[str, ...]  # incumbents kept, best rank first
    buy: tuple[str, ...]  # names entering, best rank first
    sell: tuple[str, ...]  # incumbents leaving, best rank first (unranked last)
    carry: tuple[str, ...]  # incumbents that cannot trade: kept in place, no order

    @property
    def names(self) -> tuple[str, ...]:
        """The basket after the rebalance: what is held, kept in place, and bought."""
        return (*self.keep, *self.carry, *self.buy)

    @property
    def held(self) -> frozenset[str]:
        return frozenset((*self.keep, *self.carry))


@dataclass(frozen=True)
class Order:
    symbol: str
    side: Side
    qty: int  # whole shares
    price: Decimal  # the close the order was sized at

    @property
    def notional(self) -> Decimal:
        return self.qty * self.price


def select(
    ranking: Sequence[str],
    held: Iterable[str],
    *,
    untradeable: Iterable[str] = (),
    divest: Iterable[str] = (),
    size: int = SIZE,
    hold_through: int = HOLD_THROUGH,
) -> Selection:
    """Apply the buffer and tie-break rules to a ranking, best first, and the book.

    `held` is the names in the book before the rebalance, `untradeable` those with no print
    on the signal session. An untradeable incumbent is carried; an untradeable name that is
    not held cannot be bought and is passed over. An empty book buys the top `size`.

    `divest` is the names a forced event sells whatever their rank: they are never kept,
    carried or bought, and one that is held is sold. A name in both is sold, because a stub
    with no print still has to leave the book at the price the event settled at.
    """
    if size > hold_through:
        raise ValueError(f"size {size} exceeds the hold-through rank {hold_through}")
    if len(set(ranking)) != len(ranking):
        raise ValueError("the ranking names a symbol twice")
    position = {symbol: i for i, symbol in enumerate(ranking)}
    by_rank = _by_rank(position)
    held, untradeable = frozenset(held), frozenset(untradeable)
    divest = frozenset(divest)
    passed_over = untradeable | divest

    carry = sorted((held & untradeable) - divest, key=by_rank)
    slots = max(size - len(carry), 0)
    keep = [s for s in ranking[:hold_through] if s in held and s not in passed_over]
    while len(keep) > slots:
        keep.pop()  # the lowest-ranked incumbent goes first
    sell = sorted(held - set(keep) - set(carry), key=by_rank)
    buy = [s for s in ranking if s not in held and s not in passed_over][: slots - len(keep)]
    return Selection(tuple(keep), tuple(buy), tuple(sell), tuple(carry))


def weights(names: Sequence[str], size: int = SIZE) -> dict[str, Decimal]:
    """Equal weights of 1/`size` each. A basket short of `size` names keeps the same weight per
    name and leaves the rest in cash."""
    if len(names) > size:
        raise ValueError(f"{len(names)} names for {size} slots")
    weight = Decimal(1) / Decimal(size)
    return {name: weight for name in names}


def orders(
    selection: Selection,
    weight: Mapping[str, Decimal],
    prices: Mapping[str, Decimal],
    positions: Mapping[str, Decimal | int],
    capital: Decimal,
) -> list[Order]:
    """The order plan: a buy of each entering name sized to its weight of `capital` at its
    price, in whole shares, and a sell of each leaving name's whole position. Sells come
    first, so the plan reads as the cash it raises before the cash it spends."""
    if capital <= 0:
        raise ValueError("capital must be positive")
    plan = []
    for symbol in selection.sell:
        qty = int(positions[symbol])
        if qty <= 0:
            raise ValueError(f"{symbol}: no position to sell")
        plan.append(Order(symbol, Side.SELL, qty, _price(prices, symbol)))
    for symbol in selection.buy:
        price = _price(prices, symbol)
        qty = int((capital * weight[symbol] / price).to_integral_value(rounding=ROUND_FLOOR))
        if qty <= 0:
            raise ValueError(f"{symbol}: {capital * weight[symbol]} buys no whole share at {price}")
        plan.append(Order(symbol, Side.BUY, qty, price))
    return plan


def _by_rank(position: Mapping[str, int]):
    """Sort key: rank order, with names outside the ranking last, by symbol."""
    return lambda symbol: (position.get(symbol, len(position)), symbol)


def _price(prices: Mapping[str, Decimal], symbol: str) -> Decimal:
    price = prices.get(symbol)
    if price is None or price <= 0:
        raise ValueError(f"{symbol}: no price on the signal session")
    return price
