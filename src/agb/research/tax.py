"""The UK after-tax diagnostic: an approximate capital gains tax bill for a set of trades, so
a backtest can report what the turnover costs after tax as well as before it.

The operator is a UK tax resident, so a strategy that rebalances every month realises nearly
all of its gains every year and pays tax on them every year, where buying an index and holding
it defers the whole bill to one disposal years later. That difference is large enough to change
which strategy is worth running, and it is invisible in a pre-tax return, so the backtest
report states it. The specification is explicit that tax does not enter certification: it
depends on the operator's other income, rate band and allowance usage, none of which are
properties of the strategy. Nothing here is read by `agb.research.certify` or by any gate, and
nothing here certifies anything.

What it computes is the specification's approximation, not HMRC's arithmetic. Each disposal's
allowable cost comes from a Section 104 average-cost pool per name, a year's losses net against
its gains in full, a surplus loss carries forward to relieve a later year's gains down to the
annual exempt amount, and what is left above the allowance is taxed at the higher 24% rate.
`SIMPLIFICATIONS` lists where that departs from a real computation, and the report carries that
list with the numbers so a reader cannot mistake one for the other. The honest CGT ledger —
HMRC's same-day and thirty-day matching, conversion to GBP at each trade's date, the rate band,
self-assessment output — is deferred until the build nears live, and when it arrives it belongs
in its own module against the live trade history, not here against a simulation.

The arithmetic is all `Decimal`, like every other money path in the project: a tax bill
accumulated over hundreds of disposals is exactly the place where a float's residue would show
up. The `view` function converts to float at the report boundary, because a report row is
serialised as JSON."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from agb import cost

CENT = Decimal("0.01")
# The higher rate on gains from 30 October 2024 and the allowance from 2024/25, both held
# constant across years: see SIMPLIFICATIONS.
RATE = Decimal("0.24")
EXEMPT_AMOUNT = Decimal("3000")
# The UK tax year runs from 6 April to the following 5 April.
YEAR_START_MONTH = 4
YEAR_START_DAY = 6

SIMPLIFICATIONS = (
    "the arithmetic is in USD: gains are not converted to GBP at each trade's date, so the"
    " allowance and the rate are applied to dollars as if they were pounds, and the currency"
    " gain the exchange rate adds or takes away is not taxed",
    "each name's cost basis is one Section 104 average-cost pool; HMRC's same-day and"
    " thirty-day matching rules are not applied, so a name sold and bought back within a"
    " month is pooled here where HMRC would match the two",
    "every gain above the allowance is taxed at 24%, with no basic-rate band and no other"
    " income, gains, losses or reliefs of the operator's in the calculation",
    "the 24% rate and the GBP 3,000 allowance are held constant across every tax year, where"
    " both have changed repeatedly and will change again",
    "the whole allowance is assumed free for this account and claimed in the year it falls;"
    " a carried-forward loss is assumed claimed in time and relieves only gains above the"
    " allowance, which is the order that wastes the least relief",
    "positions still open at the end are treated as disposed of at the final session's close"
    " with no dealing cost, so the bill covers the whole run and the buy-and-hold comparison"
    " is like for like",
    "dividends, the tax on them and the withholding on US dividends are outside this view, as"
    " they are outside the backtest",
)


class TaxError(Exception):
    """A set of trades that cannot be assessed, which means the caller built them wrongly."""


@dataclass(frozen=True)
class Trade:
    """One acquisition or disposal: whole shares at a price, with the dealing cost charged on
    it. A buy's cost is allowable against the pool and a sell's comes off the proceeds, which
    is how HMRC treats dealing costs."""

    day: date
    ticker: str
    side: cost.Side
    qty: int
    price: Decimal
    fee: Decimal


@dataclass(frozen=True)
class Disposal:
    """One disposal's chargeable gain: the proceeds net of the selling cost, less the share of
    the pool the shares carried."""

    day: date
    ticker: str
    qty: int
    proceeds: Decimal
    allowable_cost: Decimal

    @property
    def gain(self) -> Decimal:
        return self.proceeds - self.allowable_cost


@dataclass(frozen=True)
class Year:
    """One UK tax year's bill: the gains and losses realised in it, the brought-forward losses
    and the allowance set against them, and what the rate takes."""

    year: str
    disposals: int
    gains: Decimal
    losses: Decimal
    net: Decimal
    relieved: Decimal  # brought-forward losses used against this year's net gain
    exempt: Decimal  # the annual exempt amount used
    taxable: Decimal
    tax: Decimal
    carried: Decimal  # losses carried out of this year into the next


@dataclass(frozen=True)
class Assessment:
    """Every disposal and every tax year of a run, oldest first."""

    disposals: tuple[Disposal, ...]
    years: tuple[Year, ...]

    @property
    def tax(self) -> Decimal:
        return sum((year.tax for year in self.years), Decimal(0))


@dataclass
class _Pool:
    """A name's Section 104 pool: the shares held and the allowable cost they carry."""

    qty: int = 0
    cost: Decimal = Decimal(0)


def year_of(day: date) -> str:
    """The UK tax year a disposal on `day` falls in, as `2024/25`: the year starting on
    6 April."""
    start = day.year if (day.month, day.day) >= (YEAR_START_MONTH, YEAR_START_DAY) else day.year - 1
    return f"{start}/{(start + 1) % 100:02d}"


def realise(trades: Sequence[Trade]) -> list[Disposal]:
    """Every disposal in `trades`, in the order the trades are given, with the allowable cost
    each one takes out of its name's pool.

    The trades must be in the order they happened, because a pool is a running average: the
    caller's own order within a session is kept, so a sell that funds a buy on the same day is
    priced out of the pool before the buy goes into it, which is the order the engine fills
    them in."""
    pools: dict[str, _Pool] = {}
    disposals: list[Disposal] = []
    for trade in trades:
        pool = pools.setdefault(trade.ticker, _Pool())
        if trade.side is cost.Side.BUY:
            pool.qty += trade.qty
            pool.cost += trade.qty * trade.price + trade.fee
            continue
        if trade.qty > pool.qty:
            raise TaxError(
                f"{trade.ticker} sells {trade.qty} shares on {trade.day} from a pool of"
                f" {pool.qty}: a disposal cannot exceed the holding"
            )
        # The last disposal takes the whole remaining pool, so no rounded residue is left
        # behind to be carried into a later year's gain.
        share = (
            pool.cost
            if trade.qty == pool.qty
            else (pool.cost * trade.qty / pool.qty).quantize(CENT, rounding=ROUND_HALF_UP)
        )
        pool.qty -= trade.qty
        pool.cost -= share
        disposals.append(
            Disposal(
                day=trade.day,
                ticker=trade.ticker,
                qty=trade.qty,
                proceeds=(trade.qty * trade.price - trade.fee).quantize(
                    CENT, rounding=ROUND_HALF_UP
                ),
                allowable_cost=share,
            )
        )
    return disposals


def assess(trades: Sequence[Trade]) -> Assessment:
    """The tax year by year on `trades`.

    Gains and losses realised in the same year net in full, whatever order they fall in. A net
    loss is carried forward with no time limit; a brought-forward loss is set against a later
    year's net gain only so far as that gain is above the annual exempt amount, because relief
    taken below the allowance would be wasted. What survives both is taxed at the one rate."""
    disposals = realise(trades)
    by_year: dict[str, list[Disposal]] = {}
    for disposal in disposals:
        by_year.setdefault(year_of(disposal.day), []).append(disposal)
    years: list[Year] = []
    carried = Decimal(0)
    for year in sorted(by_year):
        gains = sum((d.gain for d in by_year[year] if d.gain > 0), Decimal(0))
        losses = sum((-d.gain for d in by_year[year] if d.gain < 0), Decimal(0))
        net = gains - losses
        above = max(net - EXEMPT_AMOUNT, Decimal(0))
        relieved = min(carried, above)
        net_after = net - relieved
        exempt = min(max(net_after, Decimal(0)), EXEMPT_AMOUNT)
        taxable = max(net_after - exempt, Decimal(0))
        carried = carried - relieved + max(-net, Decimal(0))
        years.append(
            Year(
                year=year,
                disposals=len(by_year[year]),
                gains=gains,
                losses=losses,
                net=net,
                relieved=relieved,
                exempt=exempt,
                taxable=taxable,
                tax=(taxable * RATE).quantize(CENT, rounding=ROUND_HALF_UP),
                carried=carried,
            )
        )
    return Assessment(disposals=tuple(disposals), years=tuple(years))


def view(trades: Sequence[Trade], init_cash: Decimal, final_value: Decimal) -> dict[str, Any]:
    """One side of the diagnostic as a report row: the bill on `trades` and what the run is
    worth once it is paid, given the pre-tax `final_value` the report already carries.

    The tax is taken off the end rather than at each year's end, which flatters the strategy a
    little by leaving the money compounding through the run; stating it is cheaper than
    modelling an outside account the backtest does not have. Floats, because a report row is
    serialised as JSON, and the arithmetic is over by here."""
    assessment = assess(trades)
    tax = assessment.tax
    return {
        "disposals": len(assessment.disposals),
        "realised_gains": float(sum((y.gains for y in assessment.years), Decimal(0))),
        "realised_losses": float(sum((y.losses for y in assessment.years), Decimal(0))),
        "taxable_gains": float(sum((y.taxable for y in assessment.years), Decimal(0))),
        "loss_carried_forward": float(assessment.years[-1].carried if assessment.years else 0),
        "tax": float(tax),
        "total_return": float(final_value / init_cash - 1),
        "final_value": float(final_value),
        "after_tax_final_value": float(final_value - tax),
        "after_tax_return": float((final_value - tax) / init_cash - 1),
        "years": [
            {
                "year": year.year,
                "disposals": year.disposals,
                "gains": float(year.gains),
                "losses": float(year.losses),
                "net": float(year.net),
                "relieved": float(year.relieved),
                "exempt": float(year.exempt),
                "taxable": float(year.taxable),
                "tax": float(year.tax),
                "carried": float(year.carried),
            }
            for year in assessment.years
        ],
    }
