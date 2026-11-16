"""The shared transaction cost model: one versioned definition of what a trade costs.

The vectorbt scan, the NautilusTrader validation run, the live order plan's frozen modelled
cost and the live shortfall view all call this module, so the backtest and live can never
disagree about what a trade should have cost. It imports only the standard library, which is
what lets research and the live services import it unchanged.

Every trade, buy or sell, is charged

    cost_bps = floor + k · σ_daily · √(order_$ / ADV_$)

and a sell also pays the regulatory fees. The spec is "The transaction cost model" in
docs/architecture/01-data-broker-backtesting.md.

Every constant lives on `CostModel`, under a version identifier. Changing any of them is a new
cost-model version, which counts as a trial and voids existing certifications. `VERSIONS` pins
each version to a fingerprint of its constants, and a `CostModel` whose constants do not match
its version's fingerprint cannot be constructed, so a constant cannot change without the
version changing too.
"""

import dataclasses
import hashlib
import json
import math
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from enum import StrEnum
from itertools import pairwise

BPS = 10_000
CENT = Decimal("0.01")

# Every cost-model version ever issued, pinned to the fingerprint of its constants. Append a
# new version; never edit or remove one, because certifications and frozen order costs name
# these versions.
VERSIONS: dict[str, str] = {
    "1": "f883716f6f69eccfb547e77db8742102cbc0691a5a2a20b5f1f3d3eaee35168d",
}


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class CostModel:
    """Every constant the cost depends on, fixed ex ante rather than tuned as parameters."""

    version: str
    # The spread estimator whose trailing median, halved, is the per-name floor.
    spread_estimator: str
    # Trading days in the trailing windows for the spread median, σ_daily and ADV_$.
    window: int
    # Lower bound on the floor, in bps.
    min_floor_bps: float
    # Square-root impact coefficient.
    k: float
    # SEC Section 31 fee, in dollars per $1M of principal, and how it rounds to the cent.
    sec_fee_per_million: Decimal
    sec_rounding: str
    # FINRA Trading Activity Fee, in dollars per share, its per-trade cap, and its rounding.
    taf_per_share: Decimal
    taf_cap: Decimal
    taf_rounding: str
    # Consolidated Audit Trail fee, in dollars per executed share, unrounded.
    cat_per_share: Decimal
    # Broker commission, in dollars per order.
    commission: Decimal

    def __post_init__(self) -> None:
        pinned = VERSIONS.get(self.version)
        if pinned is None:
            raise ValueError(
                f"cost-model version {self.version!r} is not in VERSIONS; to release it, add "
                f"{self.version!r}: {self.fingerprint()!r}"
            )
        if pinned != self.fingerprint():
            raise ValueError(
                f"constants differ from those pinned for cost-model version {self.version!r}; "
                "changing a constant is a new cost-model version, added to VERSIONS"
            )

    def constants(self) -> dict[str, str]:
        """Every field but the version, as text, so a type change counts as a change too."""
        return {
            f.name: repr(getattr(self, f.name))
            for f in dataclasses.fields(self)
            if f.name != "version"
        }

    def fingerprint(self) -> str:
        return fingerprint(self.constants())


def fingerprint(constants: dict[str, str]) -> str:
    """The SHA-256 of a set of constants, in the form `CostModel.constants` returns them."""
    canonical = json.dumps(constants, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


CURRENT = CostModel(
    version="1",
    spread_estimator="abdi-ranaldo-2017-two-day-median",
    window=60,
    min_floor_bps=2.0,
    k=1.0,
    sec_fee_per_million=Decimal("20.60"),
    sec_rounding=ROUND_CEILING,
    taf_per_share=Decimal("0.000195"),
    taf_cap=Decimal("9.79"),
    taf_rounding=ROUND_CEILING,
    cat_per_share=Decimal("0.000001"),
    commission=Decimal("0"),
)

# Every released cost model, by version: the constants behind each fingerprint in `VERSIONS`,
# which holds fingerprints only. A fill records the version that priced it, and checking its
# fees means recomputing them under that version's own constants, which only this registry
# can supply. Add each new version here beside its line in `VERSIONS`, and never edit or
# remove one. `CURRENT` is the newest.
MODELS: dict[str, CostModel] = {
    "1": CURRENT,
}


# Liquidity: a name's trailing-window inputs to the cost, from its daily bars.
#
# Each function reads the most recent `window` days (plus one where a day-over-day quantity
# needs it) of the bars it is given, oldest first. Point-in-time is the caller's job: pass
# only bars known when the cost is fixed, with prices adjusted for splits across the window.


@dataclass(frozen=True)
class Liquidity:
    """A name's cost inputs on one date, computed under one cost-model version."""

    model_version: str
    floor_bps: float
    sigma_daily: float
    adv_usd: float


def two_day_spreads(
    high: Sequence[float], low: Sequence[float], close: Sequence[float]
) -> list[float]:
    """Abdi–Ranaldo (2017) two-day effective-spread estimates, as fractions of price.

    For consecutive days t and t+1, s² = 4 (c_t − η_t)(c_t − η_{t+1}), where c is the log
    close and η the log mid-range, (log high + log low) / 2. A negative s² is set to zero
    before the square root, so there is one estimate per consecutive pair, oldest first.
    """
    _same_length(high, low, close)
    c = [math.log(x) for x in close]
    eta = [(math.log(h) + math.log(lo)) / 2 for h, lo in zip(high, low, strict=True)]
    return [
        math.sqrt(max(4 * (c[t] - eta[t]) * (c[t] - eta[t + 1]), 0.0)) for t in range(len(c) - 1)
    ]


def spread_floor_bps(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    model: CostModel = CURRENT,
) -> float:
    """Half the trailing median two-day spread estimate, in bps, and never below the minimum."""
    spreads = two_day_spreads(*_trailing(model.window + 1, high, low, close))
    return max(statistics.median(spreads) / 2 * BPS, model.min_floor_bps)


def daily_volatility(close: Sequence[float], model: CostModel = CURRENT) -> float:
    """The sample standard deviation of the trailing window's daily close-to-close returns."""
    (closes,) = _trailing(model.window + 1, close)
    return statistics.stdev(b / a - 1 for a, b in pairwise(closes))


def median_dollar_volume(
    close: Sequence[float], volume: Sequence[float], model: CostModel = CURRENT
) -> float:
    """ADV_$: the trailing window's median daily dollar volume, close times shares traded."""
    closes, volumes = _trailing(model.window, close, volume)
    return statistics.median(c * v for c, v in zip(closes, volumes, strict=True))


def liquidity(
    high: Sequence[float],
    low: Sequence[float],
    close: Sequence[float],
    volume: Sequence[float],
    model: CostModel = CURRENT,
) -> Liquidity:
    _same_length(high, low, close, volume)
    return Liquidity(
        model_version=model.version,
        floor_bps=spread_floor_bps(high, low, close, model),
        sigma_daily=daily_volatility(close, model),
        adv_usd=median_dollar_volume(close, volume, model),
    )


def _same_length(*series: Sequence[float]) -> None:
    if len({len(s) for s in series}) > 1:
        raise ValueError("daily series must all have the same length")


def _trailing(n: int, *series: Sequence[float]) -> list[Sequence[float]]:
    _same_length(*series)
    if len(series[0]) < n:
        raise ValueError(f"need at least {n} daily bars, got {len(series[0])}")
    return [s[len(s) - n :] for s in series]


# Fees and the per-trade cost.


@dataclass(frozen=True)
class Fees:
    """The dollar fees on one order or fill."""

    sec: Decimal
    taf: Decimal
    cat: Decimal
    commission: Decimal

    @property
    def total(self) -> Decimal:
        return self.sec + self.taf + self.cat + self.commission


def fees(
    side: Side,
    principal_usd: Decimal | float,
    shares: Decimal | float,
    model: CostModel = CURRENT,
) -> Fees:
    """The fee schedule. Regulatory fees fall on sells only; commission on every order."""
    principal, qty = _decimal(principal_usd), _decimal(shares)
    if principal <= 0 or qty <= 0:
        raise ValueError("principal and shares must be positive")
    if Side(side) is Side.BUY:
        return Fees(Decimal(0), Decimal(0), Decimal(0), model.commission)
    sec = (principal * model.sec_fee_per_million / 1_000_000).quantize(
        CENT, rounding=model.sec_rounding
    )
    taf = min(
        (qty * model.taf_per_share).quantize(CENT, rounding=model.taf_rounding), model.taf_cap
    )
    return Fees(sec=sec, taf=taf, cat=qty * model.cat_per_share, commission=model.commission)


@dataclass(frozen=True)
class TradeCost:
    """The modelled cost of one trade, and the cost-model version that produced it."""

    model_version: str
    side: Side
    order_usd: float
    floor_bps: float
    impact_bps: float
    fees: Fees

    @property
    def market_bps(self) -> float:
        """Floor plus impact: what the trade costs on either side, before fees."""
        return self.floor_bps + self.impact_bps

    @property
    def fees_bps(self) -> float:
        return float(self.fees.total) / self.order_usd * BPS

    @property
    def total_bps(self) -> float:
        return self.market_bps + self.fees_bps

    @property
    def total_usd(self) -> float:
        return self.order_usd * self.total_bps / BPS


def trade_cost(
    side: Side,
    order_usd: Decimal | float,
    shares: Decimal | float,
    liq: Liquidity,
    model: CostModel = CURRENT,
) -> TradeCost:
    """The cost of one trade, where `order_usd` is the trade's notional, not the position's."""
    if liq.model_version != model.version:
        raise ValueError(
            f"liquidity was computed under cost-model version {liq.model_version!r}, "
            f"not {model.version!r}"
        )
    if liq.adv_usd <= 0 or liq.sigma_daily < 0:
        raise ValueError("ADV_$ must be positive and σ_daily non-negative")
    notional = float(order_usd)
    fee = fees(side, order_usd, shares, model)
    impact = model.k * liq.sigma_daily * math.sqrt(notional / liq.adv_usd)
    return TradeCost(
        model_version=model.version,
        side=Side(side),
        order_usd=notional,
        floor_bps=liq.floor_bps,
        impact_bps=impact * BPS,
        fees=fee,
    )


def _decimal(value: Decimal | float) -> Decimal:
    # Through str, so a float such as 0.1 becomes Decimal("0.1") and not its binary expansion.
    return value if isinstance(value, Decimal) else Decimal(str(value))
