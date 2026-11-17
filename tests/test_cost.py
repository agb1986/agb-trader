import dataclasses
import math
import subprocess
import sys
from decimal import Decimal

import pytest

from agb import cost
from agb.cost import CURRENT, Fees, Liquidity, Side

# The fingerprint of every cost-model version released so far, pinned here independently of
# the module. Add a line when a new version is released; never change or remove one.
RELEASED = {
    "1": "f883716f6f69eccfb547e77db8742102cbc0691a5a2a20b5f1f3d3eaee35168d",
}

WINDOW = CURRENT.window


def bars(offsets, steps, width=0.01):
    """Daily high, low and close from log-space offsets, for exact Abdi–Ranaldo estimates.

    The log mid-range η starts at ln 100 and moves by `steps[t]` from day t to day t+1, and
    day t closes `offsets[t]` above its own mid-range. The two-day estimate for days t and
    t+1 is then s² = 4 · offset_t · (offset_t − step_t).
    """
    eta = [math.log(100)]
    for step in steps:
        eta.append(eta[-1] + step)
    offsets = list(offsets) + [0.0] * (len(eta) - len(offsets))
    high = [math.exp(e + width) for e in eta]
    low = [math.exp(e - width) for e in eta]
    close = [math.exp(e + a) for e, a in zip(eta, offsets, strict=True)]
    return high, low, close


def floor_of(pairs):
    """The spread floor for 60 two-day estimates, each given as an (offset, step) pair."""
    offsets, steps = zip(*pairs, strict=True)
    return cost.spread_floor_bps(*bars(offsets, steps))


# The spec's illustrative magnitudes: a $5,000 order, one twentieth of the $100k certified.

ORDER_USD = 5_000.0
SMALLEST = Liquidity(model_version="1", floor_bps=15.0, sigma_daily=0.03, adv_usd=1_000_000.0)
TYPICAL = Liquidity(model_version="1", floor_bps=5.0, sigma_daily=0.02, adv_usd=20_000_000.0)


@pytest.mark.parametrize(("liq", "expected_bps"), [(SMALLEST, 36), (TYPICAL, 8)])
def test_illustrative_magnitudes_at_100k(liq, expected_bps):
    buy = cost.trade_cost(Side.BUY, ORDER_USD, 100, liq)
    sell = cost.trade_cost(Side.SELL, ORDER_USD, 100, liq)

    assert buy.total_bps == pytest.approx(expected_bps, abs=0.5)
    assert sell.market_bps == buy.market_bps
    assert sell.total_bps == pytest.approx(expected_bps, abs=0.5)
    assert sell.total_bps > buy.total_bps


def test_smallest_name_splits_into_floor_and_impact():
    trade = cost.trade_cost(Side.BUY, ORDER_USD, 100, SMALLEST)

    assert trade.floor_bps == 15.0
    assert trade.impact_bps == pytest.approx(0.03 * math.sqrt(0.005) * 10_000)
    assert trade.fees == Fees(Decimal(0), Decimal(0), Decimal(0), Decimal(0))
    assert trade.total_usd == pytest.approx(ORDER_USD * trade.total_bps / 10_000)
    assert trade.model_version == CURRENT.version


def test_impact_grows_with_the_square_root_of_participation():
    small = cost.trade_cost(Side.BUY, 1_000.0, 20, TYPICAL)
    large = cost.trade_cost(Side.BUY, 4_000.0, 80, TYPICAL)
    assert large.impact_bps == pytest.approx(2 * small.impact_bps)


def test_sell_fees_are_charged_in_bps_of_the_trade():
    trade = cost.trade_cost(Side.SELL, ORDER_USD, 100, TYPICAL)

    assert trade.fees.total == Decimal("0.11") + Decimal("0.02") + Decimal("0.0001")
    assert trade.fees_bps == pytest.approx(0.1301 / ORDER_USD * 10_000)
    assert trade.total_bps == pytest.approx(trade.market_bps + trade.fees_bps)


def test_trade_cost_refuses_liquidity_from_another_version():
    stale = dataclasses.replace(TYPICAL, model_version="0")
    with pytest.raises(ValueError, match="version '0'"):
        cost.trade_cost(Side.BUY, ORDER_USD, 100, stale)


@pytest.mark.parametrize(
    "liq",
    [dataclasses.replace(TYPICAL, adv_usd=0.0), dataclasses.replace(TYPICAL, sigma_daily=-0.01)],
)
def test_trade_cost_refuses_impossible_liquidity(liq):
    with pytest.raises(ValueError, match="ADV_"):
        cost.trade_cost(Side.BUY, ORDER_USD, 100, liq)


@pytest.mark.parametrize(("order_usd", "shares"), [(0, 100), (-5_000, 100), (5_000, 0)])
def test_trade_cost_refuses_empty_orders(order_usd, shares):
    with pytest.raises(ValueError, match="positive"):
        cost.trade_cost(Side.BUY, order_usd, shares, TYPICAL)


# The spread floor.


def test_negative_estimate_becomes_zero():
    # Day 0 closes above its own mid-range but below the next day's: s² < 0.
    high, low, close = bars([0.001, 0.001], [0.002, 0.0])
    negative, positive = cost.two_day_spreads(high, low, close)

    assert negative == 0.0
    assert positive == pytest.approx(0.002)


def test_known_spread_gives_half_of_it_as_the_floor():
    # s² = 4 · 0.0015² every day: a 30 bps spread, so a 15 bps floor.
    assert floor_of([(0.0015, 0.0)] * WINDOW) == pytest.approx(15.0)


def test_all_negative_estimates_floor_at_the_minimum():
    assert floor_of([(0.001, 0.002)] * WINDOW) == CURRENT.min_floor_bps == 2.0


def test_negative_estimates_count_as_zero_rather_than_being_dropped():
    positive = (0.001, 0.0)  # a 20 bps estimate
    negative = (0.001, 0.002)

    assert floor_of([positive] * WINDOW) == pytest.approx(10.0)
    # Dropping the negatives would leave a 20 bps median, and a 10 bps floor.
    assert floor_of([negative] * 31 + [positive] * 29) == 2.0


def test_small_positive_spread_floors_at_the_minimum():
    # A 2 bps spread halves to 1 bps, under the minimum.
    assert floor_of([(0.0001, 0.0)] * WINDOW) == 2.0


def test_floor_reads_only_the_trailing_window():
    wide = [(0.01, 0.0)] * 40  # 200 bps spreads, from before the window
    narrow = [(0.0015, 0.0)] * WINDOW
    assert floor_of(wide + narrow) == pytest.approx(15.0)


def test_floor_needs_a_full_window_of_estimates():
    with pytest.raises(ValueError, match=f"at least {WINDOW + 1} daily bars"):
        floor_of([(0.0015, 0.0)] * (WINDOW - 1))


# Volatility and dollar volume.


def closes_from_returns(returns, start=50.0):
    closes = [start]
    for r in returns:
        closes.append(closes[-1] * (1 + r))
    return closes


def test_daily_volatility_is_the_sample_stdev_of_trailing_returns():
    earlier = [0.3, -0.3] * 10  # outside the window, so ignored
    returns = [0.02, -0.02] * (WINDOW // 2)

    sigma = cost.daily_volatility(closes_from_returns(earlier + returns))

    assert sigma == pytest.approx(0.02 * math.sqrt(WINDOW / (WINDOW - 1)))


def test_daily_volatility_needs_a_full_window_of_returns():
    with pytest.raises(ValueError, match=f"at least {WINDOW + 1} daily bars"):
        cost.daily_volatility([50.0] * WINDOW)


def test_adv_is_the_trailing_median_dollar_volume():
    volume = [10_000_000.0] * 5 + [1_000.0 * n for n in range(1, WINDOW + 1)]
    close = [10.0] * len(volume)

    # The window holds 1,000 to 60,000 shares at $10; the median is 30,500 shares.
    assert cost.median_dollar_volume(close, volume) == 305_000.0


def test_adv_needs_a_full_window():
    with pytest.raises(ValueError, match=f"at least {WINDOW} daily bars"):
        cost.median_dollar_volume([10.0] * (WINDOW - 1), [1_000.0] * (WINDOW - 1))


def test_liquidity_gathers_the_three_inputs_under_the_version():
    high, low, close = bars([0.0015] * WINDOW, [0.01, -0.01] * (WINDOW // 2))
    volume = [100_000.0] * len(close)

    liq = cost.liquidity(high, low, close, volume)

    assert liq == Liquidity(
        model_version=CURRENT.version,
        floor_bps=cost.spread_floor_bps(high, low, close),
        sigma_daily=cost.daily_volatility(close),
        adv_usd=cost.median_dollar_volume(close, volume),
    )


def test_series_of_different_lengths_are_refused():
    high, low, close = bars([0.0015] * WINDOW, [0.0] * WINDOW)
    with pytest.raises(ValueError, match="same length"):
        cost.liquidity(high, low, close, [1_000.0] * (len(close) - 1))


# Inputs the model will not price from: a NaN or infinity anywhere, and a price of zero.


def _window():
    high, low, close = bars([0.0015] * WINDOW, [0.01, -0.01] * (WINDOW // 2))
    return {"high": high, "low": low, "close": close, "volume": [100_000.0] * len(close)}


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
@pytest.mark.parametrize("column", ["high", "low", "close", "volume"])
def test_liquidity_refuses_a_number_that_is_not_finite(column, bad):
    series = _window()
    series[column][-3] = bad
    with pytest.raises(cost.CostInputError, match=rf"{column}\[\d+\] is -?(nan|inf)"):
        cost.liquidity(**series)


@pytest.mark.parametrize("price", [0.0, -1.0])
@pytest.mark.parametrize("column", ["high", "low", "close"])
def test_liquidity_refuses_a_price_of_zero_or_below(column, price):
    # A zero once raised a bare `math domain error` from the log, or a ZeroDivisionError from
    # the return the day after; now it is refused by name, never priced around.
    series = _window()
    series[column][-3] = price
    with pytest.raises(cost.CostInputError, match="a price must be above zero"):
        cost.liquidity(**series)


def test_volatility_refuses_a_zero_close_it_would_divide_by():
    close = closes_from_returns([0.02, -0.02] * (WINDOW // 2))
    close[-2] = 0.0
    with pytest.raises(cost.CostInputError, match="above zero"):
        cost.daily_volatility(close)


def test_a_zero_volume_stands_and_a_negative_one_is_refused():
    series = _window()
    series["volume"][-3] = 0.0
    assert cost.liquidity(**series).adv_usd > 0  # one quiet day leaves the median standing
    series["volume"] = [0.0] * len(series["close"])
    with pytest.raises(ValueError, match=r"ADV_\$ must be positive"):  # refused as before
        cost.trade_cost(Side.BUY, ORDER_USD, 100, cost.liquidity(**series))
    series["volume"][-3] = -1.0
    with pytest.raises(cost.CostInputError, match="cannot be negative"):
        cost.liquidity(**series)


def test_a_bad_bar_before_the_window_is_not_read():
    # Only the window is priced from, as before: an old NaN that no estimate reads still stands.
    series = _window()
    for column in ("high", "low", "close", "volume"):
        series[column] = [math.nan, *series[column]]
    series["close"][0] = 0.0
    assert cost.liquidity(**series) == cost.liquidity(**_window())


@pytest.mark.parametrize("field", ["floor_bps", "sigma_daily", "adv_usd"])
@pytest.mark.parametrize("bad", [math.nan, math.inf])
def test_trade_cost_refuses_liquidity_that_is_not_finite(field, bad):
    liq = dataclasses.replace(TYPICAL, **{field: bad})
    with pytest.raises(cost.CostInputError, match=field):
        cost.trade_cost(Side.BUY, ORDER_USD, 100, liq)


@pytest.mark.parametrize(
    ("order_usd", "shares"),
    [(math.nan, 100), (ORDER_USD, math.nan), (math.inf, 100), (Decimal("NaN"), 100)],
)
@pytest.mark.parametrize("side", list(Side))
def test_trade_cost_refuses_an_order_that_is_not_finite(order_usd, shares, side):
    with pytest.raises(cost.CostInputError, match="not a finite number"):
        cost.trade_cost(side, order_usd, shares, TYPICAL)


@pytest.mark.parametrize(("principal", "shares"), [(math.nan, 100), (5_000, Decimal("NaN"))])
def test_fees_refuse_a_number_that_is_not_finite(principal, shares):
    with pytest.raises(cost.CostInputError, match="not a finite number"):
        cost.fees(Side.SELL, principal, shares)


def test_the_named_error_is_still_a_value_error():
    assert issubclass(cost.CostInputError, ValueError)


# The fee schedule.


@pytest.mark.parametrize(
    ("principal", "sec"),
    [
        (5_000, "0.11"),  # $0.103, rounded up
        (100_000, "2.06"),  # exactly $2.06; float arithmetic would round it up to $2.07
        (100_000.0, "2.06"),
        (Decimal("100000"), "2.06"),
        (1_000_000, "20.60"),
        (1, "0.01"),  # $0.0000206, rounded up
    ],
)
def test_sec_fee_rounds_up_to_the_cent(principal, sec):
    assert cost.fees(Side.SELL, principal, 1).sec == Decimal(sec)


@pytest.mark.parametrize(
    ("shares", "taf"),
    [
        (1, "0.01"),  # $0.000195, rounded up
        (1_000, "0.20"),  # $0.195, rounded up
        (50_000, "9.75"),  # under the cap
        (50_206, "9.79"),  # $9.79017, rounded up to exactly the cap
        (60_000, "9.79"),  # $11.70, capped
        (1_000_000, "9.79"),
    ],
)
def test_taf_rounds_up_to_the_cent_and_is_capped(shares, taf):
    assert cost.fees(Side.SELL, 10_000, shares).taf == Decimal(taf)


def test_cat_fee_is_per_share_and_unrounded():
    assert cost.fees(Side.SELL, 10_000, 1_000).cat == Decimal("0.001")
    assert cost.fees(Side.SELL, 10_000, 12.5).cat == Decimal("0.0000125")


def test_sell_fees_add_up():
    fee = cost.fees(Side.SELL, 5_000, 100)
    assert fee == Fees(
        sec=Decimal("0.11"), taf=Decimal("0.02"), cat=Decimal("0.0001"), commission=Decimal(0)
    )
    assert fee.total == Decimal("0.1301")


@pytest.mark.parametrize("side", [Side.BUY, "buy"])
def test_buys_carry_no_regulatory_fees(side):
    fee = cost.fees(side, 1_000_000, 60_000)
    assert fee == Fees(Decimal(0), Decimal(0), Decimal(0), Decimal(0))
    assert fee.total == 0


def test_commission_is_zero():
    assert CURRENT.commission == 0
    assert cost.fees(Side.SELL, 5_000, 100).commission == 0


def test_unknown_side_is_refused():
    with pytest.raises(ValueError):
        cost.fees("short", 5_000, 100)


# Versioning.


def test_released_versions_are_pinned():
    # Editing a released fingerprint to fit changed constants fails here.
    assert cost.VERSIONS == RELEASED
    assert CURRENT.fingerprint() == RELEASED[CURRENT.version]


def test_every_released_version_has_its_model_and_the_model_matches_its_fingerprint():
    # The registry fee checks read: a version in VERSIONS with no model, or a model whose
    # constants drifted from the fingerprint pinned for it, fails here.
    assert set(cost.MODELS) == set(RELEASED)
    for version, model in cost.MODELS.items():
        assert model.version == version
        assert model.fingerprint() == RELEASED[version]
    assert cost.MODELS[CURRENT.version] is CURRENT


def changed(value):
    if isinstance(value, bool):
        return not value
    if isinstance(value, str):
        return value + "-changed"
    if isinstance(value, int | float | Decimal):
        return value + type(value)("1")
    raise TypeError(f"no change defined for {type(value).__name__}; add one")


CONSTANTS = [f.name for f in dataclasses.fields(CURRENT) if f.name != "version"]


@pytest.mark.parametrize("name", CONSTANTS)
def test_changing_a_constant_without_a_new_version_is_refused(name):
    with pytest.raises(ValueError, match="new cost-model version"):
        dataclasses.replace(CURRENT, **{name: changed(getattr(CURRENT, name))})


@pytest.mark.parametrize("name", CONSTANTS)
def test_every_constant_is_in_the_fingerprint(name):
    assert name in CURRENT.constants()


def test_decimal_precision_counts_as_a_change():
    with pytest.raises(ValueError, match="new cost-model version"):
        dataclasses.replace(CURRENT, sec_fee_per_million=Decimal("20.6"))


def test_changed_constants_under_an_unreleased_version_are_refused():
    draft = cost.fingerprint(CURRENT.constants() | {"k": repr(1.5)})
    with pytest.raises(
        ValueError, match=f"'2' is not in VERSIONS; to release it, add '2': '{draft}'"
    ):
        dataclasses.replace(CURRENT, version="2", k=1.5)


def test_a_new_version_carries_changed_constants(monkeypatch):
    monkeypatch.setitem(
        cost.VERSIONS, "2", cost.fingerprint(CURRENT.constants() | {"k": repr(1.5)})
    )

    v2 = dataclasses.replace(CURRENT, version="2", k=1.5)

    assert v2.version == "2"
    liq = dataclasses.replace(TYPICAL, model_version="2")
    trade = cost.trade_cost(Side.BUY, ORDER_USD, 100, liq, v2)
    assert trade.model_version == "2"
    assert trade.impact_bps == pytest.approx(1.5 * 0.02 * math.sqrt(5_000 / 20e6) * 10_000)


# Isolation.


IMPORTS = """
import sys
before = set(sys.modules)
import agb.cost
print("\\n".join(set(sys.modules) - before))
"""


def test_imports_nothing_beyond_the_standard_library():
    # In a fresh interpreter, so nothing another test imported can hide a dependency.
    loaded = subprocess.run(
        [sys.executable, "-c", IMPORTS], check=True, capture_output=True, text=True
    ).stdout.split()
    outside_stdlib = {
        name
        for name in loaded
        if name.split(".")[0] not in sys.stdlib_module_names and not name.startswith("_")
    }
    assert outside_stdlib == {"agb", "agb.cost"}
