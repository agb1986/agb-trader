"""Checks against Alpaca paper with the operator's keys: the account is reachable
and can trade, its configuration is readable, and the asset and quote endpoints answer
the questions the broker checks ask, and `suspend_trade` can be set and unset, as `/pause`
and `/resume` do. Nothing is submitted.

Opt in with AGB_LIVE_ALPACA=1 and the keys in the shell environment (ALPACA_API_KEY,
ALPACA_SECRET_KEY, and ALPACA_BASE_URL pointing at paper); the keys are read from the
environment only, never from a file in the repository.
"""

import os
from decimal import Decimal

import pytest

from agb.alpaca import Alpaca

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_ALPACA") != "1",
        reason="set AGB_LIVE_ALPACA=1 with ALPACA_API_KEY and ALPACA_SECRET_KEY in the shell",
    ),
]


@pytest.fixture(scope="module")
def alpaca() -> Alpaca:
    if not Alpaca.configured():
        pytest.skip("ALPACA_API_KEY and ALPACA_SECRET_KEY are not set")
    broker = Alpaca.from_env()
    assert "paper" in broker._trading, f"refusing to run against {broker._trading}"
    return broker


def test_the_paper_account_is_active_and_may_trade(alpaca):
    account = alpaca.account()
    assert account.status == "ACTIVE"
    assert not account.trading_blocked
    assert not account.account_blocked
    assert account.equity > 0
    assert account.buying_power > 0


def test_the_configuration_reports_suspend_trade(alpaca):
    configuration = alpaca.configuration()
    assert isinstance(configuration.suspend_trade, bool)
    assert isinstance(configuration.no_shorting, bool)


def test_a_listed_asset_is_tradable_and_fractionable(alpaca):
    asset = alpaca.asset("SPY")
    assert asset.symbol == "SPY"
    assert asset.status == "active"
    assert asset.tradable
    assert asset.fractionable


def test_the_latest_quote_gives_a_positive_nbbo_mid(alpaca):
    quotes = alpaca.latest_quotes(["SPY", "IWM"])
    assert set(quotes) == {"SPY", "IWM"}
    for quote in quotes.values():
        assert quote.bid > 0 and quote.ask >= quote.bid
        assert quote.mid == (quote.bid + quote.ask) / Decimal(2)
        assert quote.at.tzinfo is not None


def test_suspend_trade_is_set_read_back_and_unset(alpaca):
    """The broker half of `/pause` and `/resume`. The account is left as it was found."""
    before = alpaca.configuration().suspend_trade
    try:
        assert alpaca.suspend(True).suspend_trade is True
        assert alpaca.configuration().suspend_trade is True
        assert alpaca.suspend(False).suspend_trade is False
        assert alpaca.configuration().suspend_trade is False
    finally:
        alpaca.suspend(before)
