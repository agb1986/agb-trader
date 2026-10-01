"""Close-all against Alpaca paper with the operator's keys: `DELETE /v2/positions?
cancel_orders=true` answers one entry per position held, and each entry parses. This
closes every position in the paper account, so it runs only when asked.

Opt in with AGB_LIVE_LIQUIDATE=1 and the keys in the shell environment (ALPACA_API_KEY,
ALPACA_SECRET_KEY, and ALPACA_BASE_URL pointing at paper); the keys are read from the
environment only, never from a file in the repository. It refuses to run against a live
account.
"""

import os

import pytest

from agb.alpaca import Alpaca

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_LIQUIDATE") != "1",
        reason="set AGB_LIVE_LIQUIDATE=1 with ALPACA_API_KEY and ALPACA_SECRET_KEY in the shell",
    ),
]


@pytest.fixture(scope="module")
def alpaca() -> Alpaca:
    if not Alpaca.configured():
        pytest.skip("ALPACA_API_KEY and ALPACA_SECRET_KEY are not set")
    broker = Alpaca.from_env()
    assert "paper" in broker._trading, f"refusing to run against {broker._trading}"
    return broker


def test_close_all_answers_each_position_held(alpaca):
    held = {p.symbol for p in alpaca.positions()}

    closed = alpaca.close_all()

    assert {c.symbol for c in closed} == held
    for entry in closed:
        assert entry.status > 0
        assert (entry.order_id is not None) if entry.ok else bool(entry.message)
