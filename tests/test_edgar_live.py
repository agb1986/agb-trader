"""Retrieval against the real SEC EDGAR. Skipped unless AGB_LIVE_EDGAR=1, with
EDGAR_USER_AGENT set to the operator's declared agent ("agb-trader <name> <email>"):

    AGB_LIVE_EDGAR=1 EDGAR_USER_AGENT="agb-trader <name> <email>" \
        uv run pytest -m live -s tests/test_edgar_live.py

It makes a few dozen requests at the client's own rate limit. It checks what the fake EDGAR
cannot: that the ticker list and submissions pages still have the shape the client reads, and
that a large filer's real 10-K cuts to non-trivial Items 1A and 7."""

import os
from datetime import date

import pytest

from agb import edgar

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_EDGAR") != "1", reason="set AGB_LIVE_EDGAR=1 to run"
    ),
]


def test_retrieves_a_large_filers_latest_filings():
    client = edgar.Edgar.from_env()

    result = edgar.retrieve(client, "AAPL", date.today())

    print(result.outcome, result.name, result.delinquent, result.detail)
    for section in result.sections:
        print(
            f"  {section.filing.form} {section.filing.filed} {section.section}: "
            f"{len(section.text):,} chars"
        )
    assert result.outcome == "retrieved"
    assert result.cik == "0000320193"
    assert result.delinquent is None and not result.ipo
    by_section = {(s.filing.form, s.section): s.text for s in result.sections}
    assert len(by_section[("10-K", "Item 1A")]) > 20_000
    assert len(by_section[("10-K", "Item 7")]) > 10_000
    assert "Risk Factors" in by_section[("10-K", "Item 1A")][:200]
    assert any(form == "10-Q" for form, _ in by_section)
