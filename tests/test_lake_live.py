"""The pull against the vendor's real API. Skipped unless AGB_LIVE_SHARADAR=1, with the key in
SHARADAR_API_KEY in the environment of the process running pytest:

    AGB_LIVE_SHARADAR=1 SHARADAR_API_KEY=... uv run pytest -m live

It lands the latest session the vendor has into a temporary directory, checks that pulling it
again changes nothing, and queries the result with DuckDB."""

import os
from datetime import UTC, datetime, timedelta

import pytest

from agb import lake, nyse

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("AGB_LIVE_SHARADAR") != "1", reason="set AGB_LIVE_SHARADAR=1 to run"
    ),
]


def latest_pull(root, client):
    today = nyse.trading_date(datetime.now(UTC))
    recent = nyse.NyseCalendar(today - timedelta(days=14), today).sessions(
        today - timedelta(days=14), today
    )
    for session in reversed(recent[-5:]):
        try:
            return session, lake.pull(root, client, session)
        except lake.NotYet:
            continue
    pytest.fail("the vendor has none of the last five sessions")


def test_a_real_pull_lands_and_is_idempotent(tmp_path):
    client = lake.Sharadar(os.environ.get("SHARADAR_API_KEY", ""))

    session, first = latest_pull(tmp_path, client)
    again = lake.pull(tmp_path, client, session)

    assert {p.path: p.sha256 for p in again} == {p.path: p.sha256 for p in first}
    db = lake.connect(tmp_path)
    count, latest = db.sql("SELECT count(*), max(date) FROM sep").fetchone()
    assert count > 1000
    assert latest == session
    assert db.sql("SELECT count(*) FROM tickers WHERE isdelisted = 'Y'").fetchone()[0] > 0
