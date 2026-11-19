"""How many queries a page costs (#197).

The landing page, the decision log, ran about ten queries for every cycle it listed, and it
lists twenty-four: the diff (four), the month's equity readings (two or three), the audit lines,
the gate checks and the month's flags, one cycle at a time. A month's return read every equity
reading of the month to use the first and the last. The bot recomputed the promoted model's
thresholds and the equity months for each of the checks it ran on every beat.

The fix batches the per-cycle reads, reads two equity readings and not the month, and computes
the monthly aggregates once per beat. None of that is meant to change what an operator sees,
so the page and the `/pnl` text over this fixture were captured from the code as it stood
before the change and are held in tests/golden/ui_queries.json, and the query count the page
cost then, which the test holds the page's present cost against, is held there with them.

The pages are compared character for character apart from the instants the database stamped
itself while building the fixture, which are normalised: a run on another day would
otherwise differ. Every instant the fixture sets explicitly is compared as it is.

The bot's performance readings are held to the same standard: the Telegram text they produce
is in the golden file, and the beat reads the promoted thresholds and the equity months once.
A change that is meant to move the page or the text regenerates the golden file by hand, and
says in the change why it moved."""

import json
import re
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from test_bot import CHAT, FakeTelegram
from test_gate import NAMES, SOUND, TURNOVER, hold, insert, move, run, state
from test_health import distribution, mode, promote
from test_health import months as months_of_equity
from test_ui import alert, ranks

from agb import basket, health, readout, ui
from agb.nyse import NyseCalendar
from agb.services.bot import Bot, alert_text

GOLDEN = Path(__file__).parent / "golden" / "ui_queries.json"
# When the operator is looking: after October's close, with the month still running.
NOW = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)
# The cycles the log shows, and the months of equity readings behind them.
CYCLES = ui.LOG_CYCLES
READINGS_PER_MONTH = 20
# The instants the database stamps itself, in the fixture's own run, read as the day it ran.
STAMPED = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2} ET")
RUN_DAY = "2026-11-03"


class Counting(psycopg.Cursor):
    """A cursor that counts the statements it runs. `Connection.execute` opens a cursor, so it
    counts those too."""

    executed = 0

    def execute(self, *args, **kwargs):
        Counting.executed += 1
        return super().execute(*args, **kwargs)


def months(count):
    """The `count` months before November 2026, oldest first, as (period, first session)."""
    out = []
    year, month = 2026, 11
    for _ in range(count):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        out.append((f"{year}-{month:02d}", date(year, month, 1)))
    return out[::-1]


def session_of(first: date) -> date:
    """The first NYSE session on or after `first`."""
    return NyseCalendar(first, first + timedelta(days=10)).sessions(
        first, first + timedelta(days=10)
    )[0]


def release(db):
    """Close what the helpers opened. Each helper call connects afresh and the fixture closes
    them all when the test ends, which two dozen gated cycles would not leave room for."""
    while db._open:
        db._open.pop().close()


def history(db):
    """Twenty-four live cycles, oldest first: twenty-two sound months gated and, in turn,
    approved, filled or still waiting, then September's book and October's basket that breaches
    the turnover limit, with a month of equity readings behind each, ranks, alerts and a month
    with no equity readings at all. Every cycle has the diff, the gate result, the flags and the
    audit lines the log expands."""
    cycles = months(CYCLES)
    for index, (period, first) in enumerate(cycles[: CYCLES - 2]):
        session = session_of(first)
        target = replace(
            SOUND.target,
            period=period,
            session_date=session,
            snapshot_date=basket.signal_session(session),
        )
        signal_close = NyseCalendar(target.snapshot_date, session).close_at(target.snapshot_date)
        case = replace(SOUND, target=target, now=signal_close + timedelta(hours=1))
        basket_id = insert(db, target)
        ranks(db, basket_id, NAMES)
        run(db, basket_id, case)
        assert state(db, basket_id)[0] == "approved", period
        if index % 3 == 0:
            move(db, basket_id, 3, "approved", "submitted", "execution")
            move(db, basket_id, 4, "submitted", "filled", "execution")
        elif index % 3 == 1:
            move(db, basket_id, 3, "approved", "submitted", "execution")
        if index % 4 == 0:
            alert(db, "gate_breach", f"live basket {period}", period)
        if index % 5 == 0:
            alert(db, "stale_heartbeat", "scheduler", period, delivered=True)
        release(db)
    hold(db, TURNOVER.book)
    october = insert(db, TURNOVER.target)
    ranks(db, october, NAMES)
    run(db, october, TURNOVER)
    assert state(db, october)[0] == "halted"
    alert(db, "gate_breach", f"live basket {cycles[-1][0]}", cycles[-1][0], failures=2)
    # Readings through every month but one, which has none: its row must still say so.
    conn = db.connect("execution")
    with conn.cursor() as cur:
        for index, (_period, first) in enumerate(cycles):
            if index == 5:
                continue
            for reading in range(READINGS_PER_MONTH):
                at = datetime.combine(first, time(20), UTC) + timedelta(days=reading + 1)
                cur.execute(
                    "INSERT INTO agb.broker_state (observed_at, account_status, suspend_trade,"
                    " no_shorting, trading_blocked, account_blocked, equity_usd, cash_usd,"
                    " buying_power_usd, actor)"
                    " VALUES (%s, 'ACTIVE', false, false, false, false, %s, 0, 0, 'execution')",
                    (at, Decimal(100_000 + index * 700 + reading * 37)),
                )
    return cycles


def normalised(text: str) -> str:
    """The page with each instant from the day the fixture was built read as `RUN_DAY`: the
    database stamps those itself, and the explicit ones are all well before it."""
    return STAMPED.sub(
        lambda m: m.group() if m.group() < "2026-11-03" else f"{RUN_DAY} 00:00 ET", text
    )


def reading(db) -> psycopg.Connection:
    """The `ui` role, read-only, counting what it runs."""
    env = db.pg_env("ui")
    return psycopg.connect(
        host=env["PGHOST"],
        port=env["PGPORT"],
        dbname=env["PGDATABASE"],
        user=env["PGUSER"],
        password=env["PGPASSWORD"],
        options=ui.READ_ONLY,
        autocommit=True,
        cursor_factory=Counting,
    )


def measure(db):
    """The decision log and `/pnl` over every month of the fixture, and what the log cost."""
    with reading(db) as conn:
        Counting.executed = 0
        page = ui.log(conn, NOW)
        cost = Counting.executed
        texts = {
            period: readout.pnl(conn, period) for period in [*(p for p, _ in months(CYCLES)), None]
        }
    return {
        "queries": cost,
        "log": normalised(page),
        "pnl": {k or "all": v for k, v in texts.items()},
    }


def bot_cycle(db):
    """A promoted model every performance reading is configured for, and the months that trip
    the drawdown, the losing run, the tracking and the capital readings."""
    promote(
        db,
        distribution(drawdown=-0.12, losing_run=1, tracking=0.05, monthly=0.01),
        capital=Decimal(50000),
    )
    mode(db, "paper")
    months_of_equity(db, "0.05", "-0.10", "-0.05", "-0.02")


def performance_texts(db):
    """What the bot would send for the performance readings, as the Telegram text, and how many
    times the beat read the thresholds and the equity months to say it."""
    reads = {"thresholds": 0, "months": 0}
    real = health.thresholds, health.equity_months

    def thresholds(conn):
        reads["thresholds"] += 1
        return real[0](conn)

    def equity_months(conn):
        reads["months"] += 1
        return real[1](conn)

    health.thresholds, health.equity_months = thresholds, equity_months
    try:
        found = Bot(FakeTelegram(), CHAT, lambda: NOW).performance(db.connect("bot"))
    finally:
        health.thresholds, health.equity_months = real
    return {"texts": [alert_text(f) for f in found], "reads": reads}


@pytest.fixture
def cycles(db):
    return history(db)


def golden():
    return json.loads(GOLDEN.read_text())


def test_log_page_is_unchanged(db, cycles):
    assert measure(db)["log"] == golden()["log"]


def test_pnl_text_is_unchanged(db, cycles):
    assert measure(db)["pnl"] == golden()["pnl"]


def test_log_page_cost_fell_at_least_fivefold(db, cycles):
    before = golden()["queries"]
    after = measure(db)["queries"]
    print(f"decision log queries: {before} before, {after} after")
    assert after * 5 <= before


def test_log_page_cost_does_not_grow_with_cycles(db, cycles):
    """The cost is a handful of statements for any number of cycles, not a handful for each."""
    assert measure(db)["queries"] <= 20


def test_equity_window_reads_the_first_and_last_reading(db, cycles):
    with reading(db) as conn:
        window = readout.equity_window(conn, "2026-10")
        empty = readout.equity_window(conn, cycles[5][0])
        every = readout.equity_windows(conn, [p for p, _ in cycles])
    # The month is measured from the last reading before it, to its own last reading.
    before = datetime.combine(cycles[-2][1], time(20), UTC) + timedelta(days=READINGS_PER_MONTH)
    last = datetime.combine(cycles[-1][1], time(20), UTC) + timedelta(days=READINGS_PER_MONTH)
    assert window is not None
    assert (window[0], window[2]) == (before, last)
    assert empty is None
    assert every[cycles[5][0]] is None
    assert every["2026-10"] == window


def test_performance_texts_are_unchanged_and_aggregates_read_once(db):
    bot_cycle(db)
    result = performance_texts(db)
    assert result["texts"] == golden()["performance"]
    assert result["reads"] == {"thresholds": 1, "months": 1}
