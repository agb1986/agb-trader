"""The performance and cost readings (docs/architecture/04): what each one measures, where
its threshold comes from, that a threshold the certification never recorded leaves its check
silent, and what the bot says about a reading past one. The health checks around them are
exercised where the thing they watch is: tests/test_bot.py for the stale heartbeat and the
overdue event, tests/test_pause_resume.py for the broker mismatch.

Every alert here is a sentence and nothing else. The tests assert that too: the message says
what was not done, names the two levers as the operator's, and never advises."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from psycopg.types.json import Jsonb
from test_bot import CHAT, FakeTelegram

from agb import cost, health
from agb.roles import OWNER
from agb.services import bot as bot_service
from agb.services.bot import Bot

MODEL = "alerts-fake"
CAPITAL = Decimal(100000)
# 16:00 New York on the first session of October, when the fills print and the bot runs.
FILLED = datetime(2026, 10, 1, 20, 0, tzinfo=UTC)
NOW = datetime(2026, 10, 2, 20, 0, tzinfo=UTC)
# The mode in force before the first equity reading, so every month reads one.
FROM = datetime(2025, 12, 1, tzinfo=UTC)

_CERTIFY = """
    INSERT INTO agb.model_certification (
        model_id, certifies, passed, artifact_sha256, artifact_path, ledger_sha256, features,
        train_start, train_end, declared_trials, dsr, pbo, sharpe_net, dsr_2x,
        break_even_multiple, sharpe_gross, gross_sharpe_flag, participation_max,
        participation_mean, cost_model_version, certified_capital, path_quantiles, gates,
        certified_at, actor)
    VALUES (%(model_id)s, 'stage 1 (classical ranker) only', true, %(artifact)s,
            %(path)s, %(ledger)s, %(features)s, '2010-01-01', '2025-12-31', 1, 0.97, 0.1,
            0.8, 0.96, 3.0, 1.0, false, 0.001, 0.0005, %(cost_model_version)s,
            %(certified_capital)s, %(path_quantiles)s, '{}', %(certified_at)s, 'research')
    RETURNING id
"""
_COMMAND = """
    INSERT INTO agb.command (actor, surface, raw_text, verb, outcome)
    VALUES ('operator', 'cli', %s, %s, 'accepted') RETURNING id
"""
_STATE = """
    INSERT INTO agb.broker_state (observed_at, account_status, suspend_trade, no_shorting,
        trading_blocked, account_blocked, equity_usd, actor)
    VALUES (%s, 'ACTIVE', false, false, false, false, %s, 'execution')
"""
_BASKET = """
    INSERT INTO agb.basket (reference, period, session_date, signal_date, snapshot_date,
        partition_hash, universe_size, ranker, cost_model_version, capital_usd, actor)
    VALUES ('2026-10', '2026-10', '2026-10-01', '2026-09-30', '2026-09-30', %s, 20,
            %s, %s, %s, 'pipeline')
    RETURNING id
"""
_BASKET_ORDER = """
    INSERT INTO agb.basket_order (basket_id, client_order_id, symbol, side, qty,
        reference_price, notional_usd, cost_model_version, floor_bps, impact_bps, fees_usd,
        modelled_cost_bps)
    VALUES (%(basket)s, %(client)s, %(symbol)s, 'buy', 1, 100, 100, %(version)s, 1, 1, 0,
            %(modelled)s)
    RETURNING id
"""
_BROKER_ORDER = """
    INSERT INTO agb.broker_order (basket_id, basket_order_id, client_order_id, symbol, side,
        qty, cost_model_version, modelled_cost_bps, nbbo_bid, nbbo_ask, nbbo_mid, nbbo_at,
        actor)
    VALUES (%(basket)s, %(basket_order)s, %(client)s, %(symbol)s, 'buy', 1, %(version)s,
            %(modelled)s, 99, 101, 100, %(at)s, 'execution')
    RETURNING id
"""
_FILL = """
    INSERT INTO agb.fill (order_id, execution_id, qty, price, filled_at, fee_model_version,
        actor)
    VALUES (%s, %s, 1, %s, %s, %s, 'execution')
"""
_DIVIDEND = """
    INSERT INTO agb.dividend (period, symbol, amount_usd, paid_on, detail, actor, surface,
        command_id)
    VALUES (%s, %s, %s, %s, 'broker statement', 'operator', 'cli', %s)
"""


def distribution(**limits) -> dict:
    """A path distribution shaped as agb.research.gates.path_quantiles records one, holding
    only the quantiles the test needs: a certification that recorded none of them must leave
    every check that reads them silent."""
    quantiles: dict = {"paths": 240, "quantiles": [0.05, 0.5, 0.95]}
    if "drawdown" in limits:
        worst = limits["drawdown"]
        quantiles["max_drawdown"] = {"q05": worst, "min": worst * 1.5}
    if "losing_run" in limits:
        longest = limits["losing_run"]
        quantiles["longest_losing_run"] = {"q95": longest, "max": longest + 2}
    if "tracking" in limits:
        widest = limits["tracking"]
        quantiles["tracking_dispersion"] = {"q95": widest, "max": widest * 1.5}
    if "monthly" in limits:
        quantiles["monthly_return"] = {"q50": limits["monthly"]}
    return quantiles


def promote(db, quantiles: dict | None = None, capital: Decimal = CAPITAL) -> int:
    """A certification holding `quantiles` as its path distribution, promoted as the research
    import and `agb promote` record the pair. Returns its certification id."""
    research = db.connect("research")
    [(certification_id,)] = research.execute(
        _CERTIFY,
        {
            "model_id": MODEL,
            "artifact": "ab" * 32,
            "path": f"/research/certifications/{MODEL}",
            "ledger": "cd" * 32,
            "features": Jsonb(["mom_12_1"]),
            "cost_model_version": cost.CURRENT.version,
            "certified_capital": capital,
            "path_quantiles": Jsonb(quantiles if quantiles is not None else {}),
            "certified_at": datetime(2026, 1, 5, tzinfo=UTC),
        },
    ).fetchall()
    conn = db.connect("cli")
    with conn.transaction():
        [(command_id,)] = conn.execute(_COMMAND, (f"agb promote {MODEL}", "promote")).fetchall()
        conn.execute(
            "INSERT INTO agb.promotion_event (certification_id, command_id, actor)"
            " VALUES (%s, %s, 'operator')",
            (certification_id, command_id),
        )
    return certification_id


def mode(db, which: str, at: datetime = FROM) -> None:
    """The mode in force from `at`, as `agb mode` records one. Only the owner can record it
    for another time, and going live carries its attestation and its command."""
    conn = db.connect(OWNER)
    with conn.transaction():
        [(command_id,)] = conn.execute(_COMMAND, (f"agb mode {which}", "mode")).fetchall()
        conn.execute(
            "INSERT INTO agb.mode_event (mode, actor, at, reason, attestation, command_id)"
            " VALUES (%s, 'cli', %s, 'test', %s, %s)",
            (which, at, None if which == "paper" else "the paper record is sound", command_id),
        )


def equity(db, *readings: tuple[datetime, Decimal]) -> None:
    """Account equity as execution observed it, oldest first."""
    conn = db.connect("execution")
    for at, value in readings:
        conn.execute(_STATE, (at, value))


def months(db, *returns: str, start: Decimal = CAPITAL) -> None:
    """One reading at the start of January 2026 and one at the end of each month after it,
    so the account's New York months return `returns` in order."""
    value = start
    readings = [(datetime(2026, 1, 2, 21, 0, tzinfo=UTC), value)]
    for index, fraction in enumerate(returns):
        value = (value * (1 + Decimal(fraction))).quantize(Decimal("0.01"))
        readings.append((datetime(2026, 1 + index, 28, 21, 0, tzinfo=UTC), value))
    equity(db, *readings)


def dividend(db, period: str, symbol: str, amount: str, paid_on: str) -> None:
    """A dividend the operator recorded, as the CLI records one."""
    conn = db.connect("cli")
    with conn.transaction():
        [(command_id,)] = conn.execute(
            _COMMAND, (f"agb dividend {symbol} {amount}", "dividend")
        ).fetchall()
        conn.execute(_DIVIDEND, (period, symbol, Decimal(amount), paid_on, command_id))


def fills(db, shortfalls: list[str], modelled: str = "10", at: datetime = FILLED) -> None:
    """One order per shortfall, each filled once against a mid of 100 by that many basis
    points, beside a modelled cost of `modelled` basis points on the same order."""
    conn = db.connect(OWNER)
    version = cost.CURRENT.version
    [(basket_id,)] = conn.execute(_BASKET, ("ab" * 32, MODEL, version, CAPITAL)).fetchall()
    for index, shortfall in enumerate(shortfalls):
        symbol, client = f"N{index:03d}", f"2026-10-N{index:03d}"
        order = {
            "basket": basket_id,
            "client": client,
            "symbol": symbol,
            "version": version,
            "modelled": Decimal(modelled),
        }
        [(basket_order_id,)] = conn.execute(_BASKET_ORDER, order).fetchall()
        [(order_id,)] = conn.execute(
            _BROKER_ORDER, {**order, "basket_order": basket_order_id, "at": at}
        ).fetchall()
        price = Decimal(100) + Decimal(shortfall) / 100
        conn.execute(_FILL, (order_id, f"x-{index}", price, at, version))


def view(db, query: str, *params):
    return db.connect("bot").execute(query, params).fetchall()


def conn(db):
    return db.connect("bot")


# The drawdown, the losing run and the tracking dispersion.


def test_the_drawdown_alert_fires_once_the_fall_is_past_the_certified_fifth_percentile(db):
    """Acceptance criterion 1: the threshold is the fifth percentile of the certified paths'
    own worst falls, and the alert names both."""
    promote(db, distribution(drawdown=-0.12))
    mode(db, "paper")
    months(db, "0.05", "-0.10", "-0.05")

    [finding] = health.drawdown(conn(db))
    assert (finding.kind, finding.subject, finding.period) == ("drawdown", "equity", "2026-03")
    assert finding.what == "drawdown -14.50%, past the certified range"
    assert finding.facts == (
        "worst fall from peak -14.50%, through 2026-03; -12.00% at the 5th percentile of "
        "240 certified paths, and -18.00% at their deepest; "
        "live is outside the range validation explored."
    )


def test_a_drawdown_inside_the_certified_range_says_nothing(db):
    promote(db, distribution(drawdown=-0.20))
    mode(db, "paper")
    months(db, "0.05", "-0.10", "-0.05")

    assert health.drawdown(conn(db)) == []


def test_the_losing_run_alert_counts_consecutive_losing_months(db):
    promote(db, distribution(losing_run=2))
    mode(db, "paper")
    months(db, "-0.01", "-0.01", "0.01", "-0.01", "-0.01", "-0.01")

    [finding] = health.losing_run(conn(db))
    assert (finding.kind, finding.subject, finding.period) == ("losing_run", "equity", "2026-06")
    assert finding.what == "losing run 3 months, past the certified range"
    assert finding.facts == (
        "3 consecutive losing months, through 2026-06; 2 at the 95th percentile of "
        "240 certified paths, and 4 at their longest; "
        "live is outside the range validation explored."
    )
    # The same months, against paths that ran longer: a run inside the range is not an alert.
    assert health.losing_run(conn(db)) == health.losing_run(conn(db))


def test_a_losing_run_no_longer_than_the_certified_paths_ran_says_nothing(db):
    promote(db, distribution(losing_run=3))
    mode(db, "paper")
    months(db, "-0.01", "-0.01", "0.01", "-0.01", "-0.01", "-0.01")

    assert health.losing_run(conn(db)) == []


def test_tracking_measures_the_scatter_around_the_backtest_expected_month(db):
    promote(db, distribution(tracking=0.05, monthly=0.01))
    mode(db, "paper")
    months(db, "0.10", "-0.08", "0.12")

    [finding] = health.tracking(conn(db))
    assert (finding.kind, finding.subject, finding.period) == ("tracking", "equity", "2026-03")
    assert finding.what == "tracking 11.02%, past the certified range"
    assert finding.facts.startswith(
        "monthly returns scatter 11.02% around the expected 1.00% over 3 months; "
        "5.00% at the 95th percentile of 240 certified paths, and 7.50% at their widest; "
        "live is outside the range validation explored."
    )


def test_tracking_needs_two_months_to_measure_a_dispersion(db):
    promote(db, distribution(tracking=0.0001, monthly=0.01))
    mode(db, "paper")
    months(db, "0.10")

    assert health.tracking(conn(db)) == []


def test_the_recorded_paper_dividends_are_added_back_and_the_alert_says_so(db):
    """Acceptance criterion 3: a paper month is compared against the backtest's expectation
    with the dividends the paper broker never credited added back, and the alert says how
    much was added, because a reader who knows the paper broker credits none will wonder."""
    promote(db, distribution(tracking=0.05, monthly=0.01))
    mode(db, "paper")
    months(db, "0.10", "-0.08", "0.12")
    dividend(db, "2026-02", "N01", "250.00", "2026-02-20")
    dividend(db, "2026-02", "N02", "150.00", "2026-02-27")

    [(period, dividends, realised, total)] = view(
        db,
        "SELECT period, dividends_usd, return_fraction, total_return_fraction"
        " FROM agb.equity_month WHERE period = '2026-02'",
    )
    assert (period, dividends) == ("2026-02", Decimal("400.00"))
    assert round(realised, 6) == Decimal("-0.080000")
    assert round(total, 6) == Decimal("-0.076364")

    [finding] = health.tracking(conn(db))
    assert finding.facts.endswith("$400.00 of recorded dividends added back over 3 paper months.")


def test_a_live_month_is_not_credited_with_a_recorded_dividend(db):
    """Live equity already holds the cash, so adding it again would double it."""
    mode(db, "live")
    months(db, "0.10")
    dividend(db, "2026-01", "N01", "250.00", "2026-01-20")

    [(mode_, realised, total)] = view(
        db,
        "SELECT mode, return_fraction, total_return_fraction FROM agb.equity_month"
        " WHERE period = '2026-01'",
    )
    assert mode_ == "live"
    assert realised == total


# The live cost of a fill against the cost the model charged for it.


def test_the_cost_alert_waits_for_fifty_fills(db):
    promote(db)
    mode(db, "live")
    fills(db, ["15", "25"] * 24, modelled="10")

    [(count,)] = view(db, "SELECT fills FROM agb.live_cost")
    assert count == 48
    assert health.live_cost(conn(db)) == []


def test_the_cost_alert_fires_when_the_excess_is_past_two_standard_errors(db):
    """Acceptance criterion 2: the live shortfall against the frozen modelled cost, read
    against the error of its own mean rather than against a round number."""
    promote(db)
    mode(db, "live")
    fills(db, ["15", "25"] * 25, modelled="10")

    [finding] = health.live_cost(conn(db))
    assert (finding.kind, finding.subject, finding.period) == ("live_cost", "shortfall", "2026-10")
    assert finding.what == "live cost 10.0 bps above the modelled cost"
    assert finding.facts == (
        "mean shortfall 20.0 bps against 10.0 bps modelled, over 50 live fills; the excess "
        "is 14.0 standard errors, past the 2 the cost model is read against; "
        "live is outside the range validation explored."
    )


def test_an_excess_inside_two_standard_errors_says_nothing(db):
    promote(db)
    mode(db, "live")
    fills(db, ["15", "25"] * 25, modelled="19")

    assert health.live_cost(conn(db)) == []


def test_paper_fills_are_never_read_by_the_cost_alert(db):
    """The paper broker simulates neither impact nor slippage, so a paper shortfall measures
    its simulator. The view itself drops them, so no reading can be built from one."""
    promote(db)
    fills(db, ["15", "25"] * 25, modelled="10")

    assert view(db, "SELECT count(*) FROM agb.fill_shortfall WHERE mode = 'paper'") == [(50,)]
    assert view(db, "SELECT fills FROM agb.live_cost") == [(0,)]
    assert health.live_cost(conn(db)) == []


# The capital the model was certified for.


def test_equity_past_the_certified_capital_is_an_alert(db):
    promote(db, distribution(), capital=Decimal(100000))
    equity(db, (FILLED, Decimal(125000)))

    [finding] = health.certified_capital(conn(db))
    assert (finding.kind, finding.subject, finding.period) == (
        "certified_capital",
        "equity",
        "2026-10",
    )
    assert finding.what == "equity $125,000.00, past the certified capital"
    assert finding.facts == (
        "$125,000.00 observed 2026-10-01 16:00 ET, against $100,000.00 certified for "
        f"{MODEL}: 25.00% above the size validation covered; "
        "live is outside the range validation explored."
    )


def test_equity_at_the_certified_capital_is_not(db):
    promote(db, distribution(), capital=Decimal(100000))
    equity(db, (FILLED, Decimal(100000)))

    assert health.certified_capital(conn(db)) == []


# What a certification did not record, and what no certification means.


def test_a_quantile_the_certification_never_recorded_leaves_its_check_silent(db):
    """A threshold nobody validated is worse than no alert at all, so a certification without
    a path distribution raises nothing on the strategy's behaviour."""
    promote(db, {})
    mode(db, "paper")
    months(db, "-0.20", "-0.20", "-0.20")

    assert health.drawdown(conn(db)) == []
    assert health.losing_run(conn(db)) == []
    assert health.tracking(conn(db)) == []


def test_with_no_model_promoted_there_is_nothing_to_compare_against(db):
    mode(db, "paper")
    months(db, "-0.20", "-0.20", "-0.20")
    fills(db, ["15", "25"] * 25, modelled="10")

    assert health.thresholds(conn(db)) is None
    assert health.drawdown(conn(db)) == []
    assert health.losing_run(conn(db)) == []
    assert health.tracking(conn(db)) == []
    assert health.certified_capital(conn(db)) == []


# What the bot says, and how often.


@pytest.fixture
def reading(db):
    """A promoted model, a drawdown past its fifth percentile and equity past its certified
    capital: two performance readings for the bot to raise."""
    promote(db, distribution(drawdown=-0.12), capital=Decimal(50000))
    mode(db, "paper")
    months(db, "0.05", "-0.10", "-0.05")
    return db


def performance(db):
    return [
        (kind, subject, period)
        for kind, subject, period in db.connect("cli")
        .execute("SELECT kind, subject, period FROM agb.alert ORDER BY id")
        .fetchall()
        if kind in health.PERFORMANCE
    ]


def test_each_performance_reading_is_alerted_once_a_month(db, reading):
    """Acceptance criterion 4: the alert dedupes on the month it is about, so a drawdown that
    deepens for a fortnight is said once rather than every minute."""
    telegram = FakeTelegram()
    bot = Bot(telegram, CHAT, lambda: NOW)
    bot(db.connect("bot"))
    bot(db.connect("bot"))

    assert performance(db) == [
        ("drawdown", "equity", "2026-03"),
        ("certified_capital", "equity", "2026-03"),
    ]


def test_the_performance_alert_says_what_was_not_done_and_never_advises(db, reading):
    """The message is a reading and an offer of two levers, and nothing in it suggests which
    one to pull: the system does not act on a performance figure and does not hint."""
    telegram = FakeTelegram()
    Bot(telegram, CHAT, lambda: NOW)(db.connect("bot"))

    [body] = [
        text
        for _, text in telegram.sent
        if text.startswith(bot_service.PERFORMANCE_HEADER) and "drawdown" in text
    ]
    header, finding, footer = body.split("\n\n")
    assert header == "PERFORMANCE ALERT — nothing has been done and nothing will be."
    assert finding.startswith("  drawdown -14.50%, past the certified range\n    ")
    assert health.OUTSIDE in finding
    assert footer == (
        "/pause stops the next rebalance and /liquidate closes every position. "
        "Both are yours to choose; nothing in this message chooses for you, "
        "and no part of the system will act on it."
    )
    for word in ("should", "consider", "recommend", "advise", "suggest", "must"):
        assert word not in body.lower()


def test_a_health_alert_keeps_its_own_header_and_no_footer(db):
    """The performance wording is for performance only: a stale heartbeat is still the health
    alert it was."""
    text = bot_service.alert_text(
        health.Finding("stale_heartbeat", "pipeline", "2026-10", "stale heartbeat", "no beat")
    )
    assert text == "HEALTH ALERT — nothing has been done and nothing will be.\n\n" + (
        "  stale heartbeat\n    no beat"
    )
