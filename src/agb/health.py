"""What is unhealthy, read from the database: stale heartbeats, services that never beat,
overdue expected events, a rebalance that was never scheduled, open halts, the broker
disagreeing with the database, and a panel body the nightly integrity check could not read
back. The bot turns these into alerts and the pre-flight.

Then how the account is performing, which is a different question with the same answer:
nothing. A drawdown deeper than the certified paths reached, a losing run longer than they
ran, tracking wider than they dispersed, live cost above the modelled cost by more than
chance, equity beyond the capital the model was certified for — each of those is a reading
the operator is told about, and none of them moves a share. The thresholds come from the
promoted certification's own CPCV path distribution, so a figure past one of them means the
live world is outside the range validation explored, not that a number crossed a line
someone chose.

Each finding carries the dedupe key of the alert it raises, and two short lines of plain
text: what is wrong, then the facts behind it.
"""

import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg

from agb import heartbeat
from agb.cli import expected_services, format_age
from agb.nyse import NEW_YORK

# A service whose heartbeat is older than this is stale. The window is the heartbeat module's,
# so the bot, the UI, `agb status` and each container's healthcheck all apply the same one.
STALE_AFTER = heartbeat.STALE_AFTER
stale_after_from_env = heartbeat.stale_after_from_env

# How long an expected service may go without writing its first heartbeat before that is an
# alert. It is counted from when the bot itself started (the `since` of `never_started`),
# because a service that never beat leaves no row to count from, and the bot is the one thing
# known to be running. A fresh stack, started together, is therefore quiet for this long. It
# is longer than STALE_AFTER because a first beat waits behind the image pull, the migration
# and the service's own start-up, where a stale one only waits for a missed cycle.
NEVER_STARTED_GRACE = timedelta(minutes=10)

# The live cost reading is not worth a word before this many fills, and not worth one until
# the excess over the modelled cost is larger than this many standard errors of itself.
COST_FILLS = 50
COST_SIGMAS = 2.0

# The kinds the performance checks raise. The bot alerts on these in its own words, because
# the thing to say about them is what was not done.
PERFORMANCE = frozenset({"drawdown", "losing_run", "tracking", "live_cost", "certified_capital"})

# The sentence every performance finding ends on. The figure is not a failure and not a
# verdict; it is a reading from outside the region the certification measured, which is the
# one fact that makes it worth the operator's evening.
OUTSIDE = "live is outside the range validation explored"


@dataclass(frozen=True)
class Finding:
    kind: str
    subject: str
    period: str
    what: str
    facts: str

    def lines(self) -> str:
        return f"  {self.what}\n    {self.facts}"


def et(at: datetime) -> str:
    return at.astimezone(NEW_YORK).strftime("%Y-%m-%d %H:%M ET")


def ago(now: datetime, at: datetime) -> str:
    return f"{format_age(now - at)} ago"


def stale_heartbeats(conn: psycopg.Connection, now: datetime, stale_after: timedelta):
    """Every other service whose last beat is older than `stale_after`. A service that has
    never beaten has no row, and is not reported here but by `never_started`."""
    rows = conn.execute(
        "SELECT service, beat_at FROM agb.heartbeat "
        "WHERE beat_at < %s AND service <> current_user ORDER BY service",
        (now - stale_after,),
    ).fetchall()
    # One alert per stale episode: the last beat identifies it, so a service that recovers and
    # goes stale again raises a new one.
    return [
        Finding(
            "stale_heartbeat",
            service,
            beat_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"stale heartbeat: {service}",
            f"last beat {ago(now, beat_at)}, at {et(beat_at)}",
        )
        for service, beat_at in rows
    ]


def never_started(
    conn: psycopg.Connection,
    now: datetime,
    since: datetime,
    expected: Iterable[str] | None = None,
    grace: timedelta = NEVER_STARTED_GRACE,
) -> list[Finding]:
    """Every other expected service with no heartbeat row at all, once `grace` has passed since
    `since`, the moment the caller started watching (the bot's own start). The expected set is
    the one `agb status` insists on, `AGB_EXPECTED_SERVICES` or every service, unless `expected`
    is given. The checking service is never reported: if it can check, it is running.

    One alert per service, for as long as the database has no row for it. The heartbeat table
    is never emptied, so a service that has beaten once is stale or healthy from then on, and
    the alert's period is the fixed word `never`."""
    if now - since < grace:
        return []
    (me,) = conn.execute("SELECT current_user").fetchone()
    beating = {row[0] for row in conn.execute("SELECT service FROM agb.heartbeat").fetchall()}
    return [
        Finding(
            "never_started",
            service,
            "never",
            f"never started: {service}",
            f"no heartbeat {format_age(now - since)} after the bot began watching, at "
            f"{et(since)}; expected by AGB_EXPECTED_SERVICES",
        )
        for service in dict.fromkeys(expected_services() if expected is None else expected)
        if service != me and service not in beating
    ]


def overdue(conn: psycopg.Connection, now: datetime) -> list[Finding]:
    """The overdue view as of `now`: past-due expected events, and no rebalance ahead."""
    rows = conn.execute(
        "SELECT kind, period, due_at, reason FROM agb.overdue_at(%s) ORDER BY due_at, kind",
        (now,),
    ).fetchall()
    findings = []
    for kind, period, due_at, reason in rows:
        if reason == "no_rebalance_within_horizon":
            # One alert until another rebalance is scheduled: the last one identifies it.
            (last,) = conn.execute(
                "SELECT max(period) FROM agb.expected_event WHERE kind = 'submission'"
            ).fetchone()
            findings.append(
                Finding(
                    "no_rebalance_within_horizon",
                    kind,
                    last or "none",
                    "no rebalance within horizon",
                    f"none due before {due_at.astimezone(NEW_YORK):%Y-%m-%d}; "
                    f"last scheduled {last or 'none'}",
                )
            )
        else:
            findings.append(
                Finding(
                    "overdue",
                    kind,
                    period,
                    f"overdue: {kind} {period}",
                    f"due {et(due_at)}, {ago(now, due_at)}",
                )
            )
    return findings


def rebalance_not_scheduled(conn: psycopg.Connection, period: str, trade: date) -> list[Finding]:
    """The rebalance trading on `trade` has no submission expected, so nothing would miss it."""
    scheduled = conn.execute(
        "SELECT 1 FROM agb.expected_event WHERE kind = 'submission' AND period = %s", (period,)
    ).fetchone()
    if scheduled:
        return []
    return [
        Finding(
            "rebalance_not_scheduled",
            "submission",
            period,
            f"rebalance {period} not scheduled",
            f"no submission expected for {trade:%Y-%m-%d}",
        )
    ]


def open_halt_causes(conn: psycopg.Connection) -> list[Finding]:
    """Every open halt cause, oldest first. One alert per opening: when it opened
    identifies it, so a cause cleared and set again raises a new one."""
    rows = conn.execute(
        "SELECT cause, at, basket_id, actor, reason FROM agb.halt_open ORDER BY at"
    ).fetchall()
    findings = []
    for cause, at, basket_id, actor, reason in rows:
        facts = f"since {et(at)}, by {actor}"
        if basket_id is not None:
            facts += f", basket {basket_id}"
        if reason:
            facts += f": {reason}"
        findings.append(
            Finding(
                "halt",
                cause,
                at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                f"halt open: {cause}",
                facts,
            )
        )
    return findings


def broker_mismatch(conn: psycopg.Connection) -> list[Finding]:
    """The broker's `suspend_trade`, as execution last observed it, against the `pause`
    halt cause: a mismatch in either direction. Nothing is reported until execution has
    observed the broker since the pause cause last changed, nor while a pause or resume is
    waiting for execution to carry it to the broker. One alert per episode: the first
    observation of the disagreeing value since the cause last changed identifies it, so a
    mismatch that persists raises one alert and one that recurs later raises another."""
    row = conn.execute(
        "SELECT id, observed_at, suspend_trade FROM agb.broker_state ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return []
    latest_id, observed_at, suspend_trade = row
    if conn.execute("SELECT 1 FROM agb.latch_pending").fetchone() is not None:
        return []
    (changed_at,) = conn.execute(
        "SELECT max(at) FROM agb.halt_event WHERE cause = 'pause'"
    ).fetchone()
    if changed_at is not None and observed_at < changed_at:
        return []
    paused = (
        conn.execute("SELECT 1 FROM agb.halt_open WHERE cause = 'pause'").fetchone() is not None
    )
    if suspend_trade == paused:
        return []
    (since,) = conn.execute(
        """
        SELECT min(s.observed_at) FROM agb.broker_state s
        WHERE s.id > coalesce(
                  (SELECT max(o.id) FROM agb.broker_state o
                   WHERE o.suspend_trade <> %(suspend)s AND o.id < %(latest)s), 0)
          AND s.observed_at >= coalesce(%(changed)s, '-infinity'::timestamptz)
        """,
        {"suspend": suspend_trade, "latest": latest_id, "changed": changed_at},
    ).fetchone()
    return [
        Finding(
            "broker",
            "suspend_trade",
            since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"broker suspend_trade: {'on' if suspend_trade else 'off'}",
            f"{'pause open' if paused else 'no pause open'}; observed {et(observed_at)}",
        )
    ]


def position_mismatch(conn: psycopg.Connection) -> list[Finding]:
    """Every symbol in the latest position snapshot whose quantity is not the previous
    snapshot's plus the fills between the two reads. One alert per symbol and snapshot."""
    rows = conn.execute(
        "SELECT snapshot_date, symbol, expected_qty, observed_qty FROM agb.position_mismatch "
        "WHERE snapshot_id = (SELECT max(id) FROM agb.position_snapshot) ORDER BY symbol"
    ).fetchall()
    return [
        Finding(
            "position_mismatch",
            symbol,
            snapshot_date.isoformat(),
            f"position mismatch: {symbol}",
            f"broker holds {observed.normalize():f}, expected {expected.normalize():f}"
            f" from the previous snapshot plus fills; snapshot {snapshot_date}",
        )
        for snapshot_date, symbol, expected, observed in rows
    ]


def panel_bodies(conn: psycopg.Connection) -> list[Finding]:
    """The latest panel-store integrity check, if it found a body it could not read back.

    Latest by when it was recorded, not by period: an audit the operator drives for the drill
    has a suffixed period (2026-11-15-drill-143012) and must be the one read, and a period
    dated ahead of today must not shadow a real finding.

    The check itself is the pipeline's nightly `panel_audit` job, which records what it found
    whether or not anything was wrong (agb.panel_audit). One alert per night, keyed by the
    period, so a disk losing bodies raises one alert a night rather than one per body, and the
    facts name the first hash to go and look for.
    """
    row = conn.execute(
        "SELECT period, bodies, checked, missing, corrupt FROM agb.panel_audit "
        "ORDER BY at DESC, id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return []
    period, bodies, checked, missing, corrupt = row
    unsound = list(missing) + list(corrupt)
    if not unsound:
        return []
    return [
        Finding(
            "panel_body_unsound",
            "panel store",
            period,
            f"panel store: {len(missing)} missing, {len(corrupt)} corrupt of {checked} checked",
            f"checked {checked} of {bodies} referenced bodies on {period}; "
            f"first unsound {unsound[0]}",
        )
    ]


# Performance and cost. These read the same two sources: what the account did, and what the
# promoted certification said a world like this one would do. The formatters are local
# because agb.readout reads this module and not the other way round.


def _percent(value: float, places: int = 2) -> str:
    return f"{value * 100:.{places}f}%"


def _dollars(value: Decimal) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def _bps(value: float) -> str:
    return f"{value:.1f} bps"


@dataclass(frozen=True)
class Thresholds:
    """The promoted certification's alert thresholds. Every quantile is optional: a
    certification that did not record one leaves the check that needs it silent, which is the
    right answer, because a threshold nobody validated is worse than no alert at all."""

    model_id: str
    certified_capital: Decimal
    paths: int | None
    drawdown_q05: float | None
    drawdown_min: float | None
    losing_run_q95: float | None
    losing_run_max: float | None
    tracking_q95: float | None
    tracking_max: float | None
    monthly_return: float | None

    def over_paths(self) -> str:
        return f"{self.paths} certified paths" if self.paths else "the certified paths"


@dataclass(frozen=True)
class Month:
    """A New York month of the live account, as agb.equity_month reads it. `total_return`
    includes the dividends the operator recorded when the month was a paper month, because
    the paper broker credits none and the backtest's expectation assumes them."""

    period: str
    total_return: float
    mode: str | None
    dividends: Decimal


_THRESHOLDS = """
    SELECT model_id, certified_capital, paths, drawdown_q05, drawdown_min,
           losing_run_q95, losing_run_max, tracking_q95, tracking_max, monthly_return
    FROM agb.alert_threshold
"""
_MONTHS = """
    SELECT period, total_return_fraction, mode, dividends_usd
    FROM agb.equity_month
    WHERE total_return_fraction IS NOT NULL
    ORDER BY period
"""
_LIVE_COST = """
    SELECT fills, mean_shortfall_bps, mean_modelled_bps, mean_excess_bps, excess_se_bps,
           last_fill_at
    FROM agb.live_cost
"""
_EQUITY = "SELECT equity_usd, observed_at FROM agb.broker_state_now"


def thresholds(conn: psycopg.Connection) -> Thresholds | None:
    """The promoted model's thresholds, or None when no model is promoted."""
    row = conn.execute(_THRESHOLDS).fetchone()
    return None if row is None else Thresholds(*row)


def equity_months(conn: psycopg.Connection) -> list[Month]:
    """Every month the account has a return for, oldest first."""
    return [
        Month(period, float(total_return), mode, dividends)
        for period, total_return, mode, dividends in conn.execute(_MONTHS).fetchall()
    ]


class Basis:
    """The monthly aggregates the performance readings share, read at most once each: the
    promoted model's thresholds and the account's months. A beat makes one and hands it to
    every reading, which otherwise each ask the database for the same two things. Both are
    read on first use, so a beat with no model promoted never reads the months."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn
        self._limits: tuple[Thresholds | None] | None = None
        self._months: list[Month] | None = None

    @property
    def limits(self) -> Thresholds | None:
        if self._limits is None:
            self._limits = (thresholds(self._conn),)
        return self._limits[0]

    @property
    def months(self) -> list[Month]:
        if self._months is None:
            self._months = equity_months(self._conn)
        return self._months


def month_of(at: datetime) -> str:
    """The New York month `at` falls in, which is how the performance alerts dedupe: one per
    month per subject, so a drawdown that deepens for a fortnight is said once."""
    return at.astimezone(NEW_YORK).strftime("%Y-%m")


def worst_drawdown(months: Sequence[Month]) -> tuple[float, str | None]:
    """The worst peak-to-trough fall the months contain, and the month it bottomed in, or
    (0.0, None) when they never fell. The peak includes the current month, as the backtest's
    does, so the alert and the UI read the same measurement the certified paths recorded."""
    mark = peak = 1.0
    worst, trough = 0.0, None
    for month in months:
        mark *= 1.0 + month.total_return
        peak = max(peak, mark)
        fall = mark / peak - 1.0
        if fall < worst:
            worst, trough = fall, month.period
    return worst, trough


def longest_losing_run(months: Sequence[Month]) -> tuple[int, str | None]:
    """The longest run of consecutive losing months, and the month it ended in."""
    run, longest, end = 0, 0, None
    for month in months:
        run = run + 1 if month.total_return < 0 else 0
        if run > longest:
            longest, end = run, month.period
    return longest, end


def scatter(months: Sequence[Month], expected: float) -> float | None:
    """How far the months scatter around the month the backtest expected, or None under two
    months, which is too few to measure a dispersion at all."""
    if len(months) < 2:
        return None
    return statistics.stdev(month.total_return - expected for month in months)


def drawdown(conn: psycopg.Connection, basis: Basis | None = None) -> list[Finding]:
    """The account's worst peak-to-trough fall, against the fifth percentile of the certified
    paths' own worst falls. The peak includes the current month, as the backtest's does, so
    the two numbers are the same measurement. One alert per trough month. A `basis` read
    once for the beat spares the reads it already holds."""
    basis = basis or Basis(conn)
    limits = basis.limits
    if limits is None or limits.drawdown_q05 is None:
        return []
    worst, trough = worst_drawdown(basis.months)
    if trough is None or worst >= limits.drawdown_q05:
        return []
    deepest = (
        ""
        if limits.drawdown_min is None
        else f", and {_percent(limits.drawdown_min)} at their deepest"
    )
    return [
        Finding(
            "drawdown",
            "equity",
            trough,
            f"drawdown {_percent(worst)}, past the certified range",
            f"worst fall from peak {_percent(worst)}, through {trough}; "
            f"{_percent(limits.drawdown_q05)} at the 5th percentile of "
            f"{limits.over_paths()}{deepest}; {OUTSIDE}.",
        )
    ]


def losing_run(conn: psycopg.Connection, basis: Basis | None = None) -> list[Finding]:
    """The longest run of losing months the account has had, against the 95th percentile of
    the certified paths' longest runs. One alert per month the run ends in."""
    basis = basis or Basis(conn)
    limits = basis.limits
    if limits is None or limits.losing_run_q95 is None:
        return []
    longest, end = longest_losing_run(basis.months)
    if end is None or longest <= limits.losing_run_q95:
        return []
    longest_certified = (
        "" if limits.losing_run_max is None else f", and {limits.losing_run_max:g} at their longest"
    )
    return [
        Finding(
            "losing_run",
            "equity",
            end,
            f"losing run {longest} months, past the certified range",
            f"{longest} consecutive losing months, through {end}; "
            f"{limits.losing_run_q95:g} at the 95th percentile of "
            f"{limits.over_paths()}{longest_certified}; {OUTSIDE}.",
        )
    ]


def tracking(conn: psycopg.Connection, basis: Basis | None = None) -> list[Finding]:
    """How far the account's months scatter around the backtest's expected month, against the
    95th percentile of the certified paths' own dispersion. Needs two months to measure a
    dispersion at all. One alert per latest month."""
    basis = basis or Basis(conn)
    limits = basis.limits
    if limits is None or limits.tracking_q95 is None or limits.monthly_return is None:
        return []
    months = basis.months
    dispersion = scatter(months, limits.monthly_return)
    if dispersion is None or dispersion <= limits.tracking_q95:
        return []
    widest = (
        ""
        if limits.tracking_max is None
        else f", and {_percent(limits.tracking_max)} at their widest"
    )
    # The paper months were short their dividends until the operator recorded them; say so,
    # because a reader who knows the paper broker credits none will wonder.
    paper = [m for m in months if m.mode == "paper"]
    credited = sum((m.dividends for m in paper), Decimal(0))
    added = (
        f" {_dollars(credited)} of recorded dividends added back over {len(paper)} paper months."
        if credited
        else ""
    )
    return [
        Finding(
            "tracking",
            "equity",
            months[-1].period,
            f"tracking {_percent(dispersion)}, past the certified range",
            f"monthly returns scatter {_percent(dispersion)} around the expected "
            f"{_percent(limits.monthly_return)} over {len(months)} months; "
            f"{_percent(limits.tracking_q95)} at the 95th percentile of "
            f"{limits.over_paths()}{widest}; {OUTSIDE}.{added}",
        )
    ]


def live_cost(conn: psycopg.Connection) -> list[Finding]:
    """Live fills costing more than the model said they would, by more than chance: the mean
    per-fill shortfall against the reference mid at submission, less the cost the model had
    frozen for the same orders, when that excess is larger than COST_SIGMAS standard errors
    of itself. Paper fills are not read at all — the paper broker simulates neither impact
    nor slippage, so a paper shortfall measures its simulator. One alert per month of the
    last live fill."""
    fills, shortfall, modelled, excess, error, last_fill_at = conn.execute(_LIVE_COST).fetchone()
    if fills < COST_FILLS or error is None or error <= 0:
        return []
    sigmas = float(excess) / float(error)
    if sigmas <= COST_SIGMAS:
        return []
    return [
        Finding(
            "live_cost",
            "shortfall",
            month_of(last_fill_at),
            f"live cost {_bps(float(excess))} above the modelled cost",
            f"mean shortfall {_bps(float(shortfall))} against {_bps(float(modelled))} "
            f"modelled, over {fills} live fills; the excess is {sigmas:.1f} standard "
            f"errors, past the {COST_SIGMAS:g} the cost model is read against; {OUTSIDE}.",
        )
    ]


def certified_capital(conn: psycopg.Connection, basis: Basis | None = None) -> list[Finding]:
    """Account equity beyond the capital the promoted model was certified for. The
    certification measured participation and cost at that size; above it, the numbers it
    passed on are no longer the numbers that apply. One alert per month of the observation."""
    limits = (basis or Basis(conn)).limits
    if limits is None:
        return []
    account = conn.execute(_EQUITY).fetchone()
    if account is None:
        return []
    equity, observed_at = account
    if equity is None or equity <= limits.certified_capital:
        return []
    over = equity / limits.certified_capital - 1
    return [
        Finding(
            "certified_capital",
            "equity",
            month_of(observed_at),
            f"equity {_dollars(equity)}, past the certified capital",
            f"{_dollars(equity)} observed {et(observed_at)}, against "
            f"{_dollars(limits.certified_capital)} certified for {limits.model_id}: "
            f"{_percent(float(over))} above the size validation covered; {OUTSIDE}.",
        )
    ]
