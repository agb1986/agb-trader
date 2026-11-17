"""The paper record's arithmetic checks: three of the paper-to-live criteria.

Three of the criteria in docs/architecture/04 are arithmetic rather than judgement, and this
module measures each one from what paper trading recorded:

    fill_tolerance  every paper fill printed within a few basis points of the session's
                    official close, which is the assumption the whole backtest rests on
    fee_arithmetic  each fill's recorded fees reproduce under the fee schedule of the
                    cost-model version they were priced with
    partial_fills   a short fill happened and was handled: the rebalance raised the
                    partial-rebalance alert for the cause that left it short rather than
                    passing in silence

Each result is appended to `agb.paper_check`, which `agb.live_status` reads for the newest
result of each name, so re-running a check appends and never overwrites. Nothing here passes
by vacuity: a check with too little to measure raises rather than recording a pass, which
leaves the criterion reading "not run" until paper has produced the evidence.

Alpaca paper simulates neither impact nor slippage nor regulatory fees, so the cost model
itself cannot be validated here. These checks ask only that the mechanics are right.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import duckdb
import psycopg

from agb import cost, execution

log = logging.getLogger(__name__)

CHECKS = ("fill_tolerance", "fee_arithmetic", "partial_fills")

BPS = Decimal(10_000)
# How far a fill may print from the session's official close. A market-on-close order fills at
# the official close, so this is rounding room, not a slippage budget.
TOLERANCE_BPS = Decimal("5")
# The fill count the tolerance check measures over, so a pass means something.
MIN_FILLS = 20
# The cent the fee schedule rounds to. The CAT fee is unrounded, so a recorded fee agrees with
# the recomputed one when it is within half a cent of it.
CENT = Decimal("0.01")
# How many offending rows a result carries in `detail`; the rest are counted, not listed.
DETAIL_ROWS = 20


class MechanicsError(Exception):
    """A check that cannot be measured: the paper record holds too little for it."""


@dataclass(frozen=True)
class Result:
    """One check's result, as it is recorded."""

    name: str
    passed: bool
    measured: str
    threshold: str
    detail: dict[str, Any] = field(default_factory=dict)
    basket_id: int | None = None

    def __str__(self) -> str:
        return f"{self.name}: {'passed' if self.passed else 'FAILED'}, {self.measured}"


def record(conn: psycopg.Connection, result: Result, actor: str) -> int:
    """Append a check's result to `paper_check`; returns the row's id."""
    row = conn.execute(
        """
        INSERT INTO agb.paper_check (name, basket_id, passed, measured, threshold, detail, actor)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            result.name,
            result.basket_id,
            result.passed,
            result.measured,
            result.threshold,
            json.dumps(result.detail, sort_keys=True, default=str),
            actor,
        ),
    ).fetchone()
    assert row is not None
    return row[0]


# Every fill of a settled paper rebalance, with the session it was for and the order it came
# from. Paper and live are told apart by the mode the basket was gated under, which is what
# `ladder_rebalance` carries.
_FILLS = """
    SELECT f.id, r.basket_id, r.reference, r.session_date, o.symbol, o.side,
           f.qty, f.price, f.sec_fee_usd, f.taf_fee_usd, f.cat_fee_usd, f.commission_usd,
           f.fee_model_version
    FROM agb.fill f
    JOIN agb.broker_order o ON o.id = f.order_id
    JOIN agb.ladder_rebalance r ON r.basket_id = o.basket_id
    WHERE r.mode = 'paper'
    ORDER BY f.id
"""
# The partial-rebalance alert that says a settled rebalance was left short, by the state it
# settled in: a filled one filled short at the close, an unfilled one filled nothing at all,
# a halted one was halted with orders still live. Each with the start of the body the same
# alert carried under the one kind every cause shared before they were split, which is how an
# alert raised then is told from the submission's refusal alert that shared its key. The
# unfilled alert came after the split and was never raised under the shared kind.
_CAUSES = (
    ("filled", execution.PARTIAL_SHORT_FILL, "filled short on "),
    ("unfilled", execution.BASKET_UNFILLED, "filled nothing: "),
    ("halted", execution.PARTIAL_HALTED, "halted mid-submission; "),
    ("halted", execution.PARTIAL_PAUSED, "halted by a pause mid-submission; "),
)
# Every order of a settled paper rebalance the broker did not fill in full, and whether the
# rebalance it belongs to raised the partial-rebalance alert for the cause that left it short.
# Another cause's alert is not evidence: a basket that alerted at submission and then filled
# short has said nothing about the short fill.
_SHORT = """
    SELECT r.basket_id, r.reference, o.symbol, o.side, o.qty,
           coalesce(sum(f.qty), 0) AS filled,
           EXISTS (SELECT 1
                   FROM unnest(%(states)s::text[], %(kinds)s::text[], %(bodies)s::text[])
                        AS c(state, kind, body)
                   JOIN agb.alert a ON a.subject = r.reference
                   WHERE c.state = r.state
                     AND (a.kind = c.kind
                          OR (a.kind = %(legacy)s
                              AND starts_with(a.body, 'basket ' || r.reference || ' ' || c.body))))
               AS alerted
    FROM agb.ladder_rebalance r
    JOIN agb.broker_order o ON o.basket_id = r.basket_id
    LEFT JOIN agb.fill f ON f.order_id = o.id
    WHERE r.mode = 'paper'
    GROUP BY r.basket_id, r.reference, r.state, o.id, o.symbol, o.side, o.qty
    HAVING coalesce(sum(f.qty), 0) < o.qty
    ORDER BY r.basket_id, o.id
"""
# The as-traded close of every name that printed on one session, the same price and the same
# filter the basket was built from.
_CLOSES = """
    SELECT ticker, closeunadj FROM sep
    WHERE date = ? AND volume > 0 AND closeunadj IS NOT NULL AND closeunadj > 0
"""


def fill_tolerance(
    conn: psycopg.Connection,
    lake_db: duckdb.DuckDBPyConnection,
    tolerance_bps: Decimal = TOLERANCE_BPS,
    minimum: int = MIN_FILLS,
) -> Result:
    """Every paper fill against the official close of the session it was for."""
    fills = conn.execute(_FILLS).fetchall()
    if len(fills) < minimum:
        raise MechanicsError(
            f"{len(fills)} paper fills on record, fewer than the {minimum} this check measures over"
        )
    closes = _closes(lake_db, {row[3] for row in fills})
    worst, breaches = Decimal(0), []
    for _, _, reference, session, symbol, side, qty, price, *_rest in fills:
        close = closes.get((session, symbol))
        if close is None:
            raise MechanicsError(
                f"the lake has no official close for {symbol} on {session}, so the fill of"
                f" rebalance {reference} cannot be measured"
            )
        away = abs(price - close) / close * BPS
        worst = max(worst, away)
        if away > tolerance_bps:
            breaches.append(
                {
                    "reference": reference,
                    "symbol": symbol,
                    "side": side,
                    "qty": str(qty),
                    "price": str(price),
                    "close": str(close),
                    "bps": f"{away:.1f}",
                }
            )
    return Result(
        name="fill_tolerance",
        passed=not breaches,
        measured=f"{len(fills)} fills, worst {worst:.1f} bps from the close"
        + (f", {len(breaches)} outside tolerance" if breaches else ""),
        threshold=f"every fill within {tolerance_bps:.0f} bps",
        detail={
            "fills": len(fills),
            "worst_bps": f"{worst:.1f}",
            "breaches": breaches[:DETAIL_ROWS],
        },
    )


def _closes(
    lake_db: duckdb.DuckDBPyConnection, sessions: set[date]
) -> dict[tuple[date, str], Decimal]:
    closes: dict[tuple[date, str], Decimal] = {}
    for session in sorted(sessions):
        rows = lake_db.execute(_CLOSES, [session]).fetchall()
        if not rows:
            raise MechanicsError(f"the lake has no prices for session {session}")
        for ticker, close in rows:
            closes[(session, str(ticker))] = Decimal(str(close))
    return closes


def fee_arithmetic(conn: psycopg.Connection) -> Result:
    """Each paper fill's recorded fees against the schedule that priced them: the released
    cost model its own `fee_model_version` names, not the one in force now, so a fee change
    does not fail the fills made before it. A version no released model carries fails, because
    its fees cannot be recomputed. The regulatory fees fall on sells, so a record with no sell
    fill has nothing to check."""
    fills = conn.execute(_FILLS).fetchall()
    sells = [row for row in fills if row[5] == cost.Side.SELL]
    if not sells:
        raise MechanicsError(
            "no paper sell fill on record, and the regulatory fees fall on sells only"
        )
    wrong = []
    for _, _, reference, _, symbol, side, qty, price, sec, taf, cat, commission, version in fills:
        model = cost.MODELS.get(version)
        if model is None:
            wrong.append(
                {
                    "reference": reference,
                    "symbol": symbol,
                    "side": side,
                    "fee": "fee_model_version",
                    "recorded": version,
                    "expected": f"one of {', '.join(sorted(cost.MODELS))}",
                }
            )
            continue
        expected = cost.fees(side, qty * price, qty, model)
        for name, was, should in (
            ("sec_fee_usd", sec, expected.sec),
            ("taf_fee_usd", taf, expected.taf),
            ("cat_fee_usd", cat, expected.cat),
            ("commission_usd", commission, expected.commission),
        ):
            if abs(was - should) > CENT / 2:
                wrong.append(
                    {
                        "reference": reference,
                        "symbol": symbol,
                        "side": side,
                        "fee": name,
                        "recorded": str(was),
                        "expected": str(should),
                    }
                )
    return Result(
        name="fee_arithmetic",
        passed=not wrong,
        measured=f"{len(fills)} fills, {len(sells)} of them sells, "
        + ("every fee reproduces" if not wrong else f"{len(wrong)} fees do not reproduce"),
        threshold="every fee reproduces under its cost-model version",
        detail={
            "fills": len(fills),
            "sells": len(sells),
            "model_version": cost.CURRENT.version,
            "versions": sorted({row[-1] for row in fills}),
            "wrong": wrong[:DETAIL_ROWS],
        },
    )


def partial_fills(conn: psycopg.Connection) -> Result:
    """Short fills in the paper record, and whether each rebalance that had one said so. The
    check needs a short fill to have happened: paper that filled everything in full proves
    nothing about the handling of one."""
    states, kinds, bodies = (list(column) for column in zip(*_CAUSES, strict=True))
    short = conn.execute(
        _SHORT,
        {
            "states": states,
            "kinds": kinds,
            "bodies": bodies,
            "legacy": execution.PARTIAL_REBALANCE,
        },
    ).fetchall()
    if not short:
        raise MechanicsError(
            "every paper order filled in full, so there is no partial fill to check the handling of"
        )
    silent = [row for row in short if not row[6]]
    baskets = {row[1] for row in short}
    return Result(
        name="partial_fills",
        passed=not silent,
        measured=f"{len(short)} short fills in {len(baskets)} rebalances, "
        + (
            "each alerted"
            if not silent
            else f"{len({row[1] for row in silent})} of them raised no alert"
        ),
        threshold=(
            f"every rebalance with a short fill raises {execution.PARTIAL_SHORT_FILL}, "
            f"{execution.BASKET_UNFILLED} if it filled nothing, "
            f"or {execution.PARTIAL_HALTED} or {execution.PARTIAL_PAUSED} if it was halted"
        ),
        detail={
            "orders": len(short),
            "rebalances": sorted(baskets),
            "unalerted": [
                {
                    "reference": reference,
                    "symbol": symbol,
                    "side": side,
                    "ordered": str(qty),
                    "filled": str(filled),
                }
                for _, reference, symbol, side, qty, filled, _ in silent[:DETAIL_ROWS]
            ],
        },
    )


def detail_text(result: Result) -> str:
    """What the check found, for the console: the part of `detail` an operator acts on."""
    lines: list[str] = []
    if result.name == "fill_tolerance":
        for breach in result.detail.get("breaches", ()):
            lines.append(
                f"  {breach['symbol']:<6} {breach['reference']}  {breach['side']} at "
                f"{breach['price']} against the close {breach['close']}: {breach['bps']} bps"
            )
    elif result.name == "fee_arithmetic":
        for row in result.detail.get("wrong", ()):
            lines.append(
                f"  {row['symbol']:<6} {row['reference']}  {row['fee']} recorded "
                f"{row['recorded']}, schedule says {row['expected']}"
            )
    elif result.name == "partial_fills":
        for row in result.detail.get("unalerted", ()):
            lines.append(
                f"  {row['symbol']:<6} {row['reference']}  {row['side']} {row['filled']} of "
                f"{row['ordered']} filled, no alert"
            )
    return "\n".join(lines)
