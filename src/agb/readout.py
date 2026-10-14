"""The bot's answers, read from the database: status, positions, P&L, the basket, the gate
and the audit log, and the fill confirmation and gate-halt alert. Nothing here writes.

Every message is plain text in one style: a header line, then items of two lines each, the
first saying what the item is and the second its facts, then an optional footer. The bot
adds no prose of its own and never recommends.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg

from agb import analyst, blend, debate, health
from agb.health import et
from agb.nyse import NEW_YORK

# The most `/log` shows at once.
LOG_MAX = 100
LOG_DEFAULT = 10
# A detail longer than this is cut in `/log`.
DETAIL_MAX = 120
# A halt's reason is cut to this in /status.
HALT_MAX = 240


@dataclass(frozen=True)
class Item:
    what: str
    facts: str

    def lines(self) -> str:
        return f"  {self.what}\n    {self.facts}"


def message(header: str, items: Sequence[Item], footer: str | None = None) -> str:
    parts = [header, *(item.lines() for item in items)]
    if footer:
        parts.append(footer)
    return "\n\n".join(parts)


def dollars(value: Decimal) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.2f}"


def shares(qty: Decimal) -> str:
    return f"{qty.normalize():f}"


def percent(value: Decimal, places: int = 2) -> str:
    return f"{value * 100:.{places}f}%"


def signed_percent(value: Decimal) -> str:
    return f"{value * 100:+.2f}%"


def _basket_name(kind: str, reference: str | None, basket_id: int) -> str:
    return f"rebalance {reference}" if kind == "live" else f"shadow basket {basket_id}"


# Status.

_MODE = "SELECT mode, since, actor FROM agb.current_mode"
_LEVEL = "SELECT level FROM agb.current_level"
_ACCOUNT = """
    SELECT observed_at, account_status, equity_usd, cash_usd, suspend_trade
    FROM agb.broker_state_now
"""
_SNAPSHOT = """
    SELECT s.id, s.snapshot_date, count(p.id), coalesce(sum(p.market_value_usd), 0)
    FROM agb.position_snapshot s
    LEFT JOIN agb.position p ON p.snapshot_id = s.id
    WHERE s.id = (SELECT max(id) FROM agb.position_snapshot)
    GROUP BY s.id
"""
_LATEST_BASKET = """
    SELECT b.id, b.reference, b.period, b.ranker, b.capital_usd, s.state, s.since, s.actor
    FROM agb.basket b
    JOIN agb.basket_state s ON s.id = b.id
    WHERE b.kind = 'live'
    ORDER BY b.id DESC
    LIMIT 1
"""
_LIVE_MODEL = """
    SELECT model_id, certification_id, promoted_by, promoted_at, voided_at
    FROM agb.promoted_model
"""
_NEXT_SUBMISSION = """
    SELECT period, due_at FROM agb.expected_event
    WHERE kind = 'submission' AND due_at > %s
    ORDER BY due_at
    LIMIT 1
"""


def status(conn: psycopg.Connection, now: datetime) -> str:
    mode, _, _ = conn.execute(_MODE).fetchone()
    halts = health.open_halt_causes(conn)
    # The autonomy level stands beside the mode in live, where it decides what a rebalance
    # asks of the human. In paper it decides nothing, so it is not shown.
    level = conn.execute(_LEVEL).fetchone()
    badge = mode.upper() if mode != "live" or level is None else f"{mode.upper()} · {level[0]}"
    header = f"agb-trader — {badge} · {'HALTED' if halts else 'ACTIVE'}"
    # A gate breach carries every failed check in its reason; /gate lists them in full.
    items = [Item(h.what, _cut(h.facts, HALT_MAX)) for h in halts]

    account = conn.execute(_ACCOUNT).fetchone()
    if account is None:
        items.append(Item("account not yet observed", "execution has not read the broker"))
    else:
        observed_at, account_status, equity, cash, suspend_trade = account
        facts = f"observed {et(observed_at)}"
        if equity is not None:
            facts = f"equity {dollars(equity)}, cash {dollars(cash)}; {facts}"
        what = f"account {account_status or 'unknown'}"
        if suspend_trade:
            what += ", suspend_trade on"
        items.append(Item(what, facts))

    snapshot = conn.execute(_SNAPSHOT).fetchone()
    if snapshot is None:
        items.append(Item("no positions snapshot yet", "execution snapshots after each close"))
    else:
        _, snapshot_date, count, value = snapshot
        items.append(
            Item(f"{count} positions held", f"worth {dollars(value)}, snapshot {snapshot_date}")
        )

    basket = conn.execute(_LATEST_BASKET).fetchone()
    if basket is None:
        items.append(Item("no rebalance yet", "no live basket has been proposed"))
    else:
        _, reference, _, ranker, _, state, since, actor = basket
        items.append(
            Item(f"rebalance {reference} {state}", f"since {et(since)}, by {actor}; model {ranker}")
        )

    model = conn.execute(_LIVE_MODEL).fetchone()
    if model is None:
        items.append(Item("no model promoted", "rebalances fail until `agb promote` runs"))
    else:
        model_id, certification_id, promoted_by, promoted_at, voided_at = model
        facts = f"certification {certification_id}, promoted {et(promoted_at)} by {promoted_by}"
        if voided_at is None:
            items.append(Item(f"model {model_id} live", facts))
        else:
            items.append(
                Item(
                    f"model {model_id} voided {et(voided_at)}",
                    f"{facts}; rebalances fail until another is promoted",
                )
            )

    upcoming = conn.execute(_NEXT_SUBMISSION, (now,)).fetchone()
    if upcoming is None:
        items.append(Item("no rebalance scheduled", "the scheduler has none due"))
    else:
        period, due_at = upcoming
        items.append(Item(f"next rebalance {period}", f"orders due by {et(due_at)}"))
    return message(header, items)


# Positions.

_POSITIONS = """
    SELECT symbol, qty, avg_entry_price, market_value_usd
    FROM agb.position WHERE snapshot_id = %s
    ORDER BY market_value_usd DESC NULLS LAST, symbol
"""


def positions(conn: psycopg.Connection) -> str:
    snapshot = conn.execute(_SNAPSHOT).fetchone()
    if snapshot is None:
        return message(
            "POSITIONS — no snapshot yet",
            [Item("nothing to show", "execution snapshots the broker's positions after a close")],
        )
    snapshot_id, snapshot_date, count, total = snapshot
    header = f"POSITIONS — {count} held, {dollars(total)}, snapshot {snapshot_date}"
    if not count:
        return message(header, [Item("no positions", "the broker held nothing at the snapshot")])
    items = []
    for symbol, qty, entry, value in conn.execute(_POSITIONS, (snapshot_id,)).fetchall():
        facts = [dollars(value) if value is not None else "no value"]
        if value is not None and total:
            facts.append(percent(value / total, 1))
        if entry is not None:
            facts.append(f"entry {dollars(entry)}")
        items.append(Item(f"{symbol}  {shares(qty)} shares", " · ".join(facts)))
    return message(header, items)


# P&L.

PERIOD = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")

_READINGS = """
    SELECT observed_at, equity_usd FROM agb.broker_state
    WHERE equity_usd IS NOT NULL
      AND (%(start)s::timestamptz IS NULL OR observed_at >= %(start)s)
      AND (%(end)s::timestamptz IS NULL OR observed_at < %(end)s)
    ORDER BY observed_at
"""
_BEFORE = """
    SELECT observed_at, equity_usd FROM agb.broker_state
    WHERE equity_usd IS NOT NULL AND observed_at < %s
    ORDER BY observed_at DESC
    LIMIT 1
"""
_FILLS = """
    SELECT count(*),
           coalesce(sum(sec_fee_usd + taf_fee_usd + cat_fee_usd + commission_usd), 0)
    FROM agb.fill
    WHERE filled_at > %s AND filled_at <= %s
"""


def month_bounds(period: str) -> tuple[datetime, datetime]:
    """The New York month `period` names, as its first instant and the next month's."""
    year, month = (int(part) for part in period.split("-"))
    start = datetime(year, month, 1, tzinfo=NEW_YORK)
    end = datetime(year + month // 12, month % 12 + 1, 1, tzinfo=NEW_YORK)
    return start, end


def equity_window(
    conn: psycopg.Connection, period: str | None = None
) -> tuple[datetime, Decimal, datetime, Decimal] | None:
    """The two equity readings a period is measured between, as (first_at, first, last_at,
    last): from the last reading before the period, or its own first reading, to its last.
    None if there are no readings in it. The UI's decision log reads a month's realised
    return from the same two readings `/pnl` does."""
    start, end = month_bounds(period) if period else (None, None)
    readings = conn.execute(_READINGS, {"start": start, "end": end}).fetchall()
    if not readings:
        return None
    opening = conn.execute(_BEFORE, (start,)).fetchone() if start else None
    first_at, first = opening or readings[0]
    last_at, last = readings[-1]
    return first_at, first, last_at, last


def pnl(conn: psycopg.Connection, period: str | None = None) -> str:
    """Realised return from the account equity execution observed, over `equity_window`."""
    header = f"PNL — {period or 'since the first reading'}"
    window = equity_window(conn, period)
    if window is None:
        return message(header, [Item("no equity readings", "execution has not read the account")])
    first_at, first, last_at, last = window
    items = [
        Item(
            f"return {signed_percent(last / first - 1) if first else 'n/a'}",
            f"equity {dollars(first)} to {dollars(last)}, {dollars(last - first)}",
        ),
        Item("window", f"{et(first_at)} to {et(last_at)}"),
    ]
    fills, fees = conn.execute(_FILLS, (first_at, last_at)).fetchone()
    items.append(Item(f"{fills} fills", f"fees {dollars(fees)}"))
    items.append(Item("backtest comparison", "not yet available"))
    return message(header, items)


# The basket.

_TARGETS = """
    SELECT symbol, weight, shares, held, carried, rank FROM agb.basket_target
    WHERE basket_id = %s
    ORDER BY weight DESC, symbol
"""


def basket(conn: psycopg.Connection) -> str:
    latest = conn.execute(_LATEST_BASKET).fetchone()
    if latest is None:
        return message("BASKET — none yet", [Item("nothing to show", "no live basket yet")])
    basket_id, reference, _, ranker, capital, state, since, actor = latest
    targets = conn.execute(_TARGETS, (basket_id,)).fetchall()
    header = (
        f"BASKET {reference} — {state.upper()} · {len(targets)} names · {dollars(capital)}\n"
        f"{state} {et(since)} by {actor}; model {ranker}"
    )
    items = []
    for symbol, weight, qty, held, carried, rank in targets:
        facts = [f"{shares(qty)} shares", "held" if held else "new"]
        if carried:
            facts.append("carried")
        if rank is not None:
            facts.append(f"rank {rank}")
        items.append(Item(f"{symbol}  {percent(weight)}", " · ".join(facts)))
    return message(header, items)


# A rebalance's diff: the names it sells, the names it buys, the incumbents it keeps, and the
# two aggregate numbers the gate measured. The approval request and the local UI both read it
# from here, so the two surfaces cannot describe the same basket differently.

# The book the basket trades from is the last filled live basket; a sold name's old rank is
# the rank it held there.
_SOLD = """
    SELECT o.symbol, r.rank, w.rank
    FROM agb.basket_order o
    LEFT JOIN agb.basket_rank r ON r.basket_id = o.basket_id AND r.symbol = o.symbol
    LEFT JOIN agb.basket_target w ON w.symbol = o.symbol AND w.basket_id = (
        SELECT id FROM agb.basket_state
        WHERE kind = 'live' AND state = 'filled'
        ORDER BY session_date DESC, id DESC
        LIMIT 1
    )
    WHERE o.basket_id = %s AND o.side = 'sell'
    ORDER BY r.rank DESC NULLS FIRST, o.symbol
"""
_BOUGHT = """
    SELECT o.symbol, r.rank
    FROM agb.basket_order o
    LEFT JOIN agb.basket_rank r ON r.basket_id = o.basket_id AND r.symbol = o.symbol
    WHERE o.basket_id = %s AND o.side = 'buy'
    ORDER BY r.rank NULLS LAST, o.symbol
"""
_KEPT = "SELECT count(*) FROM agb.basket_target WHERE basket_id = %s AND held"
_MEASURED = """
    SELECT c.name, c.measured FROM agb.gate_check c
    JOIN agb.gate_run r ON r.id = c.run_id
    WHERE r.basket_id = %s AND c.name IN ('cash', 'turnover') AND c.symbol IS NULL
"""


@dataclass(frozen=True)
class Diff:
    """What a basket changes about the book, as the decider saw it."""

    sold: list[tuple[str, int | None, int | None]]
    bought: list[tuple[str, int | None]]
    held: int
    measured: dict[str, str]

    @property
    def width(self) -> int:
        """The symbol column, wide enough for every name on both sides."""
        return max([6, *(len(symbol) + 1 for symbol, *_ in (*self.sold, *self.bought))])


def diff(conn: psycopg.Connection, basket_id: int) -> Diff:
    return Diff(
        sold=conn.execute(_SOLD, (basket_id,)).fetchall(),
        bought=conn.execute(_BOUGHT, (basket_id,)).fetchall(),
        held=conn.execute(_KEPT, (basket_id,)).fetchone()[0],
        measured=dict(conn.execute(_MEASURED, (basket_id,)).fetchall()),
    )


def rank_text(rank: int | None) -> str:
    return f"rank {rank}" if rank is not None else "not ranked"


def turnover_text(measured: str | None) -> str:
    if measured is None:
        return "turnover not measured"
    if measured == "no book":
        return "turnover none, no book yet"
    try:
        return f"turnover {Decimal(measured.rstrip('%')):.1f}%"
    except InvalidOperation:
        return f"turnover {measured}"


def cash_text(measured: str | None, capital: Decimal) -> str:
    try:
        return f"cash after {percent(Decimal(measured) / capital, 1)}"
    except (InvalidOperation, TypeError):
        return f"cash after {measured or 'not measured'}"


# The gate.

_LATEST_GATE = """
    SELECT r.id, r.basket_id, r.at, r.actor, b.kind, b.reference
    FROM agb.gate_run r
    JOIN agb.basket b ON b.id = r.basket_id
    WHERE b.kind = 'live'
    ORDER BY r.id DESC
    LIMIT 1
"""
_GATE_CHECKS = """
    SELECT 'gate', c.level, c.name, c.symbol, c.measured, c.threshold, c.passed
    FROM agb.gate_check c
    JOIN agb.gate_run r ON r.id = c.run_id
    WHERE r.basket_id = %(basket)s
    UNION ALL
    SELECT 'broker', c.level, c.name, c.symbol, c.measured, c.threshold, c.passed
    FROM agb.broker_check c
    JOIN agb.broker_check_run r ON r.id = c.run_id
    WHERE r.basket_id = %(basket)s
"""
_BROKER_RUN = "SELECT at FROM agb.broker_check_run WHERE basket_id = %s"


@dataclass(frozen=True)
class CheckRow:
    source: str
    level: str
    name: str
    symbol: str | None
    measured: str
    threshold: str
    passed: bool

    @property
    def label(self) -> str:
        return f"{self.name} {self.symbol}" if self.symbol else self.name


def checks(conn: psycopg.Connection, basket_id: int) -> list[CheckRow]:
    rows = conn.execute(_GATE_CHECKS, {"basket": basket_id}).fetchall()
    return [CheckRow(*row) for row in rows]


def failed(check: CheckRow) -> str:
    return f"  FAIL  {check.label}\n          measured {check.measured}, needs {check.threshold}"


def _sort_key(value: str) -> tuple:
    number = re.match(r"^-?[\d,]*\.?\d+", value.lstrip("$"))
    if number:
        return (0, float(number.group().replace(",", "")), value)
    return (1, 0.0, value)


def _passed(group: list[CheckRow]) -> str:
    """Passing checks of one name, one line pair: a single check shows its measurement; a
    check run on every order shows the range of what was measured."""
    first = group[0]
    if len(group) == 1:
        return f"  ok  {first.label}\n        measured {first.measured}, needs {first.threshold}"
    values = sorted({c.measured for c in group}, key=_sort_key)
    measured = values[0] if len(values) == 1 else f"{values[0]} to {values[-1]}"
    thresholds = {c.threshold for c in group}
    needs = first.threshold if len(thresholds) == 1 else "per order"
    return f"  ok  {first.name}, {len(group)} orders\n        measured {measured}, needs {needs}"


def gate(conn: psycopg.Connection) -> str:
    latest = conn.execute(_LATEST_GATE).fetchone()
    if latest is None:
        return message("GATE — no run yet", [Item("nothing to show", "the gate has not run")])
    _, basket_id, at, _, kind, reference = latest
    rows = checks(conn, basket_id)
    fails = [c for c in rows if not c.passed]
    header = f"GATE — {_basket_name(kind, reference, basket_id)}, run {et(at)}"
    broker_at = conn.execute(_BROKER_RUN, (basket_id,)).fetchone()
    if broker_at:
        header += f"; broker checks {et(broker_at[0])}"
    groups: dict[tuple[str, str], list[CheckRow]] = {}
    for check in rows:
        if check.passed:
            groups.setdefault((check.source, check.name), []).append(check)
    parts = [header, *(failed(c) for c in fails), *(_passed(g) for g in groups.values())]
    if fails:
        footer = f"{len(fails)} of {len(rows)} checks failed. The rebalance was halted."
    else:
        footer = f"{len(rows)} of {len(rows)} checks passed."
    parts.append(footer)
    return "\n\n".join(parts)


# Why a name is in the basket: the panel's output contract (docs/architecture/05).

# The month's blended basket: the latest live one a panel scored.
_WHY_BASKET = """
    SELECT b.id, b.reference, b.universe_size, b.panel_run_id, b.panel_w
    FROM agb.basket b
    WHERE b.kind = 'live' AND b.panel_run_id IS NOT NULL
    ORDER BY b.session_date DESC, b.id DESC
    LIMIT 1
"""
# The name's two terms, with its place among the shortlist: how many names were shortlisted,
# and how many the panel scored at least as safe as this one.
_WHY_BLEND = """
    SELECT l.stage1_rank, l.llm_total, l.llm_score, l.neutral,
           (SELECT count(*) FROM agb.basket_blend o WHERE o.basket_id = l.basket_id),
           (SELECT count(*) FROM agb.basket_blend o
            WHERE o.basket_id = l.basket_id AND NOT o.neutral AND o.llm_score >= l.llm_score)
    FROM agb.basket_blend l
    WHERE l.basket_id = %s AND l.symbol = %s
"""
_WHY_RETRIEVAL = """
    SELECT r.id, r.outcome, r.detail, r.delinquent, r.ipo, s.id, s.outcome, s.detail
    FROM agb.panel_retrieval r
    LEFT JOIN agb.panel_score s ON s.retrieval_id = r.id
    WHERE r.run_id = %s AND r.symbol = %s
"""
_WHY_DIMENSIONS = """
    SELECT d.dimension, d.score, d.reason, d.source, d.judge_score
    FROM agb.panel_dimension d
    WHERE d.score_id = %s
    ORDER BY d.dimension
"""
# What the Analyst read, as it was cited: one line per section of one filing.
_WHY_SOURCES = """
    SELECT f.document, f.form, f.filed, f.section, f.sent_chars, f.chars
    FROM agb.panel_filing f
    WHERE f.retrieval_id = %s
    ORDER BY f.filed DESC, f.document
"""

# What every /why ends with, because the score cannot be weighed without it.
SCRUBBED = (
    "The panel scored a name-scrubbed extract of the filings above. It was not told which "
    "company it was looking at, or its ticker, industry or market value."
)


def _ordinal(n: int) -> str:
    if n % 100 in (11, 12, 13):
        return f"{n}th"
    return f"{n}{ {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th') }"


def _dimension_lines(rows: Sequence[tuple], width: int) -> list[str]:
    """The six rubric dimensions, each with its 0-4 score. A dimension scoring at or below
    `blend.REASON_MAX` carries the Judge's one-sentence reason: a bare integer explains
    nothing, and the low ones are what the reader is deciding about. A score a filing rule
    set, rather than the Judge, says so and gives what the Judge had scored."""
    lines = []
    for dimension, score, reason, source, judge_score in rows:
        title = analyst.RUBRIC[dimension][0]
        lines.append(f"  {dimension}. {title:<{width}} {score}/{debate.DIMENSION_MAX}")
        if source == "rule":
            lines.append(f"       set by a filing rule; the Judge scored {judge_score}")
        if score <= blend.REASON_MAX:
            lines.append(f"       {reason}")
    return lines


def why(conn: psycopg.Connection, symbol: str) -> str:
    """Why a name scored as it did: its stage-1 rank, the panel's safety score with its place
    among the shortlist, every rubric dimension, the filings the panel read, and the fact that
    it read them scrubbed of the name. Nothing here is a recommendation."""
    symbol = symbol.upper()
    basket_row = conn.execute(_WHY_BASKET).fetchone()
    if basket_row is None:
        return message(
            f"WHY {symbol}", [Item("no panel has scored a basket", "there is nothing to explain")]
        )
    basket_id, reference, universe, run_id, w = basket_row
    header = f"WHY {symbol} — rebalance {reference}, blended at w = {w}"
    blended = conn.execute(_WHY_BLEND, (basket_id, symbol)).fetchone()
    if blended is None:
        return message(
            header,
            [Item(f"{symbol} was not shortlisted", "the panel scored only the shortlist")],
        )
    rank, total, score, neutral, shortlisted, safer = blended
    parts = [
        header,
        f"  stage-1 rank {rank} of {universe} ranked, shortlisted with {shortlisted} names",
    ]
    if neutral:
        parts.append(
            "  safety not scored: the blend gave it the neutral middle, so the panel moved it "
            "neither way"
        )
    else:
        parts.append(
            f"  safety {total}/{blend.MAX_TOTAL} ({score:.2f}), "
            f"{_ordinal(safer)} safest of the {shortlisted} shortlisted"
        )

    retrieval = conn.execute(_WHY_RETRIEVAL, (run_id, symbol)).fetchone()
    if retrieval is None:
        parts.append("  the panel has no record of this name in that run")
        return "\n\n".join(parts)
    retrieval_id, outcome, detail, delinquent, ipo, score_id, score_outcome, score_detail = (
        retrieval
    )
    if delinquent:
        parts.append(f"  delinquent filer ({delinquent}), which caps dimensions 1 and 2")
    if ipo:
        parts.append("  listed within the year, which sets dimension 6 to a neutral 2")
    if outcome != "retrieved":
        parts.append(f"  EDGAR: {outcome}, {detail}")
    elif score_outcome != "scored":
        parts.append(f"  the debate failed: {score_detail}")

    if score_id is not None and score_outcome == "scored":
        rows = conn.execute(_WHY_DIMENSIONS, (score_id,)).fetchall()
        width = max(len(analyst.RUBRIC[d][0]) for d, *_ in rows) + 1 if rows else 0
        parts.append("\n".join(["RUBRIC", *_dimension_lines(rows, width)]))

    sources = conn.execute(_WHY_SOURCES, (retrieval_id,)).fetchall()
    if sources:
        lines = [f"SOURCES ({len(sources)})"]
        for document, form, filed, section, sent, chars in sources:
            read = "in full" if sent >= chars else f"first {sent:,} of {chars:,} characters"
            lines.append(f"  {document}  {form} filed {filed}, {section}, {read}")
        parts.append("\n".join(lines))
    parts.append(SCRUBBED)
    return "\n\n".join(parts)


# The audit log.

_LOG = """
    SELECT at, actor, event, subject, detail FROM (
        SELECT * FROM agb.audit_log ORDER BY at DESC, source, row_id DESC LIMIT %s
    ) latest
    ORDER BY at, source, row_id
"""


def _cut(text: str, limit: int = DETAIL_MAX) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def log(conn: psycopg.Connection, n: int = LOG_DEFAULT) -> str:
    rows = conn.execute(_LOG, (n,)).fetchall()
    if not rows:
        return message("LOG — nothing recorded yet", [])
    items = []
    for at, actor, event, subject, detail in rows:
        facts = [f"by {actor}"]
        if subject:
            facts.append(_cut(subject))
        if detail:
            facts.append(_cut(detail))
        items.append(Item(f"{et(at)}  {event}", " · ".join(facts)))
    return message(f"LOG — last {len(rows)} events, oldest first", items)


# Unprompted: the fill confirmation and the gate-halt alert.

_ORDERS = """
    SELECT o.symbol, o.side, o.qty, coalesce(s.filled_qty, 0), s.avg_fill_price,
           s.shortfall_bps, o.modelled_cost_bps, coalesce(s.fees_usd, 0)
    FROM agb.broker_order o
    LEFT JOIN agb.shortfall s ON s.order_id = o.id
    WHERE o.basket_id = %s
    ORDER BY o.symbol
"""
_APPROVED = """
    SELECT actor, at FROM agb.basket_event
    WHERE basket_id = %s AND to_state = 'approved'
    ORDER BY seq DESC
    LIMIT 1
"""


def filled_text(conn: psycopg.Connection, basket_id: int, reference: str) -> str:
    orders = conn.execute(_ORDERS, (basket_id,)).fetchall()
    sold = [o[0] for o in orders if o[1] == "sell" and o[3] > 0]
    bought = [o[0] for o in orders if o[1] == "buy" and o[3] > 0]
    items = [
        Item(f"sold {len(sold)}", ", ".join(sold) or "nothing"),
        Item(f"bought {len(bought)}", ", ".join(bought) or "nothing"),
    ]
    full = [o for o in orders if o[3] >= o[2]]
    notional = sum((o[3] * o[4] for o in orders if o[4] is not None), Decimal(0))
    fees = sum((o[7] for o in orders), Decimal(0))
    if notional:
        shortfall = sum((o[5] * o[3] * o[4] for o in orders if o[4] is not None), Decimal(0))
        modelled = sum((o[6] * o[3] * o[4] for o in orders if o[4] is not None), Decimal(0))
        facts = (
            f"shortfall {shortfall / notional:+.1f} bp vs mid, modelled "
            f"{modelled / notional:.1f} bp; {dollars(notional)} traded, fees {dollars(fees)}"
        )
    else:
        facts = "nothing traded"
    items.append(Item(f"{len(full)} of {len(orders)} orders filled in full", facts))
    for symbol, side, qty, filled_qty, *_ in orders:
        if filled_qty < qty:
            items.append(
                Item(
                    f"filled short: {symbol}", f"{side} {shares(qty)}, filled {shares(filled_qty)}"
                )
            )
    approved = conn.execute(_APPROVED, (basket_id,)).fetchone()
    if approved:
        items.append(Item(f"approved by {approved[0]}", f"at {et(approved[1])}"))
    return message(f"REBALANCE {reference} filled at the close.", items, "/positions")


HALT_FOOTER = (
    "Nothing was sent to the broker and no position was changed. "
    "The halt stays open until it is cleared."
)


def halted_text(conn: psycopg.Connection, basket_id: int, reference: str) -> str | None:
    """The gate-halt alert, or None when the basket was halted for no failed check."""
    fails = [c for c in checks(conn, basket_id) if not c.passed]
    if not fails:
        return None
    parts = [
        f"HALTED — rebalance {reference} was not submitted.",
        *(failed(c) for c in fails),
        HALT_FOOTER,
        "/gate for every check",
    ]
    return "\n\n".join(parts)
