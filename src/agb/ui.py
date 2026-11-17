"""The local UI's pages, rendered from the database by the server beside it
(docs/architecture/06-local-ui.md). Every page is a read, and a page renders on a connection
the database itself holds read-only, so a mistake on a page cannot become a write. The two
write paths are forms that post to their own routes, each of which takes a connection of its
own, and `WRITES` is the whole list of them.

The shell is the decision log, one row per rebalance cycle, newest first: the state it
reached, who decided it and from which surface, the realised return of the month it traded,
and the turnover it asked for. A row expands, with no JavaScript, into the basket diff the
decider saw, the gate's result, that cycle's flags and every audit-log line naming it. The
landing page is the log rather than a basket with a green button, because a page whose single
action is approval teaches the operator to rubber-stamp it.

Above the log sits the health banner: stale heartbeats, overdue expected events, open halt
causes, the broker and position mismatches, and undelivered alerts. It is the only place an
undelivered alert is visible at all, so a Telegram outage cannot hide a month's approval
request. A dead scheduler reads here as a stale heartbeat and a rebalance nothing has
scheduled, never as an empty page.

The views beside the log — Rebalance, Positions, Gate, Validation, Promotion, Performance,
Panel, Alerts and the kill switch — are the same state read nine other ways. Each speaks the
Telegram bot's vocabulary, from agb.readout, so the two surfaces cannot describe one basket in
two dialects.

Four of them are the evidence views, and each is written so that a figure cannot be read out of
context. Validation states every gate beside the threshold it was applied against and ends by
naming what the certification covers, which is the classical ranker alone. Promotion is the
ladder with the date, the actor and the evidence behind every transition it has made. On
Performance no live figure appears without the backtest expectation beside it, and the page says
outright that a handful of months cannot tell a model that works from one that has been lucky.
Panel is the safety panel's own record, name by name, with the Judge's reason on each dimension
and the note that the panel never learns which company it is reading about. None of the four has
a write path: nothing on them appears in `WRITES`.

The writes are exactly Telegram's two: answering the rebalance, and the kill switch. Neither
is written here. Answering goes through `agb.approval` and the switch through
`agb.killswitch`, the same functions the bot calls, with a source naming this surface, so the
semantics cannot drift between the two surfaces and the race between them is settled in one
place. Approving is never one click: the operator types the rebalance's reference, which is
the friction Telegram's typed command has and the whole reason the landing page is the log
rather than a basket with a green button. Every write is a `command` row with the actor and
the surface on it, refused ones included.

Pause, Resume and Ask to liquidate are not one click either. Each is a link from the kill switch
page to a confirmation page (`/confirm/...`, a GET that reads and writes nothing) which says what
will change, and the write is the form on that page, posted to the same route as before so the
server's host, origin and redirect protections apply to it unchanged. Resume's page lists the
open causes with a tick box each, unticked; only the ticked ones are cleared, and a form with none
ticked is refused and recorded.

A write returns an `Outcome` rather than a page: the server stores it and answers the POST with
a 303 to `/outcome?id=<token>`, which `outcome` renders, so reloading the page the operator ends
on is a GET and cannot write twice. A write that raises is `failed`: a 500 page that names the
cause and nothing else of the exception. The reference of a basket is read from the form's
reference field alone; the reason field is only ever a reason.
"""

import html
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, DivisionByZero, InvalidOperation

import psycopg

from agb import analyst, approval, debate, health, killswitch, ladder, readout
from agb.health import et
from agb.nyse import NyseCalendar, trading_date

# The log and the views, in the order the header shows them.
VIEWS = (
    ("/", "Log"),
    ("/rebalance", "Rebalance"),
    ("/positions", "Positions"),
    ("/gate", "Gate"),
    ("/validation", "Validation"),
    ("/promotion", "Promotion"),
    ("/performance", "Performance"),
    ("/panel", "Panel"),
    ("/alerts", "Alerts"),
    ("/killswitch", "Kill switch"),
)
# The most cycles the log shows, and the most alerts the Alerts view shows.
LOG_CYCLES = 24
ALERT_MAX = 50
# How the pages connect: as the `ui` role from the PG* environment, and read-only, so the
# database refuses a write even if a page ever tried one. The write routes connect without it.
READ_ONLY = "-c default_transaction_read_only=on"
# Who a write from this surface is attributed to, and the surface it is attributed to. There is
# no login here — SSH is the authentication — so there is one operator, named as such, and the
# audit trail says which surface they were sitting at.
ACTOR = "ui:operator"
SURFACE = "ui"

STYLE = """
:root { color-scheme: light dark; --line: #d6d6d6; --dim: #666; --bad: #a40000;
        --warn: #8a5300; --ok: #14612c; }
* { box-sizing: border-box; }
body { margin: 0; font: 14px/1.5 system-ui, sans-serif; }
header { border-bottom: 1px solid var(--line); padding: 10px 20px; display: flex;
         gap: 18px; align-items: baseline; flex-wrap: wrap; }
header b { font-size: 15px; }
header a { color: inherit; text-decoration: none; padding: 2px 0; }
header a.on { border-bottom: 2px solid currentColor; }
main { padding: 20px; max-width: 1100px; }
h2 { font-size: 15px; margin: 24px 0 8px; }
h3 { font-size: 13px; margin: 14px 0 4px; color: var(--dim); text-transform: uppercase;
     letter-spacing: .04em; }
p.note { color: var(--dim); margin: 6px 0; }
table { border-collapse: collapse; width: 100%; margin: 4px 0 10px; }
th, td { text-align: left; padding: 4px 10px 4px 0; border-bottom: 1px solid var(--line);
         white-space: nowrap; }
th { color: var(--dim); font-weight: 600; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
td.text, th.text { white-space: normal; min-width: 22em; }
.pair { display: flex; gap: 32px; flex-wrap: wrap; }
.pair > div { flex: 1 1 260px; }
.banner { border: 1px solid var(--line); border-left-width: 4px; padding: 8px 12px;
          margin-bottom: 16px; }
.banner.clear { border-left-color: var(--ok); }
.banner.unwell { border-left-color: var(--bad); }
.banner div { padding: 2px 0; }
.banner span { color: var(--dim); }
.row { display: grid; grid-template-columns: 7em 6em 1fr 6em 6em 6em;
       gap: 12px; align-items: baseline; }
.row .num { text-align: right; font-variant-numeric: tabular-nums; }
.head { color: var(--dim); padding: 4px 0; border-bottom: 1px solid var(--line); }
details.cycle { border-bottom: 1px solid var(--line); }
details.cycle > summary { padding: 6px 0; cursor: pointer; }
details.cycle[open] > summary { font-weight: 600; }
.detail { padding: 4px 0 14px 1.5em; }
.state { font-size: 12px; }
.ok { color: var(--ok); } .bad { color: var(--bad); } .warn { color: var(--warn); }
.dim { color: var(--dim); }
footer { color: var(--dim); padding: 0 20px 24px; max-width: 1100px; }
form.write { border: 1px solid var(--line); padding: 10px 12px; margin: 8px 0;
             max-width: 620px; }
form.write label { display: block; margin: 0 0 8px; }
form.write input { font: inherit; padding: 4px 6px; width: 14em; margin-top: 4px; }
form.write button { font: inherit; padding: 4px 14px; }
pre.reply { border: 1px solid var(--line); border-left-width: 4px; padding: 10px 12px;
            white-space: pre-wrap; max-width: 620px; }
"""

# A basket's state, coloured by what it means for the month: it traded, it is waiting, it
# will not trade, or something broke.
STATE_CLASS = {
    "proposed": "warn",
    "pending": "warn",
    "approved": "ok",
    "submitted": "ok",
    "filled": "ok",
    "unfilled": "bad",
    "rejected": "dim",
    "missed": "dim",
    "halted": "bad",
}


def connect(write: bool = False) -> psycopg.Connection:
    """A connection for one request: the `ui` role from the PG* environment, read-only unless
    the request is one of the write routes. A page gets the read-only one, so the database
    itself stands behind the claim that rendering cannot write."""
    return psycopg.connect(
        application_name="agb-ui", options="" if write else READ_ONLY, autocommit=True
    )


class Html(str):
    """Markup that is already escaped. `esc` leaves it alone; everything else it escapes."""


def esc(value: object) -> Html:
    if isinstance(value, Html):
        return value
    return Html(html.escape("" if value is None else str(value)))


def tag(name: str, body: object, **attrs: str) -> Html:
    rendered = "".join(f' {key.rstrip("_")}="{esc(value)}"' for key, value in attrs.items())
    return Html(f"<{name}{rendered}>{esc(body)}</{name}>")


def join(parts: Iterable[object]) -> Html:
    return Html("".join(esc(part) for part in parts))


def note(text: str) -> Html:
    return tag("p", text, class_="note")


def section(heading: str, body: Iterable[object]) -> Html:
    return join([tag("h2", heading), *body])


def table(
    headers: Sequence[str],
    rows: Iterable[Sequence[object]],
    numeric: Sequence[int] = (),
    wrap: Sequence[int] = (),
) -> Html:
    """One table. `numeric` is the columns that read as figures, right-aligned on a tabular
    figure; `wrap` is the columns that carry a sentence, which are the only ones allowed to
    wrap, since a reason the Judge wrote is prose and not a cell."""

    def cell(kind: str, index: int, value: object) -> str:
        if index in numeric:
            return f'<{kind} class="num">{esc(value)}</{kind}>'
        if index in wrap:
            return f'<{kind} class="text">{esc(value)}</{kind}>'
        return f"<{kind}>{esc(value)}</{kind}>"

    head = "".join(cell("th", i, header) for i, header in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(cell("td", i, value) for i, value in enumerate(row)) + "</tr>"
        for row in rows
    )
    return Html(f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>")


def write_form(action: str, button: str, fields: Sequence[tuple[str, str]] = ()) -> Html:
    """One write as a plain form posting to one of `WRITES`. The fields are what the operator
    has to type; nothing is filled in for them, because a field that arrives already correct is
    the one-click button this surface is not allowed to have."""
    body: list[object] = []
    for name, label in fields:
        body.append(
            Html(
                f"<label>{esc(label)}<br>"
                f'<input type="text" name="{esc(name)}" autocomplete="off" spellcheck="false">'
                "</label>"
            )
        )
    body.append(tag("button", button, type="submit"))
    return Html(f'<form class="write" method="post" action="{esc(action)}">{join(body)}</form>')


def page(title: str, path: str, blocks: Iterable[object]) -> str:
    nav = join(
        tag("a", label, href=href, class_="on" if href == path else "") for href, label in VIEWS
    )
    return (
        "<!doctype html>\n"
        f'<html lang="en"><head><meta charset="utf-8">'
        f'<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{esc(title)} — agb-trader</title><style>{STYLE}</style></head><body>"
        f"<header><b>agb-trader</b>{nav}</header><main>{join(blocks)}</main>"
        "<footer>Read-mostly: the pages read, and the only writes are answering a "
        "rebalance and the kill switch, both recorded against this surface. Nothing here "
        "resizes a position, edits the universe, overrides the ranker, moves the ladder or "
        "promotes a model.</footer>"
        "</body></html>\n"
    )


# The health banner.

_UNDELIVERED = """
    SELECT kind, subject, period, raised_at, failures, last_failure
    FROM agb.undelivered_alert
    ORDER BY raised_at
"""


def banner(conn: psycopg.Connection, now: datetime) -> Html:
    """What is unhealthy, in one block above everything else. `health.stale_heartbeats` leaves
    out the reader's own service, so the UI never reports itself; every other service,
    the bot included, is in here."""
    findings = [
        *health.stale_heartbeats(conn, now, health.stale_after_from_env()),
        *health.overdue(conn, now),
        *health.open_halt_causes(conn),
        *health.broker_mismatch(conn),
        *health.position_mismatch(conn),
    ]
    lines = [(finding.what, finding.facts) for finding in findings]
    for kind, subject, period, raised_at, failures, last_failure in conn.execute(_UNDELIVERED):
        lines.append(
            (
                f"alert not delivered: {kind} {subject} {period}",
                f"raised {et(raised_at)}, {failures} failed attempts"
                + (f"; last {last_failure}" if last_failure else ""),
            )
        )
    if not lines:
        upcoming = conn.execute(readout._NEXT_SUBMISSION, (now,)).fetchone()
        next_cycle = (
            f"next rebalance {upcoming[0]}, orders due by {et(upcoming[1])}"
            if upcoming
            else "no rebalance is scheduled"
        )
        body = tag("div", join([tag("b", "Healthy"), esc(f" — {next_cycle}")]))
        return tag("div", body, class_="banner clear")
    body = join(
        tag("div", join([tag("b", what), tag("span", f" — {facts}")])) for what, facts in lines
    )
    return tag("div", body, class_="banner unwell")


# The decision log.

_CYCLES = """
    SELECT s.id, s.reference, s.period, s.state, s.since, s.actor, s.surface, b.capital_usd,
           b.ranker, (SELECT count(*) FROM agb.basket_target t WHERE t.basket_id = s.id)
    FROM agb.basket_state s
    JOIN agb.basket b ON b.id = s.id
    WHERE s.kind = 'live'
    ORDER BY s.session_date DESC, s.id DESC
    LIMIT %s
"""
# Every audit-log line about one cycle. The log names a basket by the string the view builds
# for it, so this catches its state changes and its halts alike.
_CYCLE_EVENTS = """
    SELECT subject, at, actor, event, detail FROM agb.audit_log
    WHERE subject = ANY(%s)
    ORDER BY subject, at, event
"""
# A cycle's flags: the anomalies the system raised for the month it trades. The panel's
# name-level flags join here once the panel runs on the live path; a quiet month stays quiet.
_PERIOD_ALERTS = """
    SELECT period, kind, subject, raised_at, raised_by FROM agb.alert
    WHERE period = ANY(%s)
    ORDER BY period, raised_at, id
"""


@dataclass(frozen=True)
class _CycleReads:
    """Everything the cycles of the log read besides the cycles themselves, each read once
    for all of them and keyed by what a cycle looks it up by: a page that listed twenty-four
    cycles asked the database about each of them one at a time, over two hundred queries."""

    diffs: dict[int, readout.Diff]
    windows: dict[str | None, readout.Window | None]
    checks: dict[int, list[readout.CheckRow]]
    events: dict[str, list[tuple]]
    flags: dict[str, list[tuple]]

    @classmethod
    def of(cls, conn: psycopg.Connection, cycles: Sequence[Sequence]) -> "_CycleReads":
        ids = [row[0] for row in cycles]
        periods = sorted({row[2] for row in cycles})
        events: dict[str, list[tuple]] = {}
        for subject, *event in conn.execute(
            _CYCLE_EVENTS, ([f"live basket {row[1]}" for row in cycles],)
        ).fetchall():
            events.setdefault(subject, []).append(tuple(event))
        flags: dict[str, list[tuple]] = {}
        for period, *flag in conn.execute(_PERIOD_ALERTS, (periods,)).fetchall():
            flags.setdefault(period, []).append(tuple(flag))
        return cls(
            diffs=readout.diffs(conn, ids),
            windows=readout.equity_windows(conn, periods),
            checks=readout.checks_of(conn, ids),
            events=events,
            flags=flags,
        )


def period_return(window: readout.Window | None) -> tuple[str, str]:
    """A month's realised return and the window it was measured over, from the same two
    equity readings `/pnl` uses."""
    if window is None:
        return "n/a", "no equity readings in the month"
    first_at, first, last_at, last = window
    if not first:
        return "n/a", f"equity {readout.dollars(first)} at {et(first_at)}"
    return (
        readout.signed_percent(last / first - 1),
        f"equity {readout.dollars(first)} to {readout.dollars(last)} "
        f"({et(first_at)} to {et(last_at)})",
    )


def _diff_blocks(diff: readout.Diff, capital: Decimal) -> Html:
    sold = table(
        ["Out", "Rank", "Was"],
        [
            (symbol, readout.rank_text(rank), was if was is not None else "—")
            for symbol, rank, was in diff.sold
        ],
    )
    bought = table(
        ["In", "Rank"], [(symbol, readout.rank_text(rank)) for symbol, rank in diff.bought]
    )
    numbers = (
        f"{readout.turnover_text(diff.measured.get('turnover'))}  ·  "
        f"{readout.cash_text(diff.measured.get('cash'), capital)}  ·  "
        f"{diff.held} held unchanged"
    )
    return join(
        [
            Html(f'<div class="pair"><div>{sold}</div><div>{bought}</div></div>'),
            note(numbers),
        ]
    )


def _gate_block(rows: list[readout.CheckRow]) -> Html:
    if not rows:
        return note("the gate has not run on this basket")
    fails = [check for check in rows if not check.passed]
    verdict = f"{len(rows) - len(fails)} of {len(rows)} checks passed"
    if not fails:
        return note(verdict)
    return join(
        [
            note(verdict),
            table(
                ["Failed", "Measured", "Needs", "Level"],
                [(c.label, c.measured, c.threshold, c.level) for c in fails],
            ),
        ]
    )


def _flags_block(rows: list[tuple]) -> Html:
    if not rows:
        return note("none")
    return table(
        ["Flag", "Subject", "Raised", "By"],
        [(kind, subject, et(at), by) for kind, subject, at, by in rows],
    )


def _cycle(reads: _CycleReads, row: Sequence) -> Html:
    basket_id, reference, period, state, since, actor, surface, capital, ranker, names = row
    diff = reads.diffs[basket_id]
    returned, window = period_return(reads.windows[period])
    turnover = readout.turnover_text(diff.measured.get("turnover")).removeprefix("turnover ")
    summary = Html(
        f'<summary><span class="row">'
        f"<span>{esc(reference)}</span>"
        f'<span class="state {STATE_CLASS.get(state, "")}">{esc(state)}</span>'
        f'<span class="dim">{esc(f"{et(since)} · {actor} · {surface}")}</span>'
        f'<span class="num">{esc(returned)}</span>'
        f'<span class="num">{esc(turnover)}</span>'
        f'<span class="num">{esc(f"{names} names")}</span>'
        f"</span></summary>"
    )
    events = reads.events.get(f"live basket {reference}", [])
    detail = join(
        [
            tag("h3", "What it changed"),
            _diff_blocks(diff, capital),
            tag("h3", "Return"),
            note(f"{returned} over {period}: {window}"),
            tag("h3", "Gate"),
            _gate_block(reads.checks[basket_id]),
            tag("h3", "Flags"),
            _flags_block(reads.flags.get(period, [])),
            tag("h3", "Events"),
            table(
                ["When", "Event", "Actor", "Detail"],
                [(et(at), event, by, detail or "") for at, by, event, detail in events],
            ),
            note(f"model {ranker} · capital {readout.dollars(capital)} · basket {basket_id}"),
        ]
    )
    return Html(f'<details class="cycle">{summary}<div class="detail">{detail}</div></details>')


_LOG_HEAD = Html(
    '<div class="row head"><span>Rebalance</span><span>State</span><span>Decided</span>'
    '<span class="num">Return</span><span class="num">Turnover</span>'
    '<span class="num">Names</span></div>'
)


def log(conn: psycopg.Connection, now: datetime) -> str:
    """The home view: every rebalance cycle, newest first, each one expanding into what the
    decider saw."""
    cycles = conn.execute(_CYCLES, (LOG_CYCLES,)).fetchall()
    body: list[object] = [note("No rebalance has been proposed yet.")]
    if cycles:
        reads = _CycleReads.of(conn, cycles)
        body = [_LOG_HEAD, *(_cycle(reads, row) for row in cycles)]
    return page("Decision log", "/", [banner(conn, now), section("Decision log", body)])


# The Rebalance view.

_TARGETS = """
    SELECT symbol, rank, weight, shares, held, carried FROM agb.basket_target
    WHERE basket_id = %s
    ORDER BY weight DESC, symbol
"""
_ORDERS = """
    SELECT side, symbol, qty, reference_price, notional_usd, modelled_cost_bps, client_order_id
    FROM agb.basket_order
    WHERE basket_id = %s
    ORDER BY side, symbol
"""


def _standing(held: bool, carried: bool) -> str:
    if carried:
        return "carried"
    return "held" if held else "new"


def rebalance(conn: psycopg.Connection, now: datetime) -> str:
    """The latest live basket: what it holds, what it trades and where it stands."""
    latest = conn.execute(_CYCLES, (1,)).fetchone()
    if latest is None:
        body: list[object] = [note("No live basket has been proposed yet.")]
        return page("Rebalance", "/rebalance", [banner(conn, now), section("Rebalance", body)])
    basket_id, reference, period, state, since, actor, surface, capital, ranker, _ = latest
    diff = readout.diff(conn, basket_id)
    waiting = state in ("proposed", "pending")
    blocks: list[object] = [
        note(f"{state} since {et(since)} by {actor} ({surface}) · model {ranker} · {period}"),
    ]
    if waiting:
        blocks += [
            note("No answer by the session's cutoff means no trades this month."),
            tag("h3", "Your answer"),
            note(
                f"Type the reference, {reference}, into the box to answer. There is no button "
                "that answers on its own: typing it is the deliberation."
            ),
            write_form("/approve", "Approve", [("reference", "Rebalance reference")]),
            write_form(
                "/reject",
                "Reject",
                [("reference", "Rebalance reference"), ("reason", "Reason, if you want one")],
            ),
        ]
    blocks += [
        tag("h3", "Diff"),
        _diff_blocks(diff, capital),
        tag("h3", "Targets"),
        table(
            ["Symbol", "Rank", "Weight", "Shares", "Standing"],
            [
                (symbol, rank, readout.percent(weight), readout.shares(qty), _standing(h, c))
                for symbol, rank, weight, qty, h, c in conn.execute(_TARGETS, (basket_id,))
            ],
            numeric=(1, 2, 3),
        ),
        tag("h3", "Orders"),
        table(
            ["Side", "Symbol", "Qty", "Reference", "Notional", "Cost bps", "Client order id"],
            [
                (
                    side,
                    symbol,
                    readout.shares(qty),
                    readout.dollars(price) if price is not None else "—",
                    readout.dollars(notional) if notional is not None else "—",
                    bps if bps is not None else "—",
                    order_id,
                )
                for side, symbol, qty, price, notional, bps, order_id in conn.execute(
                    _ORDERS, (basket_id,)
                )
            ],
            numeric=(2, 3, 4, 5),
        ),
    ]
    return page(
        f"Rebalance {reference}",
        "/rebalance",
        [banner(conn, now), section(f"Rebalance {reference}", blocks)],
    )


# The Positions view.

_POSITIONS = """
    SELECT p.symbol, p.qty, p.avg_entry_price, p.market_value_usd
    FROM agb.position p
    WHERE p.snapshot_id = %s
    ORDER BY p.market_value_usd DESC NULLS LAST, p.symbol
"""
# Each name's stage-1 rank in the latest live basket.
_RANKS = """
    SELECT symbol, rank FROM agb.basket_rank
    WHERE basket_id = (SELECT max(id) FROM agb.basket WHERE kind = 'live')
"""
# The panel's safety score per name, from the latest run that scored. The score is on the
# retrieval, which is what carries the symbol.
_SCORES = """
    SELECT r.symbol, s.score, s.total
    FROM agb.panel_score s
    JOIN agb.panel_retrieval r ON r.id = s.retrieval_id
    WHERE s.outcome = 'scored'
      AND s.run_id = (SELECT max(run_id) FROM agb.panel_score WHERE outcome = 'scored')
"""
# How long a name has been held: the first snapshot of its current unbroken run, which is
# every snapshot after the last one that did not hold it.
_HELD_SINCE = """
    SELECT p.symbol, min(s.snapshot_date)
    FROM agb.position p
    JOIN agb.position_snapshot s ON s.id = p.snapshot_id
    WHERE s.id > coalesce((
        SELECT max(gap.id) FROM agb.position_snapshot gap
        WHERE NOT EXISTS (
            SELECT 1 FROM agb.position held
            WHERE held.snapshot_id = gap.id AND held.symbol = p.symbol
        )
    ), 0)
    GROUP BY p.symbol
"""


def _entry_return(qty: Decimal, entry: Decimal | None, value: Decimal | None) -> str:
    """What the name has made since the price it was entered at."""
    try:
        return readout.signed_percent(value / (qty * entry) - 1)
    except (TypeError, InvalidOperation, DivisionByZero, ZeroDivisionError):
        return "—"


def positions(conn: psycopg.Connection, now: datetime) -> str:
    """The latest broker snapshot: every holding with its weight, its return since entry, how
    long it has been held, the panel's safety score and its current rank."""
    snapshot = conn.execute(readout._SNAPSHOT).fetchone()
    if snapshot is None:
        blocks: list[object] = [
            note("Execution snapshots the broker's positions after each close; none yet.")
        ]
        return page("Positions", "/positions", [banner(conn, now), section("Positions", blocks)])
    snapshot_id, snapshot_date, count, total = snapshot
    ranks = dict(conn.execute(_RANKS).fetchall())
    scores = {symbol: (score, total_) for symbol, score, total_ in conn.execute(_SCORES)}
    since = dict(conn.execute(_HELD_SINCE).fetchall())
    rows = []
    for symbol, qty, entry, value in conn.execute(_POSITIONS, (snapshot_id,)):
        score = scores.get(symbol)
        rows.append(
            (
                symbol,
                readout.shares(qty),
                readout.dollars(value) if value is not None else "—",
                readout.percent(value / total, 1) if value is not None and total else "—",
                _entry_return(qty, entry, value),
                since.get(symbol) or "—",
                f"{score[0]:.2f} ({score[1]}/24)" if score else "—",
                ranks.get(symbol) if symbol in ranks else "—",
            )
        )
    blocks = [
        note(f"{count} held, {readout.dollars(total)}, snapshot {snapshot_date}"),
        table(
            ["Symbol", "Shares", "Value", "Weight", "Since entry", "Held since", "Panel", "Rank"],
            rows,
            numeric=(1, 2, 3, 4, 6, 7),
        ),
    ]
    return page("Positions", "/positions", [banner(conn, now), section("Positions", blocks)])


# The Gate view.


def gate(conn: psycopg.Connection, now: datetime) -> str:
    """The latest gate run on a live basket: what failed first, then what passed."""
    latest = conn.execute(readout._LATEST_GATE).fetchone()
    if latest is None:
        blocks: list[object] = [note("The gate has not run yet.")]
        return page("Gate", "/gate", [banner(conn, now), section("Gate", blocks)])
    _, basket_id, at, actor, kind, reference = latest
    rows = readout.checks(conn, basket_id)
    fails = [check for check in rows if not check.passed]
    heading = f"Gate — rebalance {reference}"
    facts = f"run {et(at)} by {actor}"
    broker_at = conn.execute(readout._BROKER_RUN, (basket_id,)).fetchone()
    if broker_at:
        facts += f" · broker checks {et(broker_at[0])}"
    verdict = (
        f"{len(fails)} of {len(rows)} checks failed. The rebalance was halted."
        if fails
        else f"{len(rows)} of {len(rows)} checks passed."
    )
    blocks = [
        note(facts),
        note(verdict),
        table(
            ["Check", "Source", "Level", "Measured", "Needs", "Result"],
            [
                (
                    c.label,
                    c.source,
                    c.level,
                    c.measured,
                    c.threshold,
                    "pass" if c.passed else "FAIL",
                )
                for c in sorted(rows, key=lambda c: (c.passed, c.source, c.name, c.symbol or ""))
            ],
        ),
    ]
    return page(heading, "/gate", [banner(conn, now), section(heading, blocks)])


# The Validation view.

# The live certification, or no row when nothing has been promoted. `agb.promoted_model` is the
# latest promotion alone, so this is at most one row.
_PROMOTED = """
    SELECT certification_id, model_id, passed, declared_trials, cost_model_version,
           artifact_sha256, promoted_by, promoted_at, voided, voided_at, voided_reason
    FROM agb.promoted_model
"""
# The certification the Validation view reports on: the promoted one when there is one, and
# otherwise the newest imported, so a certification that failed its gates is evidence on the page
# rather than an empty one.
_CERTIFICATION = """
    SELECT id, model_id, certifies, passed, certified_at, actor, declared_trials, dsr, pbo,
           sharpe_net, dsr_2x, break_even_multiple, sharpe_gross, gross_sharpe_flag,
           certified_capital, cost_model_version, train_start, train_end, gates, path_quantiles
    FROM agb.model_certification
    WHERE %(id)s::bigint IS NULL OR id = %(id)s
    ORDER BY id DESC
    LIMIT 1
"""
# The four gates docs/architecture/08-research.md names, each with the column its measurement is
# stored in and the decimals that measurement is read at. The threshold, the rule and the verdict
# come from the `gates` object, which is what the research profile recorded when it applied them:
# the page states each gate as it was applied, never as this module would reapply it.
GATES = (
    ("dsr", "Deflated Sharpe", 3),
    ("pbo", "Probability of backtest overfitting", 3),
    ("sharpe_net", "Cost-inclusive Sharpe", 2),
    ("dsr_2x", "Deflated Sharpe at 2x cost", 3),
)
# The gates that carry their measurement only in the `gates` object, with no column of their own:
# the suspicion check and the start-date check, binding since issue #187. A gate this module does
# not know is still listed, under its key.
LATER_GATES = {
    "gross_sharpe": "Highest annualised Sharpe (suspicion check)",
    "start_date": "Lowest Sharpe from a later start date",
}
# The returns a Sharpe gate read, as each gate records it (issue #167). Certifications up to
# lgbm-2026-10 recorded no basis and gated total returns.
BASES = {"total": "total return", "excess_spy": "excess over SPY"}
# The CPCV path distribution: what each measure is called, whether it reads as a percentage, and
# whether a lower live reading is the worse one. The last decides which end of the distribution
# the live column is marked against.
PATHS = (
    ("max_drawdown", "Worst drawdown", True, True),
    ("longest_losing_run", "Longest losing run", False, False),
    ("tracking_dispersion", "Tracking dispersion", True, False),
    ("monthly_return", "Average month", True, True),
)
# The quantiles a certification records, in the order they are read.
QUANTILES = (
    ("min", "Min"),
    ("q05", "5th"),
    ("q25", "25th"),
    ("q50", "Median"),
    ("q75", "75th"),
    ("q95", "95th"),
    ("max", "Max"),
)


def _as_float(value: object) -> float | None:
    """One number out of a jsonb object, which carries them as ints, floats or nothing."""
    return None if value is None else float(value)  # type: ignore[arg-type]


def _number(value: float | None, places: int = 3) -> str:
    return "—" if value is None else f"{value:.{places}f}"


def _fraction(value: float | None, places: int = 2) -> str:
    return "—" if value is None else f"{value * 100:.{places}f}%"


def _measure(value: float | None, percent: bool) -> str:
    """One path measurement, read as a percentage or as a count of months."""
    if value is None:
        return "—"
    return _fraction(value) if percent else f"{value:.2f}"


def _live_paths(months: Sequence[health.Month], expected: float | None) -> dict[str, float | None]:
    """The four certified path measures, measured on the months the account has actually lived.
    The dispersion is measured around the certified average month, which is what `health.tracking`
    compares against, so the page and the alert read one number."""
    if not months:
        return {}
    worst, _ = health.worst_drawdown(months)
    longest, _ = health.longest_losing_run(months)
    return {
        "max_drawdown": worst,
        "longest_losing_run": float(longest),
        "tracking_dispersion": None if expected is None else health.scatter(months, expected),
        "monthly_return": statistics.fmean(month.total_return for month in months),
    }


def _path_table(quantiles: Mapping[str, object], live: Mapping[str, float | None]) -> Html:
    """The path distribution with the live account's own measurement in the first column, marked
    when it is past the end of the distribution the certification explored. A live figure is never
    shown here without the distribution it is read against."""
    rows = []
    for key, label, percent, low in PATHS:
        spread = quantiles.get(key) or {}
        assert isinstance(spread, Mapping)
        measured = live.get(key)
        edge = _as_float(spread.get("q05" if low else "q95"))
        outside = (
            measured is not None
            and edge is not None
            and (measured < edge if low else measured > edge)
        )
        text = _measure(measured, percent)
        rows.append(
            (
                label,
                tag("span", text, class_="bad") if outside else text,
                *(_measure(_as_float(spread.get(q)), percent) for q, _ in QUANTILES),
            )
        )
    headers = ["Measure", "Live", *(label for _, label in QUANTILES)]
    return table(headers, rows, numeric=range(1, len(headers)))


def validation(conn: psycopg.Connection, now: datetime) -> str:
    """What the live model's certification says: every gate beside the threshold it was applied
    against, the same deflated Sharpe at twice the modelled cost, the break-even cost multiple,
    the trials it was deflated by, and the CPCV path distribution with the account's own drawdown
    marked on it. It ends where it has to: all of this certifies stage 1."""
    promoted = conn.execute(_PROMOTED).fetchone()
    row = conn.execute(_CERTIFICATION, {"id": promoted[0] if promoted else None}).fetchone()
    if row is None:
        empty = [note("No model has been certified. Nothing here has been validated yet.")]
        return page("Validation", "/validation", [banner(conn, now), section("Validation", empty)])
    (
        certification_id,
        model_id,
        certifies,
        passed,
        certified_at,
        actor,
        trials,
        dsr,
        pbo,
        sharpe_net,
        dsr_2x,
        break_even,
        sharpe_gross,
        gross_flag,
        capital,
        cost_model,
        train_start,
        train_end,
        gates,
        quantiles,
    ) = row
    measured = {"dsr": dsr, "pbo": pbo, "sharpe_net": sharpe_net, "dsr_2x": dsr_2x}
    gate_rows = []
    for key, label, places in GATES:
        applied = gates.get(key) or {}
        needs = (
            f"{applied.get('rule', '')} {_number(_as_float(applied.get('threshold')), places)}"
            if applied.get("threshold") is not None
            else "—"
        )
        if key == "sharpe_net":
            basis = applied.get("basis", "total")
            label = f"{label} ({BASES.get(basis, basis)})"
        gate_rows.append(
            (
                label,
                _number(_as_float(measured[key]), places),
                needs.strip(),
                "pass" if applied.get("passed") else "FAIL" if "passed" in applied else "—",
            )
        )
    for key in sorted(set(gates) - set(measured)):
        applied = gates[key]
        # The stored object carries the run's own diagnostics beside its gates — the thresholds it
        # used, the path and month counts, the start-date sweep — and those are not gates and get
        # no row. A gate is an object that says whether it passed.
        if not isinstance(applied, dict) or "passed" not in applied:
            continue
        gate_rows.append(
            (
                LATER_GATES.get(key, key),
                _number(_as_float(applied.get("value"))),
                f"{applied.get('rule', '')} {applied.get('threshold', '')}".strip(),
                "pass" if applied.get("passed") else "FAIL",
            )
        )
    if promoted is None:
        standing = (
            f"Nothing is promoted. This is certification {certification_id}, the newest imported;"
            " the pipeline refuses to rank until one is promoted."
        )
    elif promoted[8]:
        standing = (
            f"Promoted {et(promoted[7])} by {promoted[6]}, and voided {et(promoted[9])}: "
            f"{promoted[10]}. The gate refuses a voided model rather than fall back to another."
        )
    else:
        standing = f"Promoted {et(promoted[7])} by {promoted[6]}, and not voided."
    paths = quantiles.get("paths")
    counted = (
        f"{paths:,} combinatorially purged cross-validation paths."
        if isinstance(paths, int)
        else "The certification recorded no path count."
    )
    limits = health.thresholds(conn)
    blocks: list[object] = [
        note(
            f"{model_id} {'passed' if passed else 'FAILED'} its gates, certified "
            f"{et(certified_at)} by {actor} on {train_start}–{train_end} at "
            f"{readout.dollars(capital)} under cost model {cost_model}."
        ),
        note(standing),
        tag("h3", "Gates"),
        table(["Gate", "Measured", "Needs", "Result"], gate_rows, numeric=(1, 2)),
        note(
            f"Declared trials {trials:,}, which is what the deflated Sharpe is deflated by: the"
            " figure answers how much of the Sharpe survives having tried that many times."
            f" Break-even cost multiple {_number(_as_float(break_even), 2)}, the multiple of the"
            " modelled cost that would erase the edge. Gross Sharpe "
            f"{_number(_as_float(sharpe_gross), 2)}"
            + (
                ", flagged as implausibly high for a strategy of this kind."
                if gross_flag
                else ", unflagged."
            )
        ),
        tag("h3", "Path distribution"),
        note(
            f"{counted} Each measure below is the distribution across those paths, with what the"
            " live account has measured so far beside it."
        ),
        _path_table(
            quantiles,
            _live_paths(health.equity_months(conn), limits.monthly_return if limits else None),
        ),
        note(
            f"These gates certify {certifies}. The LLM safety panel, and the blend that puts its"
            " score beside the ranker's, are certified by nothing on this page: no gate in the"
            " table above measures them, and no path in the distribution was run with them in"
            " it."
        ),
    ]
    return page("Validation", "/validation", [banner(conn, now), section("Validation", blocks)])


# The Promotion view.

# The most history rows the Promotion view shows.
LADDER_MAX = 100
# Every transition the ladder has made, in one list: the certifications imported, the promotions
# and voidings of them, the mode changes and the level changes. Each row carries who did it and
# the evidence they did it on, because a transition without either is not auditable. The newest
# rows are taken and then read oldest-first, as the log's cycles are.
_PROMOTION_HISTORY = """
    SELECT at, what, detail, actor, evidence FROM (
        SELECT * FROM (
            SELECT c.certified_at AS at, c.id AS seq, 'certified' AS what,
                   c.model_id || ' ' || CASE WHEN c.passed THEN 'passed' ELSE 'FAILED' END
                       || ' its gates' AS detail,
                   c.actor::text AS actor,
                   'artifact ' || left(c.artifact_sha256, 12) AS evidence
            FROM agb.model_certification c
            UNION ALL
            SELECT p.at, p.id, 'promoted', c.model_id, p.actor::text, 'certification ' || c.id
            FROM agb.promotion_event p
            JOIN agb.model_certification c ON c.id = p.certification_id
            UNION ALL
            SELECT e.at, e.id, 'voided', c.model_id || ': ' || e.reason, e.actor::text,
                   'cost model ' || e.cost_model_version
            FROM agb.certification_event e
            JOIN agb.model_certification c ON c.id = e.certification_id
            UNION ALL
            SELECT m.at, m.id, 'mode ' || m.mode, coalesce(m.reason, ''), m.actor::text,
                   coalesce(m.attestation, '')
            FROM agb.mode_event m
            UNION ALL
            SELECT l.at, l.id, 'level ' || l.level, l.reason, l.actor::text,
                   coalesce(l.attestation, 'rebalance ' || b.reference, '')
            FROM agb.ladder_event l
            LEFT JOIN agb.basket b ON b.id = l.basket_id
        ) events
        ORDER BY at DESC, seq DESC
        LIMIT %s
    ) latest
    ORDER BY at, seq
"""


def _criteria_block(heading: str, criteria: Sequence[ladder.Criterion], standing: str) -> Html:
    """One ladder's criteria as its own view measures them, with where it stands above the table.
    A criterion nothing has measured reads as unmet, which is the honest answer."""
    return join(
        [
            tag("h3", heading),
            note(standing),
            table(
                ["Criterion", "Measured", "Needs", "Result"],
                [(c.name, c.measured, c.threshold, "met" if c.met else "unmet") for c in criteria],
                numeric=(1, 2),
            ),
        ]
    )


def promotion(conn: psycopg.Connection, now: datetime) -> str:
    """The ladder from backtest to live: which certification is live and who promoted it on what
    evidence, which mode and which level the system is at, what the level above still wants, and
    every transition it has made with the date, the actor and the evidence behind it."""
    level = ladder.current(conn)
    mode_row = conn.execute(ladder._MODE).fetchone()
    assert mode_row is not None, "agb.mode_event starts with a paper event"
    mode, mode_since, mode_actor = mode_row
    promoted = conn.execute(_PROMOTED).fetchone()
    blocks: list[object] = [
        note(f"Trading {mode} since {et(mode_since)}, set by {mode_actor}."),
        note(f"Level {level.level} since {et(level.since)}, set by {level.actor}: {level.reason}"),
        note(f"At {level.level}, {ladder.MEANING[level.level]}."),
    ]
    if level.attestation:
        blocks.append(note(f"Attested: {level.attestation}"))
    blocks.append(tag("h3", "The live model"))
    if promoted is None:
        blocks.append(
            note(
                "No certification has been promoted. The pipeline refuses to rank without one, so"
                " nothing on the backtest side of the ladder has crossed it yet."
            )
        )
    else:
        (
            certification_id,
            model_id,
            model_passed,
            trials,
            cost_model,
            artifact,
            promoted_by,
            promoted_at,
            voided,
            voided_at,
            voided_reason,
        ) = promoted
        blocks.append(
            note(
                f"{model_id} is live: certification {certification_id}, promoted "
                f"{et(promoted_at)} by {promoted_by}, "
                f"{'passed' if model_passed else 'FAILED'} its gates on {trials:,} declared trials"
                f" under cost model {cost_model}, artifact {artifact[:12]}."
            )
        )
        if voided:
            blocks.append(
                note(f"It was voided {et(voided_at)}: {voided_reason}. The gate refuses it.")
            )
        blocks.append(
            tag(
                "p",
                join(
                    [
                        "Its gates and its path distribution are on the ",
                        tag("a", "Validation", href="/validation"),
                        " page.",
                    ]
                ),
                class_="note",
            )
        )
    criteria = ladder.status(conn)
    if not criteria:
        blocks.append(join([tag("h3", "The level above"), note("L2 is the top of the ladder.")]))
    else:
        eligible = ladder.ready(conn)
        target = (
            eligible.target
            if eligible is not None
            else ladder.LEVELS[ladder.LEVELS.index(level.level) + 1]
        )
        if eligible is not None and eligible.ready:
            standing = (
                f"{target} is earned. It is raised at the terminal, with the attestation a human"
                " types, and never from this page: the ladder loosens only where someone says so"
                " in their own words."
            )
        elif eligible is not None and eligible.outstanding:
            standing = f"{target} is not earned: {', '.join(eligible.outstanding)}."
        else:
            standing = f"{target} is not earned yet."
        blocks.append(_criteria_block(f"Towards {target}", criteria, standing))
    live = ladder.live_ready(conn)
    if mode == "live":
        standing = "Trading live already. The criteria below are measured as they stand now."
    elif live.ready:
        standing = "Paper has earned live. The switch is thrown at the terminal, not here."
    else:
        standing = (
            f"Paper, with {len(live.outstanding)} of these criteria outstanding: "
            f"{', '.join(live.outstanding)}."
        )
    blocks.append(_criteria_block("Towards live", ladder.live_status(conn), standing))
    history = conn.execute(_PROMOTION_HISTORY, (LADDER_MAX,)).fetchall()
    blocks.append(tag("h3", "Every transition"))
    if not history:
        blocks.append(note("Nothing has been certified, promoted or moved yet."))
    else:
        blocks.append(
            table(
                ["When", "Event", "What", "Actor", "Evidence"],
                [
                    (et(at), what, detail, who, evidence)
                    for at, what, detail, who, evidence in history
                ],
                wrap=(2,),
            )
        )
    return page("Promotion", "/promotion", [banner(conn, now), section("Promotion", blocks)])


# The Performance view.

# The promoted model's path distribution, which is what a live figure is read against.
_PROMOTED_PATHS = """
    SELECT c.model_id, c.path_quantiles
    FROM agb.promoted_model p
    JOIN agb.model_certification c ON c.id = p.certification_id
"""


def performance(conn: psycopg.Connection, now: datetime) -> str:
    """What the account did, never on its own: every live figure sits beside the certified
    distribution it is read against, and the page says how many months it is reading. A handful of
    months cannot tell a model that works from one that has been lucky, and the page says so."""
    months = health.equity_months(conn)
    promoted = conn.execute(_PROMOTED_PATHS).fetchone()
    limits = health.thresholds(conn)
    blocks: list[object] = []
    quantiles: Mapping[str, object] = {}
    if promoted is None:
        blocks.append(
            note(
                "No model is promoted, so there is no certified distribution to read the account"
                " against. Every figure below would stand alone, which is the one way this page is"
                " not meant to be read."
            )
        )
    else:
        model_id, quantiles = promoted
        paths = quantiles.get("paths")
        blocks.append(
            note(
                f"Read against {model_id}'s "
                + (f"{paths:,} certified paths." if isinstance(paths, int) else "certified paths.")
            )
        )
    if not months:
        blocks.append(note("No month has a return yet, so there is nothing to compare."))
        return page(
            "Performance", "/performance", [banner(conn, now), section("Performance", blocks)]
        )
    expected = limits.monthly_return if limits else None
    blocks.append(
        table(["What", "Facts"], [(item.what, item.facts) for item in readout.comparison(conn)])
    )
    blocks.append(tag("h3", "Beside the backtest"))
    blocks.append(_path_table(quantiles, _live_paths(months, expected)))
    worst = min(months, key=lambda month: month.total_return)
    average = quantiles.get("monthly_return") or {}
    assert isinstance(average, Mapping)
    blocks.append(
        note(
            f"Worst month {worst.period} at {_fraction(worst.total_return)}, against a certified"
            f" median path averaging {_measure(_as_float(average.get('q50')), True)} a month and a"
            f" fifth-percentile path averaging {_measure(_as_float(average.get('q05')), True)}."
            " Those certified figures are each path's average month and not its worst one, so a"
            " single month is a rough reference against them and nothing more."
        )
    )
    blocks.append(tag("h3", "Every month"))
    blocks.append(
        table(
            ["Period", "Mode", "Return", "Dividends added back", "vs certified average month"],
            [
                (
                    month.period,
                    month.mode or "—",
                    _fraction(month.total_return),
                    readout.dollars(month.dividends) if month.dividends else "—",
                    "—"
                    if expected is None
                    else f"{(month.total_return - expected) * 100:+.2f} points",
                )
                for month in months
            ],
            numeric=(2, 3, 4),
        )
    )
    paper = [month for month in months if month.mode == "paper"]
    if paper:
        blocks.append(
            note(
                f"{len(paper)} of these {len(months)} months were paper months, restated with the"
                " dividends the operator recorded added back: the paper broker credits none, and"
                " the backtest's expectation assumes them. Live equity already holds them."
            )
        )
    blocks.append(
        note(
            f"{len(months)} month{'' if len(months) == 1 else 's'} measured. That is far too few"
            " to tell a model that works from one that has been lucky: the certified distribution"
            " above is what says which readings are ordinary, and a reading inside it is evidence"
            " of neither."
        )
    )
    return page("Performance", "/performance", [banner(conn, now), section("Performance", blocks)])


# The Panel view.

# The most names the Panel view renders in full.
PANEL_MAX = 60
# The shortlist of the latest live basket the panel scored, with both terms of each name's blended
# score. A name the panel could not score is `neutral` and keeps the middle of the range.
_PANEL_BLEND = """
    SELECT symbol, stage1_rank, llm_total, llm_score, neutral
    FROM agb.basket_blend
    WHERE basket_id = %s
    ORDER BY stage1_rank
    LIMIT %s
"""
_PANEL_SHORTLIST = "SELECT count(*) FROM agb.basket_blend WHERE basket_id = %s"
# What the panel did with each name in that run: what EDGAR returned, the flags its filings set,
# and the debate's own outcome.
_PANEL_RETRIEVALS = """
    SELECT r.symbol, r.id, r.outcome, r.detail, r.delinquent, r.ipo, s.id, s.outcome, s.detail
    FROM agb.panel_retrieval r
    LEFT JOIN agb.panel_score s ON s.retrieval_id = r.id
    WHERE r.run_id = %s
"""
# Every dimension of every score in the run, with the Judge's own reason for it.
_PANEL_DIMENSIONS = """
    SELECT d.score_id, d.dimension, d.score, d.reason, d.source, d.judge_score
    FROM agb.panel_dimension d
    JOIN agb.panel_score s ON s.id = d.score_id
    WHERE s.run_id = %s
    ORDER BY d.score_id, d.dimension
"""
# What the Analyst read, as it was cited: one row per section of one filing.
_PANEL_FILINGS = """
    SELECT f.retrieval_id, f.document, f.form, f.filed, f.section, f.sent_chars, f.chars
    FROM agb.panel_filing f
    JOIN agb.panel_retrieval r ON r.id = f.retrieval_id
    WHERE r.run_id = %s
    ORDER BY f.retrieval_id, f.filed DESC, f.document
"""


def names_of(dimensions: Iterable[int]) -> str:
    """Some rubric dimensions, by number and by the rubric's own name for them. The rules that
    cap a dimension name it by number alone, and a reader should not have to count rows to find
    out which one a rule touched. The number leads, because that is the column the table below
    carries, and two of the six names have an "and" of their own."""
    named = [f"dimension {number} ({analyst.RUBRIC[number][0].lower()})" for number in dimensions]
    if len(named) < 2:
        return "".join(named)
    return f"{', '.join(named[:-1])} and {named[-1]}"


def _panel_name(
    symbol: str,
    rank: int,
    total: int | None,
    score: float | None,
    neutral: bool,
    place: int,
    scored: int,
    retrieval: Sequence[object] | None,
    dimensions: Sequence[Sequence[object]],
    filings: Sequence[Sequence[object]],
) -> Html:
    """One name's whole panel record: its two terms, its six rubric dimensions with the Judge's
    reason on each, the flags its filings set, and the filings themselves."""
    if neutral or total is None or score is None:
        safety = "safety not scored, held at the neutral middle"
    else:
        safety = (
            f"safety {total}/{debate.MAX_TOTAL} ({score:.2f}), "
            f"{readout._ordinal(place)} safest of the {scored} scored"
        )
    summary = tag("summary", f"{symbol} — stage-1 rank {rank}; {safety}")
    body: list[object] = []
    if retrieval is None:
        body.append(note("The panel has no record of this name in that run."))
        return Html(
            f'<details class="cycle">{summary}<div class="detail">{join(body)}</div></details>'
        )
    _, outcome, detail, delinquent, ipo, _, score_outcome, score_detail = retrieval
    if outcome != "retrieved":
        body.append(note(f"EDGAR returned {outcome}: {detail}"))
    elif score_outcome is None:
        body.append(note("Its filings were read, but the debate left no score."))
    elif score_outcome != "scored":
        body.append(note(f"The debate failed ({score_outcome}): {score_detail}"))
    if delinquent:
        capped = names_of(debate.DELINQUENT_DIMENSIONS)
        body.append(
            note(
                f"Flagged {delinquent} from its filings: a late or missing annual report caps"
                f" {capped} at {debate.DELINQUENT_CAP} by rule, whatever the Judge scored them."
            )
        )
    if ipo:
        body.append(
            note(
                f"Flagged as a recent listing: {names_of([debate.IPO_DIMENSION])} is held at a"
                f" neutral {debate.IPO_SCORE} by rule rather than scored, because a first-time"
                " filer has no prior annual report to be compared against."
            )
        )
    if dimensions:
        body.append(
            table(
                ["#", "Dimension", "Score", "The Judge's reason"],
                [
                    (
                        number,
                        analyst.RUBRIC[number][0],
                        f"{dimension_score}/{debate.DIMENSION_MAX}",
                        reason
                        if source != "rule"
                        else join(
                            [
                                esc(reason),
                                tag(
                                    "span",
                                    f" Set by a filing rule; the Judge scored {judge_score}.",
                                    class_="dim",
                                ),
                            ]
                        ),
                    )
                    for number, dimension_score, reason, source, judge_score in dimensions
                ],
                numeric=(0, 2),
                wrap=(3,),
            )
        )
    if filings:
        body.append(
            table(
                ["Document", "Form", "Filed", "Section", "Read"],
                [
                    (
                        document,
                        form,
                        filed,
                        part,
                        "in full"
                        if sent is None or chars is None or sent >= chars
                        else f"first {sent:,} of {chars:,} characters",
                    )
                    for document, form, filed, part, sent, chars in filings
                ],
            )
        )
    elif outcome == "retrieved":
        body.append(note("No filing section was recorded for this name."))
    return Html(f'<details class="cycle">{summary}<div class="detail">{join(body)}</div></details>')


def panel(conn: psycopg.Connection, now: datetime) -> str:
    """The safety panel's own evidence for the latest live basket it scored: every shortlisted name
    with its six-dimension rubric, the score and the Judge's reason on each dimension, the filings
    the Analyst read and how much of each it sent, and the fact that the panel was never told which
    company it was reading about."""
    latest = conn.execute(readout._WHY_BASKET).fetchone()
    if latest is None:
        empty = [note("No panel has scored a live basket yet.")]
        return page("Panel", "/panel", [banner(conn, now), section("Panel", empty)])
    basket_id, reference, universe, run_id, w = latest
    shortlisted = conn.execute(_PANEL_SHORTLIST, (basket_id,)).fetchone()
    blend_rows = conn.execute(_PANEL_BLEND, (basket_id, PANEL_MAX)).fetchall()
    retrievals = {row[0]: row[1:] for row in conn.execute(_PANEL_RETRIEVALS, (run_id,)).fetchall()}
    dimensions: dict[int, list[Sequence[object]]] = {}
    for row in conn.execute(_PANEL_DIMENSIONS, (run_id,)).fetchall():
        dimensions.setdefault(row[0], []).append(row[1:])
    filings: dict[int, list[Sequence[object]]] = {}
    for row in conn.execute(_PANEL_FILINGS, (run_id,)).fetchall():
        filings.setdefault(row[0], []).append(row[1:])
    ranked = sorted(
        (row[3] for row in blend_rows if not row[4] and row[3] is not None), reverse=True
    )
    count = shortlisted[0] if shortlisted else len(blend_rows)
    blocks: list[object] = [
        note(
            f"Rebalance {reference}: {count} names shortlisted out of a universe of "
            f"{universe:,}, each blended at w = {w} between the ranker's place and the panel's"
            f" safety score. Panel run {run_id}."
        ),
        note(readout.SCRUBBED),
    ]
    if len(blend_rows) < count:
        blocks.append(note(f"The {len(blend_rows)} best-ranked of them are below."))
    for symbol, rank, total, score, neutral in blend_rows:
        retrieval = retrievals.get(symbol)
        score_id = retrieval[5] if retrieval else None
        place = 1 + sum(1 for other in ranked if score is not None and other > score)
        blocks.append(
            _panel_name(
                symbol,
                rank,
                total,
                score,
                neutral,
                place,
                len(ranked),
                retrieval,
                dimensions.get(score_id, ()) if score_id is not None else (),
                filings.get(retrieval[0], ()) if retrieval else (),
            )
        )
    return page("Panel", "/panel", [banner(conn, now), section("Panel", blocks)])


# The Alerts view.

_ALERTS = """
    SELECT a.kind, a.subject, a.period, a.body, a.raised_at, a.raised_by,
           max(e.at) FILTER (WHERE e.event = 'delivered'),
           max(e.at) FILTER (WHERE e.event = 'acknowledged'),
           count(e.id) FILTER (WHERE e.event = 'failed')
    FROM agb.alert a
    LEFT JOIN agb.alert_event e ON e.alert_id = a.id
    GROUP BY a.id
    ORDER BY a.raised_at DESC, a.id DESC
    LIMIT %s
"""


def _delivery(delivered: datetime | None, acknowledged: datetime | None, failures: int) -> Html:
    if acknowledged:
        return tag("span", f"acknowledged {et(acknowledged)}", class_="ok")
    if delivered:
        return tag("span", f"delivered {et(delivered)}", class_="ok")
    if failures:
        return tag("span", f"not delivered, {failures} failed attempts", class_="bad")
    return tag("span", "not sent yet", class_="warn")


def alerts(conn: psycopg.Connection, now: datetime) -> str:
    """Every alert, newest first, with whether it reached the operator. An alert that never
    left the server is visible here and in the banner, and nowhere else."""
    rows = conn.execute(_ALERTS, (ALERT_MAX,)).fetchall()
    if not rows:
        blocks: list[object] = [note("Nothing has been raised.")]
        return page("Alerts", "/alerts", [banner(conn, now), section("Alerts", blocks)])
    listed = []
    for kind, subject, period, body, raised_at, by, delivered, acknowledged, failures in rows:
        listed.append(
            (
                et(raised_at),
                kind,
                f"{subject} {period}",
                by,
                _delivery(delivered, acknowledged, failures),
                Html(f"<details><summary>body</summary><pre>{esc(body)}</pre></details>"),
            )
        )
    blocks = [table(["Raised", "Kind", "Subject", "By", "Delivery", ""], listed)]
    return page("Alerts", "/alerts", [banner(conn, now), section("Alerts", blocks)])


# The kill switch.


def kill_switch(conn: psycopg.Connection, now: datetime) -> str:
    """The kill switch as the bot has it: what is latched, pause, resume, and liquidation in two
    steps. The page shows whether a token is outstanding and until when, never the token
    itself, because a page that redisplayed it would hand the second step to whoever reloaded
    it."""
    latched = killswitch.causes(conn)
    minutes = int(killswitch.TOKEN_VALID.total_seconds() // 60)
    expires = killswitch.outstanding(conn, now)
    blocks: list[object] = [
        table(
            ["Open cause", "Since", "By", "Reason"],
            [(cause, et(at), actor, reason or "") for cause, at, actor, reason in latched],
        )
        if latched
        else note("No halt cause is open."),
        tag("h3", "Pause"),
        note(
            "Opens the pause cause, so no further order goes out, and asks execution to suspend "
            "trading at the broker. Execution confirms that as an alert. The next page says "
            "what will change and writes nothing until it is confirmed."
        ),
        tag("p", tag("a", "Pause…", href=CONFIRM_PAUSE)),
        tag("h3", "Resume"),
        note(
            f"Clears the causes you tick on the next page, one clear each. Open now: "
            f"{', '.join(c for c, *_ in latched)}."
            if latched
            else "Clears the causes you tick, one clear each. Nothing is open to clear."
        ),
        tag("p", tag("a", "Resume…", href=CONFIRM_RESUME)),
        tag("h3", "Liquidate"),
        note(
            "Two steps. The first says what closing every position would mean and issues a "
            f"token good for {minutes} minutes; the second is that token typed back. A wrong or "
            "late token is refused, spends the token, and you start again. Asking for the token "
            "is a write too, so it has its own confirmation page first."
        ),
        tag("p", tag("a", "Ask to liquidate…", href=CONFIRM_LIQUIDATE)),
        note(
            f"A token is outstanding until {et(expires)}. Type it back to go ahead."
            if expires
            else "No token is outstanding."
        ),
        write_form("/liquidate", "Liquidate", [("token", "Token")]),
    ]
    return page("Kill switch", "/killswitch", [banner(conn, now), section("Kill switch", blocks)])


# Where each kill-switch write is confirmed. The kill switch page links here, and these pages
# only read: the first click on Pause, Resume or Ask to liquidate writes nothing, and the write is
# the form on the page it lands on, posted to the same route as before.
CONFIRM_PAUSE = "/confirm/pause"
CONFIRM_RESUME = "/confirm/resume"
CONFIRM_LIQUIDATE = "/confirm/liquidate"
# What a ticked cause is called in the form the resume confirmation posts: `cause.<name>`.
CAUSE_FIELD = "cause."


def confirm_form(action: str, button: str, ticks: Sequence[str] = ()) -> Html:
    """The confirmed write as a plain form posting to one of `WRITES`, with a way back. The ticks
    are unticked and carry no value: nothing here is chosen for the operator, and an untouched
    form clears nothing."""
    body: list[object] = [
        Html(
            f'<label><input type="checkbox" name="{esc(CAUSE_FIELD + cause)}"> {esc(cause)}</label>'
        )
        for cause in ticks
    ]
    body.append(tag("button", button, type="submit"))
    body.append(Html(' <a href="/killswitch">Cancel</a>'))
    return Html(f'<form class="write" method="post" action="{esc(action)}">{join(body)}</form>')


def confirm_pause(conn: psycopg.Connection, now: datetime) -> str:
    already = next(
        ((at, actor) for cause, at, actor, _ in killswitch.causes(conn) if cause == "pause"), None
    )
    blocks: list[object] = [
        note("Nothing has been written yet. Confirming does these things:"),
        Html(
            "<ul><li>opens the <b>pause</b> halt cause, so no further order goes out, whatever "
            "else is open;</li><li>asks execution to set the broker's suspend_trade, and to "
            "cancel the sent orders of a basket still being submitted, which halts that "
            "basket;</li><li>records the command against this surface.</li></ul>"
        ),
        note(
            f"The pause cause is open already, since {et(already[0])} by {already[1]}: "
            "confirming records the command and changes nothing else."
            if already
            else "The pause cause is not open."
        ),
        confirm_form("/pause", "Confirm pause"),
    ]
    return page("Confirm pause", "/killswitch", [banner(conn, now), section("Pause?", blocks)])


def confirm_resume(conn: psycopg.Connection, now: datetime) -> str:
    latched = killswitch.causes(conn)
    if latched:
        blocks: list[object] = [
            note(
                "Nothing has been written yet. Tick each cause to clear; only those are "
                "cleared, one clear each, and an unticked cause stays open and keeps trading "
                "halted. Nothing is ticked for you, and ticking none clears nothing."
            ),
            table(
                ["Open cause", "Since", "By", "Reason"],
                [(cause, et(at), actor, reason or "") for cause, at, actor, reason in latched],
            ),
            note(
                "The broker's suspend_trade is asked off once the pause cause is cleared, and "
                "stays on while it is open. A halted rebalance is not resubmitted."
            ),
            confirm_form("/resume", "Confirm resume", [cause for cause, *_ in latched]),
        ]
    else:
        blocks = [
            note("No halt cause is open, so there is nothing to clear."),
            tag("p", tag("a", "Back", href="/killswitch")),
        ]
    return page("Confirm resume", "/killswitch", [banner(conn, now), section("Resume?", blocks)])


def confirm_liquidate(conn: psycopg.Connection, now: datetime) -> str:
    minutes = int(killswitch.TOKEN_VALID.total_seconds() // 60)
    expires = killswitch.outstanding(conn, now)
    blocks: list[object] = [
        note("Nothing has been written yet. Confirming is step one of two, and does only this:"),
        Html(
            f"<ul><li>issues a token good for {minutes} minutes, and records the request;</li>"
            "<li>answers with what closing every position would mean: the positions held and "
            "their value, and the account's equity.</li></ul>"
        ),
        note(
            "It closes nothing and opens no halt cause. Step two is that token typed back on "
            "the kill switch page, which opens the liquidation cause and has execution cancel "
            "open orders and close every position at market."
        ),
        note(
            f"A token is outstanding until {et(expires)}; a new one supersedes it."
            if expires
            else "No token is outstanding."
        ),
        confirm_form("/liquidate", "Confirm: issue a token"),
    ]
    return page(
        "Confirm liquidate",
        "/killswitch",
        [banner(conn, now), section("Ask to liquidate?", blocks)],
    )


# The write paths. Each takes the fields the form posted and returns the `Outcome`: the reply
# the shared module produced, which is the same text the bot sends to Telegram. The server does
# not render it in answer to the POST: it holds it and redirects to `OUTCOME`, a GET, so a
# reload re-reads the outcome and never posts the write again.

# Where the server redirects a write to, with the held outcome's token in the query.
OUTCOME = "/outcome"


@dataclass(frozen=True)
class Outcome:
    """What a write did: the page it belongs to, the heading and the shared module's reply."""

    path: str
    heading: str
    reply: str


def _source(text: str) -> approval.Source:
    """Who the write is attributed to and what they typed, as the shared modules take it. A row
    written from here differs from the bot's only in the actor and the surface."""
    return approval.Source(ACTOR, SURFACE, text)


def calendar(now: datetime) -> NyseCalendar:
    """The sessions the approval loop needs to find a rebalance's cutoff. A month either side
    of today covers every reference that can still be answered."""
    today = trading_date(now)
    return NyseCalendar(today - timedelta(days=7), today + timedelta(days=30))


def _answer(
    conn: psycopg.Connection, verb: str, fields: Mapping[str, str], now: datetime
) -> Outcome:
    """Approve or reject the rebalance whose reference was typed. `agb.approval` checks the
    reference, refuses a wrong, missing or late one and records that refusal, and settles a race
    with the bot on the basket's own sequence, so the loser is told who answered and when.

    The reference is read from its own field and from nowhere else. Whatever the reason box
    holds is only a reason, so a reference typed there leaves the reference empty and is
    refused as missing. A reference box that holds more than one word is not a reference either:
    it is looked up whole, found missing, refused and recorded."""
    reference = fields.get("reference", "").strip()
    words = [reference, *fields.get("reason", "").split()] if reference else []
    source = _source(" ".join([f"/{verb}", *words]))
    reply = approval.decide(conn, verb, words, source, now, calendar(now))
    return Outcome("/rebalance", "Your answer", reply)


def approve(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    return _answer(conn, "approve", fields, now)


def reject(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    return _answer(conn, "reject", fields, now)


def pause(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    return Outcome("/killswitch", "Kill switch", killswitch.pause(conn, _source("/pause")))


def resume(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    """Clear the causes the confirmation page ticked, and no others. A form that ticks none is
    refused and recorded, and a cause that opened after the page was drawn is not on it, so it
    stays open."""
    ticked = sorted(name[len(CAUSE_FIELD) :] for name in fields if name.startswith(CAUSE_FIELD))
    source = _source(" ".join(["/resume", *ticked]))
    return Outcome("/killswitch", "Kill switch", killswitch.resume(conn, source, ticked))


def liquidate(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> Outcome:
    words = fields.get("token", "").split()
    source = _source(" ".join(["/liquidate", *words]))
    reply = killswitch.liquidate(conn, source, words[0] if words else None, now)
    return Outcome("/killswitch", "Kill switch", reply)


def outcome(conn: psycopg.Connection, now: datetime, held: Outcome | None) -> str:
    """The page a write redirects to: the reply it produced, above the banner as it reads now.
    An outcome the server no longer holds, because it expired or the server restarted, says so
    and points at the log, where every write the operator made is a row."""
    if held is None:
        return page(
            "No outcome",
            "/",
            [
                banner(conn, now),
                section(
                    "No outcome",
                    [
                        note(
                            "That outcome is no longer held here: it is kept for a few minutes "
                            "after a write. What the write did is a row in the log."
                        ),
                        tag("p", tag("a", "Log", href="/")),
                    ],
                ),
            ],
        )
    blocks = [
        Html(f'<pre class="reply">{esc(held.reply)}</pre>'),
        tag("p", tag("a", "Back", href=held.path)),
    ]
    return page(held.heading, held.path, [banner(conn, now), section(held.heading, blocks)])


def failed(path: str, cause: str, wrote: bool) -> str:
    """The page for a request that raised something that was not the database's: the route and
    the cause by name, and no traceback, which goes to the log. A write may have been recorded
    before it raised, so its page says to look in the log and not to retry."""
    heading = "The write failed" if wrote else "The page failed"
    blocks = [
        note(f"{path} raised an error."),
        Html(f'<pre class="reply">{esc(cause)}</pre>'),
    ]
    if wrote:
        blocks.append(note("It may have been recorded before it failed: check the log first."))
    blocks.append(tag("p", tag("a", "Log", href="/")))
    return page(heading, path, [section(heading, blocks)])


# What the server routes: the pages, which only read, and the writes, which are these five and
# no others. A page's form posts to one of these and nowhere else, and the server answers a
# POST to a page with 405.
ROUTES = {
    "/": log,
    "/rebalance": rebalance,
    "/positions": positions,
    "/gate": gate,
    "/validation": validation,
    "/promotion": promotion,
    "/performance": performance,
    "/panel": panel,
    "/alerts": alerts,
    "/killswitch": kill_switch,
    CONFIRM_PAUSE: confirm_pause,
    CONFIRM_RESUME: confirm_resume,
    CONFIRM_LIQUIDATE: confirm_liquidate,
}
WRITES = {
    "/approve": approve,
    "/reject": reject,
    "/pause": pause,
    "/resume": resume,
    "/liquidate": liquidate,
}


def not_found(path: str) -> str:
    return page(
        "Not found", path, [note(f"There is no {path} here."), tag("p", tag("a", "Log", href="/"))]
    )


def not_allowed(path: str) -> str:
    """A POST to a page. The pages read; the writes are the routes in `WRITES`."""
    return page(
        "Not allowed",
        path,
        [
            note(f"{path} is a page, and pages are read with GET."),
            tag("p", tag("a", "Log", href="/")),
        ],
    )
