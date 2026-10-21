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

The views beside the log — Rebalance, Positions, Gate, Alerts and the kill switch — are the
same state read five other ways. Each speaks the Telegram bot's vocabulary, from agb.readout,
so the two surfaces cannot describe one basket in two dialects.

The writes are exactly Telegram's two: answering the rebalance, and the kill switch. Neither
is written here. Answering goes through `agb.approval` and the switch through
`agb.killswitch`, the same functions the bot calls, with a source naming this surface, so the
semantics cannot drift between the two surfaces and the race between them is settled in one
place. Approving is never one click: the operator types the rebalance's reference, which is
the friction Telegram's typed command has and the whole reason the landing page is the log
rather than a basket with a green button. Every write is a `command` row with the actor and
the surface on it, refused ones included.
"""

import html
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal, DivisionByZero, InvalidOperation

import psycopg

from agb import approval, health, killswitch, readout
from agb.health import et
from agb.nyse import NyseCalendar, trading_date

# The log and the views, in the order the header shows them.
VIEWS = (
    ("/", "Log"),
    ("/rebalance", "Rebalance"),
    ("/positions", "Positions"),
    ("/gate", "Gate"),
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
    headers: Sequence[str], rows: Iterable[Sequence[object]], numeric: Sequence[int] = ()
) -> Html:
    def cell(kind: str, index: int, value: object) -> str:
        return f'<{kind} class="num">{esc(value)}</{kind}>' if index in numeric else (
            f"<{kind}>{esc(value)}</{kind}>"
        )  # fmt: skip

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
    SELECT at, actor, event, detail FROM agb.audit_log
    WHERE subject = %s
    ORDER BY at, event
"""
# A cycle's flags: the anomalies the system raised for the month it trades. The panel's
# name-level flags join here once the panel runs on the live path; a quiet month stays quiet.
_PERIOD_ALERTS = """
    SELECT kind, subject, raised_at, raised_by FROM agb.alert
    WHERE period = %s
    ORDER BY raised_at, id
"""


def period_return(conn: psycopg.Connection, period: str) -> tuple[str, str]:
    """A month's realised return and the window it was measured over, from the same two
    equity readings `/pnl` uses."""
    window = readout.equity_window(conn, period)
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


def _gate_block(conn: psycopg.Connection, basket_id: int) -> Html:
    rows = readout.checks(conn, basket_id)
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


def _flags_block(conn: psycopg.Connection, period: str) -> Html:
    rows = conn.execute(_PERIOD_ALERTS, (period,)).fetchall()
    if not rows:
        return note("none")
    return table(
        ["Flag", "Subject", "Raised", "By"],
        [(kind, subject, et(at), by) for kind, subject, at, by in rows],
    )


def _cycle(conn: psycopg.Connection, row: Sequence) -> Html:
    basket_id, reference, period, state, since, actor, surface, capital, ranker, names = row
    diff = readout.diff(conn, basket_id)
    returned, window = period_return(conn, period)
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
    events = conn.execute(_CYCLE_EVENTS, (f"live basket {reference}",)).fetchall()
    detail = join(
        [
            tag("h3", "What it changed"),
            _diff_blocks(diff, capital),
            tag("h3", "Return"),
            note(f"{returned} over {period}: {window}"),
            tag("h3", "Gate"),
            _gate_block(conn, basket_id),
            tag("h3", "Flags"),
            _flags_block(conn, period),
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
        body = [_LOG_HEAD, *(_cycle(conn, row) for row in cycles)]
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
            "trading at the broker. Execution confirms that as an alert."
        ),
        write_form("/pause", "Pause"),
        tag("h3", "Resume"),
        note(
            f"Clears every open cause, one clear each: {', '.join(c for c, *_ in latched)}."
            if latched
            else "Clears every open cause, one clear each. Nothing is open to clear."
        ),
        write_form("/resume", "Resume"),
        tag("h3", "Liquidate"),
        note(
            "Two steps. The first says what closing every position would mean and issues a "
            f"token good for {minutes} minutes; the second is that token typed back. A wrong or "
            "late token is refused, spends the token, and you start again."
        ),
        write_form("/liquidate", "Ask to liquidate"),
        note(
            f"A token is outstanding until {et(expires)}. Type it back to go ahead."
            if expires
            else "No token is outstanding."
        ),
        write_form("/liquidate", "Liquidate", [("token", "Token")]),
    ]
    return page("Kill switch", "/killswitch", [banner(conn, now), section("Kill switch", blocks)])


# The write paths. Each takes the fields the form posted and answers with a page carrying the
# reply the shared module produced, which is the same text the bot sends to Telegram.


def _source(text: str) -> approval.Source:
    """Who the write is attributed to and what they typed, as the shared modules take it. A row
    written from here differs from the bot's only in the actor and the surface."""
    return approval.Source(ACTOR, SURFACE, text)


def calendar(now: datetime) -> NyseCalendar:
    """The sessions the approval loop needs to find a rebalance's cutoff. A month either side
    of today covers every reference that can still be answered."""
    today = trading_date(now)
    return NyseCalendar(today - timedelta(days=7), today + timedelta(days=30))


def _reply(conn: psycopg.Connection, now: datetime, path: str, heading: str, reply: str) -> str:
    blocks = [
        Html(f'<pre class="reply">{esc(reply)}</pre>'),
        tag("p", tag("a", "Back", href=path)),
    ]
    return page(heading, path, [banner(conn, now), section(heading, blocks)])


def _answer(conn: psycopg.Connection, verb: str, fields: Mapping[str, str], now: datetime) -> str:
    """Approve or reject the rebalance whose reference was typed. `agb.approval` checks the
    reference, refuses a wrong, missing or late one and records that refusal, and settles a race
    with the bot on the basket's own sequence, so the loser is told who answered and when."""
    words = [*fields.get("reference", "").split(), *fields.get("reason", "").split()]
    source = _source(" ".join([f"/{verb}", *words]))
    reply = approval.decide(conn, verb, words, source, now, calendar(now))
    return _reply(conn, now, "/rebalance", "Your answer", reply)


def approve(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> str:
    return _answer(conn, "approve", fields, now)


def reject(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> str:
    return _answer(conn, "reject", fields, now)


def pause(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> str:
    return _reply(
        conn, now, "/killswitch", "Kill switch", killswitch.pause(conn, _source("/pause"))
    )


def resume(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> str:
    return _reply(
        conn, now, "/killswitch", "Kill switch", killswitch.resume(conn, _source("/resume"))
    )


def liquidate(conn: psycopg.Connection, fields: Mapping[str, str], now: datetime) -> str:
    words = fields.get("token", "").split()
    source = _source(" ".join(["/liquidate", *words]))
    reply = killswitch.liquidate(conn, source, words[0] if words else None, now)
    return _reply(conn, now, "/killswitch", "Kill switch", reply)


# What the server routes: the pages, which only read, and the writes, which are these five and
# no others. A page's form posts to one of these and nowhere else, and the server answers a
# POST to a page with 405.
ROUTES = {
    "/": log,
    "/rebalance": rebalance,
    "/positions": positions,
    "/gate": gate,
    "/alerts": alerts,
    "/killswitch": kill_switch,
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
