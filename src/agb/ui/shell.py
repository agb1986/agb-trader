"""The page shell: what every view and every form is built from. The routes and styles, the one
connection a request gets, the escaped-markup helpers, the health banner, and the NYSE sessions a
cutoff is found in."""

import html
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta

import psycopg

from agb import health, readout
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
:root { color-scheme: light dark; --bg: #ffffff; --fg: #1a1a1a; --dim: #595959; --line: #d0d0d0;
        --link: #0b57d0; --bad: #a40000; --warn: #8a5300; --ok: #14612c; }
@media (prefers-color-scheme: dark) {
  :root { --bg: #121212; --fg: #ececec; --dim: #ababab; --line: #3d3d3d; --link: #8ab4f8;
          --bad: #ff8f8f; --warn: #f2b25c; --ok: #6fcf8a; }
}
* { box-sizing: border-box; }
html { background: var(--bg); }
body { margin: 0; font: 14px/1.5 system-ui, sans-serif; background: var(--bg); color: var(--fg);
       overflow-wrap: anywhere; }
a { color: var(--link); }
.skip { position: absolute; left: -999px; top: 0; background: var(--bg); padding: 6px 10px; }
.skip:focus { left: 0; z-index: 1; }
.sr { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%);
      white-space: nowrap; }
header { border-bottom: 1px solid var(--line); padding: 10px 20px; display: flex;
         gap: 6px 18px; align-items: baseline; flex-wrap: wrap; }
header .brand { font-size: 15px; font-weight: 700; margin: 0; }
header nav { display: flex; gap: 4px 18px; flex-wrap: wrap; }
header nav a { color: inherit; text-decoration: none; padding: 2px 0; }
header nav a[aria-current] { border-bottom: 2px solid currentColor; font-weight: 600; }
main { padding: 20px; max-width: 1100px; }
h1 { font-size: 18px; margin: 24px 0 8px; }
.banner + h1 { margin-top: 8px; }
h2 { font-size: 13px; margin: 14px 0 4px; color: var(--dim); text-transform: uppercase;
     letter-spacing: .04em; }
p.note { color: var(--dim); margin: 6px 0; }
.scroll { position: relative; overflow-x: auto; max-width: 100%; margin: 4px 0 10px; }
.scroll:focus-visible { outline: 2px solid var(--link); outline-offset: 2px; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 4px 10px 4px 0; border-bottom: 1px solid var(--line);
         white-space: nowrap; }
th { color: var(--dim); font-weight: 600; }
tbody th { color: var(--fg); font-weight: 400; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
td.text, th.text { white-space: normal; min-width: 22em; }
td.empty { color: var(--dim); white-space: normal; }
.pair { display: flex; gap: 32px; flex-wrap: wrap; }
.pair > div { flex: 1 1 260px; min-width: 0; }
.banner { border: 1px solid var(--line); border-left-width: 4px; padding: 8px 12px;
          margin-bottom: 16px; }
.banner.clear { border-left-color: var(--ok); }
.banner.unwell { border-left-color: var(--bad); }
.banner div { padding: 2px 0; }
.banner span { color: var(--dim); }
table.log td.expand { padding: 0; max-width: 0; white-space: normal; }
table.log tbody.cycle th, table.log tbody.cycle td { border-bottom: 0; }
table.log tbody.cycle { border-bottom: 1px solid var(--line); }
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
form.write input[type=text] { font: inherit; padding: 4px 6px; width: 14em; max-width: 100%;
                              margin-top: 4px; }
form.write button { font: inherit; padding: 4px 14px; }
pre { white-space: pre-wrap; max-width: 100%; overflow-x: auto; }
pre.reply { border: 1px solid var(--line); border-left-width: 4px; padding: 10px 12px;
            max-width: 620px; }
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
    rendered = "".join(
        f' {key.rstrip("_").replace("_", "-")}="{esc(value)}"' for key, value in attrs.items()
    )
    return Html(f"<{name}{rendered}>{esc(body)}</{name}>")


def join(parts: Iterable[object]) -> Html:
    return Html("".join(esc(part) for part in parts))


def note(text: str) -> Html:
    return tag("p", text, class_="note")


def section(heading: str, body: Iterable[object]) -> Html:
    """A page's heading and what is under it. The heading is the page's one `h1`: every page
    builds exactly one section, and the headings inside it are `h2`."""
    return join([tag("h1", heading), *body])


def table(
    headers: Sequence[str],
    rows: Iterable[Sequence[object]],
    numeric: Sequence[int] = (),
    wrap: Sequence[int] = (),
    *,
    caption: str,
    empty: str = "Nothing to show.",
) -> Html:
    """One table. `numeric` is the columns that read as figures, right-aligned on a tabular
    figure; `wrap` is the columns that carry a sentence, which are the only ones allowed to
    wrap, since a reason the Judge wrote is prose and not a cell.

    The `caption` names the table for a screen reader and is hidden from the eye, since a
    heading sits above most of them. Every header scopes its column. With no rows the table
    says `empty` in a row of its own, so it is never a header with nothing under it. The table
    sits in a box that scrolls sideways, so a wide one never widens the page; the box takes
    focus (`tabindex`) so a keyboard can scroll it."""

    def cell(kind: str, index: int, value: object) -> str:
        scope = ' scope="col"' if kind == "th" else ""
        if index in numeric:
            return f'<{kind} class="num"{scope}>{esc(value)}</{kind}>'
        if index in wrap:
            return f'<{kind} class="text"{scope}>{esc(value)}</{kind}>'
        return f"<{kind}{scope}>{esc(value)}</{kind}>"

    head = "".join(cell("th", i, header) for i, header in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(cell("td", i, value) for i, value in enumerate(row)) + "</tr>"
        for row in rows
    ) or (f'<tr><td class="empty" colspan="{len(headers)}">{esc(empty)}</td></tr>')
    return Html(
        '<div class="scroll" tabindex="0"><table>'
        f'<caption class="sr">{esc(caption)}</caption>'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"
    )


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
    """The shell: a skip link, the header with the views' navigation (the page being read is
    marked `aria-current`), the page's own content in `main` and the footer. The content's
    first heading is the page's `h1`; see `section`."""
    nav = join(
        tag("a", label, href=href, aria_current="page")
        if href == path
        else tag("a", label, href=href)
        for href, label in VIEWS
    )
    return (
        "<!doctype html>\n"
        f'<html lang="en"><head><meta charset="utf-8">'
        f'<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{esc(title)} — agb-trader</title><style>{STYLE}</style></head><body>"
        '<a class="skip" href="#main">Skip to the page</a>'
        f'<header><p class="brand">agb-trader</p><nav aria-label="Views">{nav}</nav></header>'
        f'<main id="main">{join(blocks)}</main>'
        "<footer>Read-mostly: the pages read, and the only writes are answering a "
        "rebalance, the kill switch and acknowledging an alert, all recorded against this "
        "surface. Nothing here "
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
        upcoming = conn.execute(readout.NEXT_SUBMISSION, (now,)).fetchone()
        next_cycle = (
            f"next rebalance {upcoming[0]}, orders due by {et(upcoming[1])}"
            if upcoming
            else "no rebalance is scheduled"
        )
        body = tag("div", join([tag("b", "Healthy"), esc(f" — {next_cycle}")]))
        return tag("section", body, class_="banner clear", aria_label="Health")
    body = join(
        tag("div", join([tag("b", what), tag("span", f" — {facts}")])) for what, facts in lines
    )
    return tag("section", body, class_="banner unwell", aria_label="Health")


# Where each kill-switch write is confirmed. The kill switch page links here, and these pages
# only read: the first click on Pause, Resume or Ask to liquidate writes nothing, and the write is
# the form on the page it lands on, posted to the same route as before.
CONFIRM_PAUSE = "/confirm/pause"
CONFIRM_RESUME = "/confirm/resume"
CONFIRM_LIQUIDATE = "/confirm/liquidate"


def calendar(now: datetime) -> NyseCalendar:
    """The sessions the approval loop needs to find a rebalance's cutoff. A month either side
    of today covers every reference that can still be answered."""
    today = trading_date(now)
    return NyseCalendar(today - timedelta(days=7), today + timedelta(days=30))
