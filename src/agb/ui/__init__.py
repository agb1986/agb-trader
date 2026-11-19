"""The local UI's pages, rendered from the database by the server beside it
(docs/architecture/06-local-ui.md). Every page is a read, and a page renders on a connection
the database itself holds read-only, so a mistake on a page cannot become a write. The
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

The writes are Telegram's: answering the rebalance, the kill switch, and acknowledging an
alert. None is written here. Answering goes through `agb.approval`, the switch through
`agb.killswitch` and acknowledging through `agb.alerts`, the same functions the bot calls,
with a source naming this surface, so the semantics cannot drift between the two surfaces and
the race between them is settled in one place. Approving is never one click: the operator
types the rebalance's reference, which is the friction Telegram's typed command has and the
whole reason the landing page is the log rather than a basket with a green button.
Acknowledging is typed too: the Alerts page lists each alert's id, and the operator types the
one they have seen. Every write is a `command` row with the actor and the surface on it,
refused ones included.

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

The package is three modules and this file. `shell` holds what every page is built from: the
routes and styles, the connection, the escaped-markup helpers and the health banner. `views`
holds the log and the nine views beside it. `forms` holds the confirmation pages, the writes, the
outcome page and the `ROUTES` and `WRITES` tables. Every public name is importable from `agb.ui`,
and `ROUTES` and `WRITES` here are the very dicts `forms` defines.
"""

from agb.ui.forms import (
    CAUSE_FIELD,
    OUTCOME,
    ROUTES,
    WRITES,
    Outcome,
    ack,
    approve,
    confirm_form,
    confirm_liquidate,
    confirm_pause,
    confirm_resume,
    failed,
    liquidate,
    not_allowed,
    not_found,
    outcome,
    pause,
    reject,
    resume,
)
from agb.ui.shell import (
    ACTOR,
    ALERT_MAX,
    CONFIRM_LIQUIDATE,
    CONFIRM_PAUSE,
    CONFIRM_RESUME,
    LOG_CYCLES,
    READ_ONLY,
    STATE_CLASS,
    STYLE,
    SURFACE,
    VIEWS,
    Html,
    banner,
    calendar,
    connect,
    esc,
    join,
    note,
    page,
    section,
    table,
    tag,
    write_form,
)
from agb.ui.views import (
    BASES,
    GATES,
    LADDER_MAX,
    LATER_GATES,
    PANEL_MAX,
    PATHS,
    QUANTILES,
    alerts,
    gate,
    kill_switch,
    log,
    names_of,
    panel,
    performance,
    period_return,
    positions,
    promotion,
    rebalance,
    validation,
)

__all__ = [
    "ACTOR",
    "ALERT_MAX",
    "BASES",
    "CAUSE_FIELD",
    "CONFIRM_LIQUIDATE",
    "CONFIRM_PAUSE",
    "CONFIRM_RESUME",
    "GATES",
    "Html",
    "LADDER_MAX",
    "LATER_GATES",
    "LOG_CYCLES",
    "OUTCOME",
    "Outcome",
    "PANEL_MAX",
    "PATHS",
    "QUANTILES",
    "READ_ONLY",
    "ROUTES",
    "STATE_CLASS",
    "STYLE",
    "SURFACE",
    "VIEWS",
    "WRITES",
    "ack",
    "alerts",
    "approve",
    "banner",
    "calendar",
    "confirm_form",
    "confirm_liquidate",
    "confirm_pause",
    "confirm_resume",
    "connect",
    "esc",
    "failed",
    "gate",
    "join",
    "kill_switch",
    "liquidate",
    "log",
    "names_of",
    "not_allowed",
    "not_found",
    "note",
    "outcome",
    "page",
    "panel",
    "pause",
    "performance",
    "period_return",
    "positions",
    "promotion",
    "rebalance",
    "reject",
    "resume",
    "section",
    "table",
    "tag",
    "validation",
    "write_form",
]
